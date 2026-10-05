"""HTTP 接口（仅标准库）：批准变更、按学校租户隔离的通知查询、运营管理端点。

租户隔离：学校令牌只能访问本学校租户的通知内容，跨学校访问返回 403；
积压与重投等运营端点仅管理员令牌可用，且积压视图只含计数不含通知内容。
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .clock import Clock, SystemClock
from .db import Database
from .errors import ConflictError, InvalidStateError, NotFoundError
from .service import ApprovalRequest, NotificationService


def create_server(
    db_path: str,
    *,
    tenant_tokens: dict[str, str],
    admin_tokens: set[str] | frozenset[str],
    host: str = "127.0.0.1",
    port: int = 0,
    clock: Clock | None = None,
) -> ThreadingHTTPServer:
    service = NotificationService(Database(db_path), clock=clock or SystemClock())
    tenant_tokens = dict(tenant_tokens)
    admin_tokens = set(admin_tokens)

    class Handler(BaseHTTPRequestHandler):
        server_version = "NotificationOutbox/0.1"

        # ---------- 基础工具 ----------
        def _send_json(self, status: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _error(self, status: int, code: str, message: str) -> None:
            self._send_json(status, {"error": {"code": code, "message": message}})

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length") or 0)
            if length == 0:
                return {}
            try:
                return json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError("请求体不是合法 JSON") from exc

        def _auth(self) -> tuple[str, str | None] | None:
            """返回 ("admin", None) 或 ("tenant", tenant_id)；无效凭证返回 None。"""
            header = self.headers.get("Authorization", "")
            token = header.removeprefix("Bearer ").strip() if header.startswith("Bearer ") else None
            if token and token in admin_tokens:
                return ("admin", None)
            if token and token in tenant_tokens:
                return ("tenant", tenant_tokens[token])
            return None

        def log_message(self, *args) -> None:  # 保持测试输出安静
            pass

        # ---------- 路由 ----------
        def do_POST(self) -> None:
            path = urlparse(self.path).path
            m = re.fullmatch(r"/api/changes/([^/]+)/approve", path)
            if m:
                return self._approve(m.group(1))
            m = re.fullmatch(r"/api/admin/events/([^/]+)/redrive", path)
            if m:
                return self._redrive(m.group(1))
            self._error(404, "not_found", "接口不存在")

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            m = re.fullmatch(r"/api/tenants/([^/]+)/notifications", path)
            if m:
                return self._tenant_notifications(m.group(1))
            if path == "/api/admin/backlog":
                return self._backlog()
            self._error(404, "not_found", "接口不存在")

        # ---------- 端点 ----------
        def _approve(self, change_id: str) -> None:
            auth = self._auth()
            if auth is None:
                return self._error(401, "unauthorized", "缺少有效凭证")
            try:
                body = self._read_json()
            except ValueError as exc:
                return self._error(400, "bad_json", str(exc))
            required = ["school_id", "docent_id", "venue_id", "activity_date", "approved_by"]
            missing = [key for key in required if not body.get(key)]
            if missing:
                return self._error(400, "bad_request", "缺少字段：" + "、".join(missing))
            if auth[0] == "tenant" and auth[1] != body["school_id"]:
                return self._error(403, "forbidden", "不能代其他学校提交变更")
            try:
                result = service.approve_change(
                    ApprovalRequest(
                        change_id=change_id,
                        school_id=body["school_id"],
                        docent_id=body["docent_id"],
                        venue_id=body["venue_id"],
                        activity_date=body["activity_date"],
                        approved_by=body["approved_by"],
                        note=body.get("note", ""),
                    )
                )
            except ConflictError as exc:
                return self._error(409, "conflict", str(exc))
            self._send_json(
                200 if result.replayed else 201,
                {
                    "change_id": result.change_id,
                    "event_id": result.event_id,
                    "state": result.state,
                    "replayed": result.replayed,
                },
            )

        def _tenant_notifications(self, tenant_id: str) -> None:
            auth = self._auth()
            if auth is None:
                return self._error(401, "unauthorized", "缺少有效凭证")
            if auth[0] == "tenant" and auth[1] != tenant_id:
                return self._error(403, "forbidden", "无权查看其他学校的通知")
            self._send_json(
                200,
                {
                    "tenant_id": tenant_id,
                    "notifications": service.list_tenant_notifications(tenant_id),
                },
            )

        def _backlog(self) -> None:
            auth = self._auth()
            if auth is None:
                return self._error(401, "unauthorized", "缺少有效凭证")
            if auth[0] != "admin":
                return self._error(403, "forbidden", "仅运营管理员可查看积压")
            self._send_json(200, service.backlog())

        def _redrive(self, event_id: str) -> None:
            auth = self._auth()
            if auth is None:
                return self._error(401, "unauthorized", "缺少有效凭证")
            if auth[0] != "admin":
                return self._error(403, "forbidden", "仅运营管理员可人工重投")
            try:
                body = self._read_json()
            except ValueError as exc:
                return self._error(400, "bad_json", str(exc))
            try:
                result = service.redrive_event(
                    event_id,
                    actor=body.get("actor", "admin-api"),
                    reason=body.get("reason", ""),
                )
            except NotFoundError as exc:
                return self._error(404, "not_found", str(exc))
            except InvalidStateError as exc:
                return self._error(409, "invalid_state", str(exc))
            except ValueError as exc:
                return self._error(400, "bad_request", str(exc))
            self._send_json(200, result)

    return ThreadingHTTPServer((host, port), Handler)
