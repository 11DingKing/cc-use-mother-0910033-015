"""HTTP API（标准库实现，无第三方依赖）。

租户隔离：学校令牌在认证时解析出 ``school_id``，所有事件/投递项查询都
强制附加该校过滤，其他学校的通知内容一律不可见（返回 404，不承认存在）。
管理端接口（``/admin/*``）仅接受管理员令牌，可跨校查看积压。
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

from . import repositories as repo
from .database import connect, transaction
from .models import RecipientStatus
from .service import BusinessError, approve_change, create_change
from .worker import reinject_dead


@dataclass(frozen=True)
class Principal:
    token: str
    school_id: int | None
    is_admin: bool


# token -> (school_id, is_admin)。生产环境应换成共享的令牌/会话存储。
TokenRegistry = dict[str, Principal]


@contextmanager
def connection_scope(db_path: str) -> Iterator[sqlite3.Connection]:
    """每个请求一个连接，出作用域即关闭。"""
    conn = connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


class ApiError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


def _recipient_to_dict(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "event_id": row["event_id"],
        "school_id": row["school_id"],
        "channel": row["channel"],
        "address": row["address"],
        "template": row["template"],
        "params": json.loads(row["params"]),
        "status": row["status"],
        "attempts": row["attempts"],
        "not_before": row["not_before"],
        "last_error": row["last_error"],
        "leased_until": row["leased_until"],
        "succeeded_at": row["succeeded_at"],
        "dead_at": row["dead_at"],
        "event_type": row["event_type"],
        "aggregate_id": row["aggregate_id"],
        "idempotency_key": row["idempotency_key"],
    }


class OutboxApi:
    """可直接在测试中调用的应用层：handle 返回 (status, body)。"""

    def __init__(
        self,
        conn_factory: Callable[[], sqlite3.Connection],
        tokens: TokenRegistry,
    ) -> None:
        self._conn_factory = conn_factory
        self._tokens = tokens

    def handle(
        self, method: str, path: str, headers: dict[str, str], body: bytes | None
    ) -> tuple[int, dict]:
        try:
            principal = self._authenticate(headers)
            raw_path, _, raw_query = path.partition("?")
            query = {k: v[-1] for k, v in parse_qs(raw_query).items()}
            data = self._parse_json(body)
            return self._route(method, raw_path, principal, data, query)
        except ApiError as exc:
            return exc.status, {"error": exc.message}
        except BusinessError as exc:
            return exc.code, {"error": str(exc)}

    # ------------------------------------------------------------- 认证/解析

    def _authenticate(self, headers: dict[str, str]) -> Principal:
        auth = headers.get("Authorization") or headers.get("authorization", "")
        token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
        principal = self._tokens.get(token)
        if principal is None:
            raise ApiError(401, "未认证或令牌无效")
        return principal

    @staticmethod
    def _parse_json(body: bytes | None) -> dict:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(400, "请求体必须是合法 JSON")
        if not isinstance(value, dict):
            raise ApiError(400, "请求体必须是 JSON 对象")
        return value

    # ----------------------------------------------------------------- 路由

    def _route(
        self, method: str, path: str, principal: Principal,
        data: dict, query: dict[str, str],
    ) -> tuple[int, dict]:
        m = re.fullmatch(r"/api/schedule-changes(?:/(\d+)/approve)?", path)
        if m:
            self._require_school(principal)
            if method == "POST" and m.group(1) is None:
                return self._create_change(principal, data)
            if method == "POST" and m.group(1):
                return self._approve_change(principal, int(m.group(1)))

        m = re.fullmatch(r"/api/events/(\d+)", path)
        if m and method == "GET":
            self._require_school(principal)
            return self._get_event(principal, int(m.group(1)))

        m = re.fullmatch(r"/api/recipients(?:/(\d+)/attempts)?", path)
        if m and method == "GET":
            self._require_school(principal)
            if m.group(1):
                return self._get_attempts(principal, int(m.group(1)))
            return self._list_recipients(principal, query)

        if path == "/admin/backlog" and method == "GET":
            self._require_admin(principal)
            return self._admin_backlog()

        if path == "/admin/recipients" and method == "GET":
            self._require_admin(principal)
            return self._admin_list_dead(query)

        if path == "/admin/recipients/reinject" and method == "POST":
            self._require_admin(principal)
            return self._admin_reinject(principal, data)

        raise ApiError(404, "接口不存在")

    @staticmethod
    def _require_school(principal: Principal) -> None:
        if principal.is_admin or principal.school_id is None:
            raise ApiError(403, "该接口需要学校身份令牌")

    @staticmethod
    def _require_admin(principal: Principal) -> None:
        if not principal.is_admin:
            raise ApiError(403, "该接口仅管理员可用")

    # ------------------------------------------------------------- 学校接口

    def _create_change(self, principal: Principal, data: dict) -> tuple[int, dict]:
        # school_id 以令牌为准，忽略请求体中的同名字段，无法伪造跨校数据。
        payload = {k: v for k, v in data.items() if k != "school_id"}
        payload["school_id"] = principal.school_id
        with self._conn_factory() as conn:
            with transaction(conn):
                change_id = create_change(conn, payload)
        return 201, {"id": change_id}

    def _approve_change(self, principal: Principal, change_id: int) -> tuple[int, dict]:
        with self._conn_factory() as conn:
            with transaction(conn):
                event_id = approve_change(
                    conn, change_id, school_id=principal.school_id  # type: ignore[arg-type]
                )
        return 200, {"event_id": event_id}

    def _get_event(self, principal: Principal, event_id: int) -> tuple[int, dict]:
        with self._conn_factory() as conn:
            event = repo.get_event(conn, event_id, school_id=principal.school_id)
            if event is None:
                raise ApiError(404, "事件不存在")
            recipients = repo.list_event_recipients(
                conn, event_id, school_id=principal.school_id
            )
        return 200, {
            "id": event["id"],
            "event_type": event["event_type"],
            "aggregate_type": event["aggregate_type"],
            "aggregate_id": event["aggregate_id"],
            "school_id": event["school_id"],
            "payload": json.loads(event["payload"]),
            "created_at": event["created_at"],
            "recipients": [
                {
                    "id": r["id"],
                    "channel": r["channel"],
                    "address": r["address"],
                    "template": r["template"],
                    "status": r["status"],
                    "attempts": r["attempts"],
                    "last_error": r["last_error"],
                }
                for r in recipients
            ],
        }

    def _list_recipients(self, principal: Principal, query: dict) -> tuple[int, dict]:
        status = self._parse_status(query.get("status"))
        limit = self._parse_limit(query.get("limit"), 50)
        offset = self._parse_limit(query.get("offset"), 0)
        with self._conn_factory() as conn:
            rows = repo.list_recipients(
                conn,
                status=status,
                school_id=principal.school_id,
                limit=limit,
                offset=offset,
            )
        return 200, {"items": [_recipient_to_dict(r) for r in rows]}

    def _get_attempts(self, principal: Principal, recipient_id: int) -> tuple[int, dict]:
        with self._conn_factory() as conn:
            row = repo.recipient_by_id(conn, recipient_id, school_id=principal.school_id)
            if row is None:
                raise ApiError(404, "投递项不存在")
            attempts = repo.list_attempts(conn, recipient_id)
        return 200, {
            "recipient_id": recipient_id,
            "status": row["status"],
            "attempts": [
                {
                    "attempt_no": a["attempt_no"],
                    "result": a["result"],
                    "error_code": a["error_code"],
                    "error_detail": a["error_detail"],
                    "lease_owner": a["lease_owner"],
                    "created_at": a["created_at"],
                }
                for a in attempts
            ],
        }

    # ------------------------------------------------------------- 管理接口

    def _admin_backlog(self) -> tuple[int, dict]:
        with self._conn_factory() as conn:
            return 200, repo.backlog_summary(conn)

    def _admin_list_dead(self, query: dict) -> tuple[int, dict]:
        status = self._parse_status(query.get("status"), default=RecipientStatus.DEAD)
        limit = self._parse_limit(query.get("limit"), 50)
        with self._conn_factory() as conn:
            rows = repo.list_recipients(conn, status=status, limit=limit)
        return 200, {"items": [_recipient_to_dict(r) for r in rows]}

    def _admin_reinject(self, principal: Principal, data: dict) -> tuple[int, dict]:
        ids = data.get("recipient_ids")
        if not isinstance(ids, list) or not ids or not all(isinstance(x, int) for x in ids):
            raise ApiError(400, "recipient_ids 必须是非空整数数组")
        with self._conn_factory() as conn:
            reinjected = reinject_dead(
                conn, ids, actor=principal.token, note=str(data.get("note", ""))
            )
        return 200, {"reinjected": reinjected}

    # --------------------------------------------------------------- 工具

    @staticmethod
    def _parse_status(raw: object, *, default: RecipientStatus | None = None):
        if raw is None or raw == "":
            return default
        try:
            return RecipientStatus(str(raw))
        except ValueError:
            raise ApiError(400, f"未知状态：{raw}")

    @staticmethod
    def _parse_limit(raw: object, default: int) -> int:
        if raw is None or raw == "":
            return default
        try:
            value = int(raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            raise ApiError(400, "limit/offset 必须是非负整数")
        if value < 0 or value > 500:
            raise ApiError(400, "limit/offset 超出允许范围")
        return value


# ---------------------------------------------------------------- HTTP 封装


def build_handler(api: OutboxApi) -> type[BaseHTTPRequestHandler]:
    class _Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            self._dispatch()

        def do_POST(self) -> None:
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else None
            status, payload = api.handle(
                self.command, self.path, dict(self.headers), body
            )
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, fmt: str, *args: object) -> None:
            return  # 由调用方统一配置日志

    return _Handler


def serve(
    api: OutboxApi, *, host: str = "127.0.0.1", port: int = 8080
) -> ThreadingHTTPServer:
    """创建并返回 HTTP 服务器（调用方负责 serve_forever）。"""
    return ThreadingHTTPServer((host, port), build_handler(api))
