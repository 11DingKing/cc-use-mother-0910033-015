# 变更通知投递箱

排班变更批准后，学校、讲解员、场馆会收到**内容不同**的通知。本服务把业务变更
与通知事件在**同一数据库事务**写入投递箱，由工作进程按**租约**批量认领投递，
配合**接收方幂等键**与**失败重投审计**，保证：

- 请求超时重试不会产生重复通知；
- 数据库更新成功但通知发送失败时可以补发；
- 发送失败按指数退避重试，超过上限进入终止失败（dead），可人工重投；
- 工作进程崩溃或服务重启后，过期租约自动回收，消息不丢；
- 普通 API 只能看到本校通知，跨校访问返回 404。

## 领域契约

- `domain/contract.json`：领域角色、状态、约束和样例。
- 四个关键不变量与实现的对应关系：

| 契约不变量 | 实现 |
| --- | --- |
| 事务投递箱 | `service.approve_change` 与 `repositories.add_event` 在同一事务写业务表和 `outbox_event`/`outbox_recipient` |
| 租约批量认领 | `worker.DeliveryWorker.claim_batch`：`lease_owner` + `leased_until`，过期租约可被回收 |
| 接收方幂等键 | 事件 `idempotency_key`（业务单号）+ 消息 `dedupe_key`（事件键:渠道:地址） |
| 失败重投审计 | `delivery_attempt` 记录每次成败，`outbox_admin_action` 记录人工重投 |

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/notification_outbox/`：投递箱后端。
  - `database.py`：SQLite 表结构、WAL、事务。
  - `models.py`：通知事件与接收方模型（渠道：school/docent/venue）。
  - `repositories.py`：同事务写入、积压统计、按校查询。
  - `service.py`：排班变更业务（创建/批准），批准即同事务生成三类接收方。
  - `rendering.py`：三类接收方各自的通知模板。
  - `senders.py`：发送通道协议，瞬时/永久错误分类。
  - `worker.py`：租约认领、退避重试、终止失败、人工重投。
  - `api.py`：HTTP API 与多租户隔离。
  - `cli.py` / `__main__.py`：管理命令与服务入口。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约、事务、投递、并发、API 隔离与端到端回归测试。

## 投递状态机

```
pending ──认领──▶ leased ──发送成功──────────────▶ succeeded（终态）
                    │
                    ├──瞬时失败──▶ pending（not_before = 指数退避）
                    │                 └──再次认领…… attempts 达上限──▶ dead（终态）
                    └──永久错误──────────────────────────────────────▶ dead（终态）

dead ──人工 reinject──▶ reopened（attempts 清零，not_before=现在）──▶ leased …
```

租约有效期内消息不会被其他进程认领；持有租约的进程崩溃后，租约到期消息
自动重新进入可认领状态，因此服务重启不丢消息。同一消息可能被发送两次
（至少一次语义），消息携带稳定 `dedupe_key`，由通道侧完成最终去重。

## 命令行

```bash
export PYTHONPATH=src

python -m notification_outbox init-db --db app.sqlite3
python -m notification_outbox backlog --db app.sqlite3        # 查看积压
python -m notification_outbox list --db app.sqlite3 --status dead
python -m notification_outbox reinject --db app.sqlite3 \
    --actor ops-wang --note "地址已更正" 12 15                # 人工重投
python -m notification_outbox worker --db app.sqlite3         # 投递工作进程
python -m notification_outbox serve --db app.sqlite3 \
    --tokens tokens.json --port 8080                          # HTTP API
```

令牌文件格式：

```json
{
  "school-token-1": {"school_id": 1},
  "school-token-2": {"school_id": 2},
  "admin-token": {"admin": true}
}
```

## HTTP API

| 方法/路径 | 身份 | 说明 |
| --- | --- | --- |
| `POST /api/schedule-changes` | 学校 | 创建排班变更（school_id 以令牌为准） |
| `POST /api/schedule-changes/{id}/approve` | 学校 | 批准；同事务写入三类通知，重复批准幂等 |
| `GET /api/events/{id}` | 学校 | 查看事件与本校接收方投递状态（跨校 404） |
| `GET /api/recipients?status=pending` | 学校 | 本校投递项列表 |
| `GET /api/recipients/{id}/attempts` | 学校 | 该接收方每次投递的审计记录 |
| `GET /admin/backlog` | 管理员 | 跨校积压统计 |
| `GET /admin/recipients?status=dead` | 管理员 | 终止失败列表 |
| `POST /admin/recipients/reinject` | 管理员 | 人工重投，body：`{"recipient_ids":[...], "note":"..."}` |

## 接入真实发送通道

实现 `senders.Sender` 协议替换 CLI 默认的日志发送器：

- 发送成功才正常返回；超时/5xx/限流抛 `TransientError`；
- 地址不存在等不可恢复错误抛 `PermanentError`（立即终止）；
- 不确定是否送达时一律按瞬时错误处理（宁可重发，由 `dedupe_key` 去重）。

## 验证

```bash
python3 -m unittest discover -s tests -v       # 38 个测试
python3 -m compileall -q src tools tests
python3 tools/check_contract.py domain/contract.json
```
