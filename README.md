# 变更通知投递箱

本项目维护变更通知投递箱的领域约定、角色边界与样例数据，供后端服务、接口和自动化验证统一使用。当前契约覆盖活动统筹员、讲解员、学校联系人、场馆管理员，并明确事务投递箱、租约批量认领、接收方幂等键、失败重投审计等关键约束。

## 目录

- `domain/contract.json`：领域角色、状态、约束和样例。
- `src/domain_contract/`：契约读取与确定性校验。
- `src/notification_outbox/`：投递箱后端（业务服务、工作进程、HTTP 接口、管理命令）。
- `tools/check_contract.py`：命令行摘要检查。
- `tests/`：契约完整性与后端行为回归测试。

## 后端架构

排班变更批准后，学校联系人、讲解员、场馆管理员会收到内容各不相同的通知。为杜绝「请求内直接发送」在超时重试下产生重复消息、以及「数据库更新成功但通知失败」无法补发的问题，后端采用事务投递箱模式，仅依赖 Python 标准库与 SQLite：

- **事务投递箱**（`service.py`）：`approve_change` 在同一事务中写入排班变更、投递箱事件与每个接收方的投递记录；`(event_type, aggregate_id)` 唯一约束让超时重试幂等返回原事件，通知写失败则业务变更一起回滚。
- **租约批量认领**（`worker.py`）：工作进程在单事务内把一批到期事件置为 `processing` 并记录 `claimed_by` / `claimed_until`；租约到期未完成的事件可被任意进程重新认领，工作进程崩溃或服务重启都不丢消息。
- **接收方幂等键**（`delivery.idempotency_key`）：键全局唯一且稳定（`{event_id}:{role}`），通道侧按键去重；「已送达但结果未记录」的尝试重试时记为 `duplicate`，接收方只收到一条消息。
- **退避重试与终止失败**：可重试错误按指数退避（`base * 2^n`，封顶）重排 `next_attempt_at`；超过 `max_attempts` 或收到不可重试错误时事件转 `dead`（终止失败），每个接收方的每次尝试都记录在 `delivery_attempt`。
- **失败重投审计**：`dead` 事件只能经 `redrive_event` 人工重投（必须填写原因），`redrive_audit` 记录操作者、原因与原状态。
- **租户隔离**（`api.py`）：学校令牌只能读取本学校租户的通知内容，跨学校访问返回 403；积压与重投端点仅管理员可用，且积压视图只含计数不含内容。

## 验证

测试命令：`python3 -m unittest discover -s tests -v`

编译命令：`python3 -m compileall -q src tools tests`

命令行检查：`python3 tools/check_contract.py domain/contract.json`

## 管理命令

```bash
# 查看投递积压（事件/接收方计数、最老积压时长、重投次数）
PYTHONPATH=src python3 -m notification_outbox.cli --db outbox.db backlog

# 运行工作进程（--once 处理完当前可认领批次后退出）
PYTHONPATH=src python3 -m notification_outbox.cli --db outbox.db worker --once

# 人工重投终止失败的事件（写入重投审计）
PYTHONPATH=src python3 -m notification_outbox.cli --db outbox.db redrive evt_xxx --actor ops --reason "通道恢复后补发"

# 启动 HTTP 接口
PYTHONPATH=src OUTBOX_ADMIN_TOKENS=ops OUTBOX_TENANT_TOKENS=tok1=school-1 \
  python3 -m notification_outbox.cli --db outbox.db serve --port 8080
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/changes/{change_id}/approve` | 批准排班变更并同事务写入投递箱；重放幂等 |
| GET | `/api/tenants/{tenant_id}/notifications` | 查询本学校通知（学校令牌仅限本租户） |
| GET | `/api/admin/backlog` | 查看投递积压（仅管理员，只含计数） |
| POST | `/api/admin/events/{event_id}/redrive` | 人工重投终止失败的事件（仅管理员） |
