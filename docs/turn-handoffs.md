# 工位调度与回合交接

主管根据工位的工作空间、节点在线状态、滚动和每周配额、能力与审核者模型家族筛选候选。任一配额窗口使用量达到 95% 时停止接单，重置时间到达后重新计算。余下工位按配置的角色优先级、角色匹配度、计费层级、配额余量、重置时间与负载排序。同分时按节点和工位标识确定顺序。主管在提交派单的 SQLite 事务内计入新预留的工位负载，因此下一笔并发派单能看到尚未被远端心跳确认的任务。

固定会话进行中可以发送 `/switch worker/spare`，也可以发送 `@spare 下一项任务`。响应中的 `handoff: pending` 和 `after_run_id` 表示请求已经保存。主管收到原回合的终态回执后才改变绑定，再把结果写回同一会话。重复投递相同来源的请求会返回原确认。运行状态为 `unconfirmed` 时，主管等待执行节点重新确认。

Codex 或 Claude 的结构化限流错误可以触发自动交接。自定义驱动也可以在当前任务内向运行钩子报告限流，提供可核实的进度摘要。环境变量由执行器注入，`resets_at` 为 Unix 秒。

```sh
curl --fail-with-body "$TARLA_HOOK_URL/rate-limit" \
  -H "Authorization: Bearer $TARLA_RUN_TOKEN" \
  -H "Content-Type: application/json" \
  --data "{\"resets_at\":$(($(date +%s) + 600)),\"checkpoint\":\"已完成配置核对，剩余工作为运行验证用例。\"}"
```

执行器确认程序以失败状态退出后，才将工位标记为冷却，将进度与普通回执写入持久发件箱。后续任务保留会话、任务目标、验收条件、能力与审核来源，并获得独立的 `run_id`。主管优先选择同节点同驱动的备用工位。具备相同工作目录和可验证的本地原生会话时，执行器仅复制该会话对应的 transcript 到备用账号，并以确切的会话 ID 调用原生 resume。其他工位收到有界进度摘要。S0 原文保留在执行机。已经承担过该交接链的工位会被排除。

`GET /v1/runs/{id}` 提供 `handoff_state`、`handoff_detail`、`predecessor_run_id` 和 `successor_run_id`。候选工位都不可用时，主管保存挂起的交接，并在工位状态变化后重试。可以使用原任务的取消操作撤销尚未派出的后续任务。交接关系、确认和工位选择均保存在本工作空间的 SQLite 数据库。

`python3 tests/stage2_scheduler.py` 在两台守护进程上验证公开派单接口。设置 `TARLA_TEST_HC` 后，`python3 tests/pinned.py` 和 `python3 tests/quota_handoff.py` 会通过私有终端验证延迟切换、限流、重启与原生会话恢复。
