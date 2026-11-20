# 从 CLI 或网关接管任务终端

每个任务的终端留在执行它的节点上。CLI 可以直连执行机，也可以通过在线主管转发。先在主管上运行 `tarla --workspace work runs`，把目标任务的 `run_id` 设为下文的 `RUN_ID`。

| 连接条件 | 操作 |
| --- | --- |
| 客户端能访问执行机 | `tarla --workspace work attach "$RUN_ID"` 默认直连并只读查看。已建立的直连可在主管离线后继续使用。 |
| 只有主管能访问执行机 | `tarla --workspace work attach "$RUN_ID" --via-supervisor` 经主管的配对网络查看终端。主管和执行机均须在线。 |
| 主管已经离线，需要重新连接 | 将 `TARLA_URL` 指向可达的执行机，令 `TARLA_TOKEN` 取该执行机同一工作空间的客户端令牌，然后执行 `tarla --workspace work attach "$RUN_ID"`。 |

用 `tarla --workspace work run "$RUN_ID"` 查看任务的 `terminal_modes`。包含 `interactive` 时，可在上述命令末尾加 `--interactive` 输入按键，按 `Ctrl-]` 脱离。结构化运行的 Codex 和 Claude 工位仅支持旁观。要给它们下一条指令，在原会话运行 `tarla --workspace work chat "$CONVERSATION_ID" "继续检查编译结果"`。执行机还未确认允许输入或任务已经结束时，可从任务详情的 `terminal_input_hint` 查看原因。

## 配置直连地址

假设主管经私网访问执行机 `worker`，而客户端可以访问 `https://worker.example.net`。初次配对时，在主管上执行以下命令，随后按双机配对步骤配置执行机。已有配对可在停止主管后，为 `workspace.toml` 中对应的 `[[peers]]` 增加 `client_url = "https://worker.example.net"`，再启动主管。HTTPS 入口需要支持 WebSocket Upgrade。[双机配对步骤](../README.md#配对两台主机)

```sh
tarla --workspace work pair worker http://100.64.0.2:7878 \
  --token-env OFFICE_WORKER_KEY --client-url https://worker.example.net
```

`OFFICE_WORKER_KEY` 是两台守护进程之间的配对密钥。客户端请求业务接口使用所连接节点的工作空间令牌，该令牌保存在该节点工作空间的 `runtime/api.token` 中。

## 客户端协议

1. 向当前 Core 的 `POST /v1/runs/{id}/attach` 发送工作空间 Bearer 令牌和 JSON，例如 `{"mode":"observe","route":"auto"}`。请求远程任务时，`auto` 在已配置 `client_url` 时直连，否则经主管转发。省略 `route` 或显式传 `direct` 时，沿用原有直连行为，优先取 `client_url`，否则取配对地址 `url`。显式传 `supervisor` 可指定中继。
2. 读取应答中的 `ticket`。直连远程任务时在返回的 `node_url` 上打开 `/v1/terminal/{id}`；主管中继或本机任务则在当前 Core 地址上打开该路径。WebSocket 握手的 `Authorization: Bearer` 头使用本次票据。票据绑定工作空间、节点、任务、终端和模式，可在 60 秒内兑换一次。重连时重新申请。

## 从 IM 收到终端提示

启用审批通知的已绑定话题会收到终端输入提醒。当程序等待键盘输入时，提醒中包含该任务的 CLI 接管命令。在能访问主管且已有工作空间凭证的机器上执行它，回答会送达正在等待的进程。同一逻辑会话中的消息按入站顺序处理，其他会话可以同时回复。主管的模型后端暂时全部不可用时，网关会保存待处理消息并重试。[网关配置与配对](../crates/tarla-gateway/README.md)

终端验收使用两台守护进程、真实 Herdr 和 CLI，覆盖直连、中继、主管离线、只读权限和过期票据。网关验收覆盖模型恢复及真实终端提示。[终端验收](../tests/stage3_terminal.py) · [网关验收](../tests/gateway_recovery.py)
