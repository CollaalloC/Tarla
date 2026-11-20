# Tarla 消息网关

主管按工作空间加载以下配置。每个空间使用自己的平台应用、凭证及公开 HTTPS 回调地址。生产部署用反向代理只公开需要的 webhook 路由。配对管理和 Core API 使用工作空间持有者的 Bearer token。

```toml
[gateway.telegram]
enabled = true
bot_token_env = "WORK_TELEGRAM_BOT_TOKEN"
webhook_secret_env = "WORK_TELEGRAM_WEBHOOK_SECRET"

[gateway.feishu]
enabled = true
app_id = "cli_example"
app_secret_env = "WORK_FEISHU_APP_SECRET"
verification_token_env = "WORK_FEISHU_VERIFICATION_TOKEN"
encrypt_key_env = "WORK_FEISHU_ENCRYPT_KEY"  # 飞书启用 Encrypt Key 时填写
```

- Telegram 用 Bot API 的 `setWebhook` 将 URL 指向 `/gateway/telegram`，传入与配置相符的 `secret_token`，订阅 `message` 和 `callback_query`。论坛话题按 `message_thread_id` 区分。
- 飞书在事件与回调订阅中使用同一个 `/gateway/feishu` 地址，订阅 `im.message.receive_v1`、新版 `card.action.trigger`，开启机器人接收文本、发送消息权限并发布版本。应用通过 tenant access token 发消息。配置 Encrypt Key 时，网关验证请求签名、解密 AES-CBC 数据及校验 Verification Token。
- Slack 可启用 `[gateway.slack]` 并设置 `enabled=true`、`team_id`、`signing_secret_env`、`bot_token_env`。事件和 Interactivity 地址均为 `/gateway/slack`。按需订阅 `message.im`、`message.channels`、`message.groups` 并授予相应历史读取 scope 和 `chat:write`。线程按 `thread_ts` 区分。
- Discord 可启用 `[gateway.discord]` 并设置 `enabled=true`、`application_id`、十六进制 `public_key` 及 `bot_token_env`。在 Developer Portal 将 Interactions Endpoint 设为 `/gateway/discord`，注册带必填字符串参数 `text` 的 `/tarla` 应用命令。将机器人安装至目标服务器并授予发送消息的权限，子线程使用各自的 channel ID。

## 配对与撤销

先在 App 或 CLI 选择逻辑会话。以下示例把 Telegram 用户 `123456` 的当前话题连接到会话 `planning`。把请求发给对应工作空间的 Core，替换成实际用户 ID 和会话 ID。

```sh
curl -X POST "$CORE/gateway/pairings" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"platform":"telegram","actor_id":"123456","conversation_id":"planning","approvals":true}'
```

将返回的五分钟一次性代码作为 `/pair CODE` 或普通文本 `tarla pair CODE` 发到目标聊天 / 话题。Discord 的输入形式为 `/tarla text:tarla pair CODE`。Slack 建议用普通文本。飞书使用发送者的 open_id。只有指定平台与用户可兑换，之后只有该用户可通过这条绑定发消息或审批。选择群组时，同一话题中的成员都能看到后续回复。需要单独给另一个人使用时，请绑定新的私聊或话题。

网关按接收顺序处理属于同一会话的消息，包括从其他平台接续的消息。不同会话可以并行处理。主管配置的所有模型均暂时不可用时，Core 返回 HTTP 503。网关保留该消息和后续消息，重启后继续重试。开启 `approvals` 的话题还会收到终端输入提醒，内含指向具体工作空间和任务的 `tarla attach` 命令。

多个渠道可配对到相同会话，直接使用 Core 的会话绑定 API 设置 pinned 工位。生成配对码时记录会话游标，兑换后投递该游标之后的助手消息与当前待处理审批。审批卡只对绑定会话中的任务生效，且配对时需设置 `approvals:true`。使用授权的 `GET /gateway/pairings` 查看绑定，`DELETE /gateway/pairings/{id}` 撤销绑定及其待投递数据。

## 独立进程

独立网关读取其私有目录内的 `workspace.toml`。配置 `TARLA_WORKSPACE_DIR`、`TARLA_CORE_URL`、`TARLA_CORE_TOKEN`，启动 `tarla-gateway`。监听地址由 `TARLA_GATEWAY_LISTEN` 配置，默认 `127.0.0.1:7701`。同一平台应用只运行一组回调和投递服务。使用独立部署时，仅在网关端启用相应的渠道。

收到的已鉴权消息先写入 SQLite，后台用同一来源 ID 重试暂时失败的请求。Telegram、飞书、Slack 和 Discord 的流式回复会在发起提问的话题显示草稿。网关只查询这次请求的公开回答文本，每 650 毫秒检查一次变化，最多每 1.8 秒安排一次草稿更新。草稿末尾显示 `▍`，工具参数与推理过程不会发往聊天。支持编辑机器人已发送文本并保留相应平台权限后，用户会看到同一条消息逐步补全。

网关每三秒按游标拉取已完成的助手消息。最终回复会替换草稿，超出平台长度的部分按顺序续发。其他已绑定入口收到最终回复。快速回复、未提供增量的模型和异步任务回执直接经过最终消息通道。远端消息 ID 与待投递数据保存在工作空间的 SQLite 中，重启后可继续编辑同一条草稿。如果平台已删除草稿或拒绝编辑，会单独发送最终回复。出站投递为至少一次，平台已接收但本地尚未记录时的崩溃可能产生重复通知。
