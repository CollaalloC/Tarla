# 主管借用本机订阅工位

在主管的 `workspace.toml` 中，把可供借用的本机账号配置为 `[[slots]]`。每个 Claude 账号使用独立的工位 ID。先用该用户在对应的 `CLAUDE_CONFIG_DIR` 登录订阅，再启动守护进程。默认目录为 `~/.tarla/slots/<slot_id>`，指定 `--home` 时使用该目录下的 `slots/<slot_id>`。所选 Claude Code 版本须支持下文列出的全部参数。仅主管节点也可登记用于借用的账号，使用同一套[工位遥测接口](../crates/tarlad/README.md)接收配额更新。

```toml
[supervisor.brain]
fallback_chain = ["slot_borrow:subscriptions", "http:local"]

[supervisor.brain.backends.subscriptions]
kind = "slot_pool"
slots = ["claude-max-a", "claude-max-b"]

[supervisor.brain.backends.local]
kind = "openai"
endpoint = "http://127.0.0.1:11434/v1/chat/completions"
model = "qwen3"

[[slots]]
slot_id = "claude-max-a"
driver = "claude"
model_family = "anthropic"
billing_tier = "prepaid_subscription"
workdir = "/absolute/path/to/project"
argv = ["/absolute/path/to/claude"]

[[slots]]
slot_id = "claude-max-b"
driver = "claude"
model_family = "anthropic"
billing_tier = "prepaid_subscription"
workdir = "/absolute/path/to/project"
argv = ["/absolute/path/to/claude"]
```

单个工位也可直接写入 `fallback_chain = ["slot_borrow:claude-max-a"]`。已有的 `kind = "claude_slot"` 配置仍按显式的 `executable`、`credential_dir` 和可选 `model` 启动独立账号。使用注册工位或工位池即可纳入配额、健康状态和运行账本检查。

每次模型决策都会重新读取当前工作空间的本机工位遥测，以滚动和周配额中较少的剩余百分比排序。相同余量按池中的配置顺序选择。已过重置时间的窗口重新计为零使用率。任一窗口已用达 95%、账号过期、尚在冷却、已有未终结任务或账户锁被占用时跳过该工位。只考虑 `prepaid_subscription` 和 `free_quota`。账号实际计费遵循供应商订阅和额外用量设置。

借用时只启动 `argv[0]` 指定的绝对路径，只传递主管适配器固定的参数。决策在临时空目录进行，输入为当前主管窗口和控制工具的 JSON。Claude 使用 `-p --safe-mode --tools "" --disallowedTools "mcp__*" --strict-mcp-config --mcp-config '{"mcpServers":{}}' --max-turns 1 --no-session-persistence`。CLI 报错或不可用时继续试池中下一个账号，然后试下一个配置的后端。格式错误的模型决策会终止该轮。组织级 Claude 托管策略仍可运行管理员配置的钩子，请在允许借用的主机上核对该策略。[Claude CLI 参数](https://code.claude.com/docs/en/cli-reference)

Codex 的 `exec --ephemeral` 支持临时会话，`--sandbox read-only` 仍允许读取。Gemini 无头模式允许工具执行，也会管理会话历史。当前主管适配器把这两种工位标记为无法借用；无需启动 CLI 即可继续回退。执行机仍可将这两类工位用于普通任务。[Codex CLI](https://developers.openai.com/codex/cli/reference/) · [Gemini 无头模式](https://geminicli.com/docs/cli/headless/) · [Gemini 会话历史](https://geminicli.com/docs/cli/session-management/)
