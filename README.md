# Tarla

Tarla 是按工作空间运行的个人智能体。每个空间指定一个主管节点，执行节点保存任务现场。手机、桌面、CLI 和消息网关都通过同一个业务 API 接入。服务器运行 `tarlad`，命令行程序名为 `tarla`。

[预编译核心程序和校验方法](docs/core-binaries.md)适用于 Linux x86-64、Linux ARM64、macOS Apple Silicon 和 Windows x64。

## 在一台机器上启动

从仓库根目录执行以下命令。需要稳定版 Rust。SQLite 随 Rust 依赖编译。

```sh
cargo build --locked --workspace
./target/debug/tarla --workspace work init --node office
./target/debug/tarlad --workspace work
```

另开一个终端检查服务，向同一个会话发送一条指令，并写入一条带版本的记忆。

```sh
./target/debug/tarla --workspace work status
./target/debug/tarla --workspace work chat project-a /help
./target/debug/tarla --workspace work remember fixture "参考工作距离为 285 mm" --base-version 0
./target/debug/tarla --workspace work memories
```

默认监听 `127.0.0.1:7878`。使用 `--home` 或 `TARLA_HOME` 可以更换整套数据目录。工作空间默认放在 `~/.tarla/workspaces/<id>/`。手机通过受信任的 HTTPS 反向代理连接，终端接管还需要代理允许 WebSocket Upgrade。CLI 的 `TARLA_URL` 和 `TARLA_TOKEN` 可用于访问已配对的服务。工作空间客户端令牌在 `runtime/api.token`，执行机的接管签名密钥在独立文件中。

## 启用模型和执行工位

`init` 后可立即运行斜杠指令。普通自然语言对话需要至少一个可用模型。编辑 `workspace.toml` 中已有的 `[supervisor.brain]` 表，在该表设置回退顺序，然后添加后端。下例使用本地 OpenAI 兼容服务，模型名称应与该服务实际加载的名称一致。

```toml
[supervisor.brain]
max_tool_steps = 5
fallback_chain = ["http:local"]

[supervisor.brain.backends.local]
kind = "openai"
endpoint = "http://127.0.0.1:11434/v1/chat/completions"
model = "qwen3"
```

远端 HTTP 后端可通过 `api_key_env` 引用环境变量。Anthropic 后端使用 `kind = "anthropic"` 及完整 Messages 端点。主管也可在每轮决策中按剩余配额借用本机 Claude 订阅工位。配置方法、CLI 能力要求和回退规则见[订阅工位配置](docs/subscription-slots.md)。

执行主机需要单独安装 herdr-core 和实际要运行的 Agent CLI。从所选上游发行渠道验证下载后，以下命令将已解包的可执行文件复制到私有缓存。最后一个参数是可执行文件本身的 SHA-256。

```sh
tarla --home ~/.tarla install-tool herdr-core 0.1.19 /path/to/herdr-core --sha256 <binary-sha256>
```

在执行节点的 `workspace.toml` 追加工位。目录应已存在。示例中的 Codex 使用该工位专属的 `CODEX_HOME`，需要事先在该目录完成账号登录。执行机把有界任务简报作为末尾的单一参数传给配置好的程序。

```toml
[[slots]]
slot_id = "codex-main"
driver = "codex"
model_family = "openai"
supported_roles = ["implementer"]
billing_tier = "prepaid_subscription"
workdir = "/absolute/path/to/project"
argv = ["/absolute/path/to/codex", "exec"]
```

重启配置过的守护进程即可查看可用工位、派发并查询任务。

```sh
tarla --workspace work slots
tarla --workspace work dispatch project-a "检查项目是否能编译" --accept "返回编译结果"
tarla --workspace work runs
tarla --workspace work log <run-id>
tarla --workspace work attach <run-id>
```

## 配对两台主机

下面以主管 `office` 和执行机 `worker` 为例。先初始化相同名称的工作空间，再停下对应的守护进程，设置双向配对。每一对节点使用独立生成的高熵共享密钥。通过受控路径把同一值提供给两端的 `OFFICE_WORKER_KEY` 环境变量，至少 32 字符。

```sh
# 主管机器
tarla --workspace work init --node office
tarla --workspace work pair worker https://worker.example.net --token-env OFFICE_WORKER_KEY

# 执行机器
tarla --workspace work init --node worker --role executor_only --supervisor office
tarla --workspace work pair office https://office.example.net --token-env OFFICE_WORKER_KEY
```

启动两端后，主管根据执行机上报的工位选址。执行机在本地持久化认领，再启动任务。网络断开后，本地 Outbox 保存状态与回执。主管重连时通过同一 `run_id` 对账。S2 记忆保存在执行机本地，可在离线时读取。配对地址须在选定网络上双向可达，SOCKS 出站代理和对端服务的入站地址应分别配置。

计划迁移时先停止旧主管，使用 `tarla supervisor handover --to <new-node> --snapshot <file>` 生成面向指定新机器的账本快照并降级旧主管。在新机器上用同一工作空间名及指定节点名初始化干净目录，再用 `tarla supervisor restore --snapshot <file>` 导入。根据新拓扑配置模型、网关及配对凭证后启动新主管。其余执行机停机调用 `tarla supervisor set-authority --node <new-node> --term <new-term>` 更新主管。手工恢复要求提供新任期，执行节点拒绝旧任期。快照含会话和长期记忆，应经私有通道传输。

## 测试及客户端

```sh
cargo test --locked --workspace
cargo build --locked --workspace
python3 tests/acceptance.py
```

验收脚本启动两个独立的守护进程、两个 SQLite 数据库和一个测试用 HTTP 模型，覆盖模型限流回退、消息重放、离线记忆、主管重启和冲突候选。设置环境变量 `TARLA_TEST_HC` 指向真实的 Linux herdr-core 可执行文件后，脚本还会派发跨节点任务，在执行期间重启两端，并校验进程只启动一次。真实机器人回调需要平台凭证和可访问的回调 URL。

| 工程规范阶段 | 可以复核的验收内容 | 入口 |
| --- | --- | --- |
| 一 · 单机主管 | 156 轮真实接口对话、异步派单与主动回报、宿主 Herdr 隔离、执行器重启与消息重放 | [单机使用与验收](docs/stage1.md) · [长会话测试](tests/stage1_history.py) · [执行恢复测试](tests/executor_recovery.py) |
| 二 · 配对与同步 | 在独立进程间同步 S2 记忆，断线补传和重启对账，验证限时终端票据及工位交接 | [核心 CI](https://github.com/CollaalloC/Tarla/actions/workflows/check.yml) · [双网配置](docs/managed-networks.md) · [双节点延迟实测](docs/replication-latency.md) · [工位验收](tests/pinned.py) |
| 三 · 网关与客户端 | 对话、任务、审批和记忆的客户端接口，四个 IM 平台的入站和出站协议，以及原生应用构建和模拟器启动 | [客户端对真实守护进程的测试](apps/tarla-app/test/daemon_contract_test.dart) · [网关配置](crates/tarla-gateway/README.md) · [客户端构建与下载](https://github.com/CollaalloC/Tarla/actions/workflows/flutter-platforms.yml) |

- [Flutter 客户端及真机/模拟器连接](apps/tarla-app/README.md)
- [飞书、Telegram、Slack、Discord 网关的配置与配对](crates/tarla-gateway/README.md)
- [记忆后端、外部索引与离线行为](crates/tarla-memory/README.md)
- [运行状态、执行机权限与双网配置](crates/tarlad/README.md)
- [EasyTier、Tailscale 及节点入站地址设置](docs/managed-networks.md)
- [原始工程规范](docs/Tarla_Architecture_Spec.md)
- [上游组件与致谢](docs/upstreams.md)
