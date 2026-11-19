# tarla 跨平台个人智能体（Personal Agent）技术架构与工程实现方案

**产品名称**：`tarla`（命令行与客户端：`tarla`，后台无头核心守护进程：`tarlad`）  
**文档定位**：面向工程落地的完整架构规范，涵盖 Core/App 解耦、多工作空间硬隔离、**主管原生控制循环与 `BrainBackend` 可插拔模型回退链**、`Agent Slot` 四步漏斗调度、**无限长会话治理与双向“上下文防火墙”**、**S0/S1/S2 三层冷热分级记忆同步机制**、受管私有边车（Sidecar）零冲突集成、SQLite 表结构及 Rust Workspace 模块划分。

## 1. 产品定位、核心架构原则与交互边界

### 1.1 产品定位

`tarla` 是一个围绕**“人与具体事项”**持续工作的独立跨平台个人智能体系统。用户可以从手机、桌面客户端、命令行（CLI）或即时通讯渠道（飞书、Telegram、Discord、Slack 等）与同一个个人助手连续对话数周乃至数月，并按工作空间授权它调度自己的多台设备（常驻迷你主机、轻量云 VPS、macOS/Windows/Linux 工作站、GPU 服务器及边缘计算节点）。

底层远程持久终端、SSH 通道与异地组网能力复用 `HerdrOne` 既有技术积累与上游受管组件（[herdr-core](https://github.com/herdrdev/herdr-core)、[EasyTier](https://easytier.cn/guide/network/configurations.html)、[Tailscale](https://tailscale.com/docs/concepts/userspace-networking)），而顶层多工作空间隔离、消息网关归一化、技能加载、上下文压缩与可插拔记忆生命周期则参考 [Hermes Agent](https://github.com/NousResearch/hermes-agent/blob/main/LICENSE)（`MIT` 协议）的设计思想，使用 **Rust 原生重写**内化为 `tarla` 自身的核心模块。

### 1.2 三大核心架构铁律

1. **单机无头 Core 独立闭环**：任何一台机器只需安装无界面的 `tarlad`，配置好模型或本机智能体工位后，即可通过 `tarla` CLI 或消息渠道拥有完整的长会话对话、记忆、任务编排与权限审批体验；后续安装桌面或手机 App 仅相当于新增一个可视化视图入口，无需迁移数据。
2. **按工作空间显式指定唯一主管 Core**：每个隔离的工作空间（Workspace）在同一时刻仅有一个显式指定的**主管 Core（Supervisor Core）**负责维护该空间的对话顺序、全局任务账本、共享记忆协调与权限策略。首版采用“显式指定主管 + 手动迁移/恢复 + 主管离线期间允许已配对客户端直连执行机局部工作”，不做基于心跳超时的自动抢主，从根源上杜绝网络分区引发的双主并发下令。
3. **每次任务运行（`run_id`）由实际执行节点负责权威现场**：严格区分**“主管知道用户要求做什么（期望意图）”**与**“执行机知道本机到底做到了哪一步（真实进展）”**。执行机暂时失联时，主管仅将该 `run_id` 标记为 `unconfirmed`（暂时无法确认），绝不因看不见进度而擅自向另一台机器重复派发同一任务；主管重启恢复后主动向各执行机拉取对账。

### 1.3 围绕“人和事”的产品首页四入口

为拉开与纯底层机器/终端管理工具的产品心智差距，`tarla` 客户端首页固定保留四个一级入口，而将设备节点、虚拟网络路径与实时 PTY 终端下沉至具体任务的详情页：

| 首页一级入口 | 面向用户的核心问题 | 承载的核心视图与交互操作 |
| :--- | :--- | :--- |
| **对话（Conversations）** | “我要交代一件新事，或继续之前的话题” | 展示当前工作空间的无限连续对话流（跨飞书、Telegram、手机、桌面共享同一 `conversation_id`），支持直接绑定目标频道或动态切换接管角色 |
| **进行中的任务（Active Runs）** | “昨晚或刚才交代的事现在跑得怎么样了？” | 聚合展示活跃与近期完成的 `run_id` 进度、精简回执摘要及产物对比；点进详情页后才展示承载节点、网络路径及“实时接管终端”按钮 |
| **等我处理（Action Needed）** | “有哪些卡住的审批、异常或冲突需要我拍板？” | 集中汇集三类待办：① 高危工具/命令执行审批（`PreToolUse` / `OSC 7501 blocked`）；② 跨端修改引发的**记忆版本冲突候选单**；③ 某台机器上 `Agent Slot` 订阅登录态过期提醒 |
| **记忆（Memories）** | “助手记住了我哪些偏好、项目约定与机器环境？” | 分栏查看和修订当前工作空间的“S2 个人与项目长期记忆”（带版本号与跨节点同步状态）以及各配对设备上报的“机器与工作区环境事实摘要” |

## 2. 总体分层架构与 Core / App 安全解耦

### 2.1 四层能力与部署形态正交拆分

“四层纵向架构”定义了系统具备哪些技术能力，“Core / App / Gateway 横向拆分”定义了这些能力安装在哪些位置：

| 架构组件 | 部署位置 | 核心职责边界 |
| :--- | :--- | :--- |
| **`tarla Core` (`tarlad`)** | 常驻迷你主机、云 VPS、macOS / Windows / Linux 工作站、边缘计算板 | 同构单一 Rust 守护进程，按配置叠加**主管角色（Supervisor）**与**执行角色（Executor）**：管理工作空间身份、原生主管微循环、长会话摘要 DAG、S0/S1/S2 三层状态同步、配额感知调度器及本地 SQLite 库 |
| **`tarla App / CLI`** | iOS / Android 手机（Flutter）、桌面端（macOS / Windows / Linux）或命令行 | 无状态/轻状态视图端，展示同一份对话、任务进度、待处理审批与记忆；桌面 App 既可内置托管本机 `tarlad`，也可纯远程连接已有服务器上的 `tarlad` |
| **`tarla Gateway`** | 默认内嵌于主管 `tarlad` 同进程运行（亦支持独立部署通过 API 接入） | 使用 Rust 原生实现飞书、Telegram、Discord、Slack 等平台协议归一化、配对码鉴权、流式消息节流及交互式审批卡片回调；与 App 调用完全相同的 `tarla` 业务 API |
| **执行与双网边车适配器** | 安装在需要对应能力的 `tarlad` 所在节点私有目录中 | 由 `tarlad` 按私有命名空间托管 [herdr-core](https://github.com/herdrdev/herdr-core)（提供不死 PTY 与 `OSC 7501` 状态遥测）以及 [EasyTier](https://easytier.cn/guide/network/configurations.html) / [Tailscale](https://tailscale.com/docs/concepts/userspace-networking) 用户态穿透实例 |

### 2.2 接口安全边界与单终端票据接管（Scoped Attach Lease）

[herdr-core 官方安全文档](https://github.com/herdrdev/herdr-core/blob/master/docs/security.md)明确指出，其守护进程并非权限沙箱，任何能够连接到 `herdr-core` Socket 的客户端均直接拥有对应操作系统账户的任意命令执行、文件读写与全部终端控制权。因此，`tarla` 锁死以下两条安全边界：

1. **业务 API 与底层运行时物理隔离**：`tarla App`、`tarla CLI` 与 `tarla Gateway` 仅能访问 `tarlad` 暴露的受控业务接口（发消息、查任务、批复审批、查改记忆、申请接管票据），任何外部客户端均不可直接触达 `herdr-core` 的本地 Unix Socket 或 Windows Named Pipe。
2. **限时单终端接管票据（`attach_ticket`）**：当用户在任务详情页点击“打开实时终端”时，客户端先向 `tarlad` 校验用户权限并签发一张绑定到唯一 `(node_id, run_id, hc_terminal_id)`、短时有效（如 60 秒内完成握手）且带有模式范围（`observe` 只读旁观 / `interactive` 读写接管）的签名票据。客户端随后通过穿透网直连目标执行节点的 `tarlad` 终端代理端点；目标 `tarlad` 验票通过后，在宿主机内部通过本机私有 Socket 向 `herdr-core` 发起 `hello {"framed": true}` 流式 `attach`（遵循 [herdr-core 协议规范](https://github.com/herdrdev/herdr-core/blob/master/schema/README.md)），仅将该单一终端的字节流与窗口尺寸控制桥接给客户端 `xterm.dart`。

## 3. 多工作空间（Multi-Workspace）四位一体硬隔离

借鉴 [Hermes 多 Profile 隔离机制](https://hermes-agent.nousresearch.com/docs/user-guide/multi-profile-gateways)并将其下沉至虚拟网络层，`tarla` 以 **`Workspace`（工作空间）** 作为顶层物理隔离单元，满足“工作机器与个人生活网络彻底隔离、互不可见、零记忆串味”的核心诉求。

### 3.1 工作空间的四个隔离维度

* **专属虚拟穿透网络隔离**：`work` 工作空间拉起独立的办公网 `EasyTier` 实例（独立 `--network-name` 与密钥）或办公 `Tailscale` 状态实例；`life` 生活空间拉起独立的家庭 `EasyTier` 实例。不同工作空间分配不同的本地动态 SOCKS5 拨号端口，生活空间在网络路由层面物理无法触达办公设备。
* **可见设备池与主管独立性隔离**：每个工作空间拥有独立的设备配对表与自己的常驻主管节点（例如 `work` 空间以公司常开 Linux 主机为主管，`life` 空间以家里迷你主机为主管）。
* **角色设定（Profile）与记忆后端隔离**：每个工作空间维护独立的系统人设提示词（`SOUL.md` / Role Profile）、独立的本地 SQLite 库文件以及独立的外部记忆后端实例，彻底杜绝工作代码路径或内部术语混入私人生活对话。
* **消息渠道入口隔离**：飞书工作机器人绑定至 `work` 空间，个人 Telegram Bot 绑定至 `life` 空间；桌面与手机 App 顶部提供显式的工作空间切换器。

### 3.2 本地磁盘目录树规范

```text
~/.tarla/
├── config.toml                       # 本机全局守护进程基础配置
├── bin/                              # 受管私有边车二进制缓存（与宿主系统 PATH 完全隔离）
│   ├── toolchain-manifest.json       # 版本锁定清单与 SHA-256 校验值
│   ├── herdr-core/0.1.19/hc
│   ├── easytier/2.7.0/easytier-core
│   └── tailscale/1.82.0/tailscaled
├── slots/                            # 本机各订阅账号隔离凭证目录（防多账号串号）
│   ├── claude-work-max/              # 注入 CLAUDE_CONFIG_DIR
│   ├── claude-personal/              # 注入 CLAUDE_CONFIG_DIR
│   └── codex-pro-main/               # 注入 CODEX_HOME
└── workspaces/                       # 多工作空间硬隔离沙箱目录
    ├── work/
    │   ├── workspace.toml            # 工作空间角色、主管模型回退链、网络及记忆后端配置
    │   ├── SOUL.md                   # 当前工作空间主管专属人设与行为准则
    │   ├── state.db                  # 工作空间专属 SQLite（含本机 S2 记忆镜像，严格本地存储）
    │   ├── skills/                   # 工作空间专属 SKILL.md 技能包
    │   └── runtime/                  # 私有边车运行态句柄与动态端口记录
    │       ├── hc.sock               # 专属 herdr-core Socket
    │       ├── hc-state/             # 专属 XDG_STATE_HOME
    │       ├── easytier.rpc          # 专属 EasyTier 动态端口信息
    │       └── tailscaled.sock       # 专属 Tailscale Userspace Socket
    └── life/
        ├── workspace.toml
        ├── SOUL.md
        ├── state.db
        └── runtime/
```

### 3.3 多工作空间、主管大脑链与上下文预算配置示例（`workspace.toml`）

```toml
[workspace]
id = "work"
display_name = "研发工作空间"
role_mode = "supervisor_and_executor" # supervisor_only | executor_only | supervisor_and_executor
supervisor_node_id = "office-mini-pc"
supervisor_term = 3

[supervisor.brain]
max_tool_steps = 5                    # 单次唤醒最多允许 5 步内部元工具调用，防死循环
fallback_chain = [
    "http:anthropic-sonnet",          # 首选：直连高性价比 HTTP API（毫秒级响应 + 高命中缓存）
    "http:lan-ollama-qwen3",          # 次选：穿透网内工作站暴露的本地开源推理服务（零账单）
    "slot_borrow:claude-max-slot",    # 兜底：无状态单轮借用空闲包月订阅工位（零额外 API 费用）
]

[gateway.feishu]
enabled = true
app_id = "cli_xxxxxx"
default_mode = "orchestrator"         # 私聊默认走主管协调器，群话题支持直通绑定

[network]
mode = "managed_sidecar"              # managed_sidecar（默认零冲突）| reuse_host_network
enable_easytier = true
easytier_network_name = "corp-dev-mesh"
easytier_network_secret_ref = "keychain:tarla/work/et_secret"
enable_tailscale = false

[context_policy]
supervisor_active_budget_tokens = 24000   # 主管活跃对话组装预算上限，防上下文膨胀
compaction_high_watermark_pct = 75        # 超过预算 75% 在回合间隙触发无损 DAG 折叠
protected_verbatim_turns = 8              # 最近 8 轮对话始终保留一字不差原文
mission_packet_max_tokens = 1800          # 派发给下游子 Agent 的任务简报硬上限
run_receipt_max_tokens = 500              # 子 Agent 回传主管会话的结果回执硬上限

[memory]
primary_backend = "mnemosyne"             # builtin_sqlite | mnemosyne | honcho | mem0
mnemosyne_db_path = "~/.tarla/workspaces/work/mnemosyne.db"
prefetch_token_budget = 1200              # 会话启动前冻结注入上限，保护 Prefix Cache

[scheduler.roles]
architect = ["claude-max-slot", "codex-pro-slot"]
implementer = ["codex-pro-slot", "claude-max-slot"]
reviewer = ["gemini-cli-slot", "claude-max-slot"] # 强制异构模型交叉审查
distiller = ["local-qwen-slot", "gemini-cli-slot"]
```

## 4. 主管角色原生实现机制、`Agent Slot` 工位模型与四步漏斗调度

### 4.1 主管角色（Supervisor）的设定与原生控制循环实现

主管**绝不直接绑定像 Claude Code 或 Codex 那样的外部重型编程 CLI 会话**（重型 CLI 自带上万字单机写代码系统提示词与本地 `Bash` / `Edit` 文件修改工具，不仅容易误改主管所在低功耗宿主机的文件，且其封闭黑盒上下文无法接入 `tarla` 的无损摘要 DAG 与 24K 窗口预算治理）。

相反，主管在架构中被设定为**“剥离了本地读写手脚、只握有全局账本与调度大印的幕僚长”**（遵循 [Claude Code 官方关于主控与子智能体工具权限分离的设计原则](https://code.claude.com/docs/en/agent-sdk/subagents)），由 `tarlad` 进程内部用 Rust 原生实现为一个**事件驱动的极简控制面智能体循环（`SupervisorEngine`）**：

#### 1. 主管专用的 6 个内部控制面元工具（无本地 `Bash` / `WriteFile`）

| 内部元工具名称 | 执行位置与耗时 | 核心职责与返回结果 |
| :--- | :--- | :--- |
| **`list_fleet_and_runs`** | 主管本地 SQLite（毫秒级） | 查询当前工作空间所有配对设备在线状态、`Agent Slot` 剩余配额百分比及活跃/近期 `run_id` 账本摘要 |
| **`dispatch_mission`** | 主管调度器异步派发（毫秒级） | 将主管提炼的目标、验收条件与精简背景打包为 `MissionPacket`，交由四步漏斗调度器挑出最优设备工位异步下发，**立即返回 `run_id` 结束本轮，绝不原地阻塞等待** |
| **`inspect_run_log`** | 按需向执行机 `S0` 分页拉取 | 当用户追问某次失败或运行中任务的具体报错细节时，按行号或关键词向目标执行机懒加载拉取受限片段 |
| **`session_recall`** | 主管本地 SQLite（毫秒级） | 通过全文检索或指定 `dag_node_id` 无损展开当前长会话几个月前被折叠的原始对话消息 |
| **`memory_manage`** | 主管本地 `S2` 记忆库 | 新增、修订或归档当前工作空间的个人偏好与项目约定（自动递增版本号并触发 `sync_seq` 跨节点镜像分发） |
| **`bind_channel`** | 主管本地路由表 | 将当前飞书话题或 Telegram Topic 切换为直通绑定某台机器的指定工作目录与工位（或解除直绑恢复总管模式） |

#### 2. `BrainBackend` 可插拔模型回退链（不绑死单一模型，支持零额外账单模式）

主管循环通过统一的异步 `BrainBackend` Trait 调用底层推理能力，支持将以下三种供给方式配置为优先级回退数组（`fallback_chain`），主模型遇 `429` 限流或断网时自动降级切换：

| 模型供给方式 | 典型后端示例 | 延迟与成本特征 | 底层适配器实现机制 |
| :--- | :--- | :--- | :--- |
| **1. 轻量直连 HTTP API（推荐首选）** | Anthropic Sonnet/Haiku、Gemini Flash、DeepSeek、OpenRouter | 首字百毫秒级；因窗口静态前缀稳定，可吃满 90% 以上的 Prompt Cache 缓存折扣（见 [Hermes 缓存优化机制](https://hermes-agent.nousresearch.com/docs/developer-guide/context-compression-and-caching)），日均几十轮仅数美分 | `tarlad` 内置基于 `reqwest` 的纯 Rust OpenAI / Anthropic 兼容流式 Function-Calling 客户端 |
| **2. 内网本地开源推理服务** | 穿透网内工作站暴露的 Ollama / vLLM / LM Studio 端点 | **0 账单、数据 100% 不出私有网**；主管迷你主机本身无需显卡即可远程调用工作站算力 | 复用同一套 OpenAI 兼容 HTTP 客户端经由私有 SOCKS5 隧道直连内网模型端口 |
| **3. 无状态借用包月订阅 CLI（零额外账单兜底）** | 借用已注册的 Claude Pro/Max、Codex 或免费 Gemini CLI 工位 | **0 额外 API 充值账单**（直接复用已有包月池），单次进程冷启动比 HTTP 慢约 1~2 秒 | 参考 [protonmob/shiftover](https://glama.ai/mcp/servers/protonmob/shiftover) 与 [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) 思路，以非交互单轮无状态模式（如 `claude -p --output-format json --tools ""` 禁用 CLI 内置写文件工具）动态挑配额最充裕的工位完成当次 JSON 决策 |

#### 3. 三段式异步驱动流水线与 `RunCompleted` 主动报喜唤醒

主管在 `tarlad` 中并非死循环轮询线程，而是由事件总线按需唤醒，遵循以下核心代码骨架：

```rust
#[async_trait::async_trait]
pub trait BrainBackend: Send + Sync {
    async fn generate_turn(
        &self,
        messages: &[ChatMessage],
        control_tools: &[ToolSpec], // 仅暴露上述 6 个内部控制面元工具
    ) -> Result<BrainTurnOutput, BrainError>;
}

pub enum SupervisorTrigger {
    UserMessage { conversation_id: String, text: String },
    RunCompleted { conversation_id: String, receipt: RunReceipt }, // 远端执行机完工推回回执时自动唤醒主管
}

impl SupervisorEngine {
    pub async fn handle_trigger(&self, trigger: SupervisorTrigger) -> Result<()> {
        // 阶段 1：0-Token 短路分流（已 Pinned 直绑话题、UI 审批按钮点击与斜杠命令直接由纯 Rust 处理）
        if let SupervisorTrigger::UserMessage { ref conversation_id, ref text } = trigger {
            if let Some(binding) = self.db.get_pinned_binding(conversation_id)? {
                return self.router.forward_direct(&binding, text).await;
            }
        }

        // 阶段 2：组装有界窗口 [SOUL.md + S2 冻结记忆] + [高阶摘要 DAG] + [最近 8 轮原文 + 本次触发载荷]
        let conv_id = trigger.conversation_id();
        let mut ctx = self.context_builder.assemble_bounded_window(conv_id, &trigger)?;

        // 阶段 3：受控微循环（硬限制 max_tool_steps = 5，支持按 fallback_chain 自动故障降级）
        for _step in 0..self.config.max_tool_steps {
            let reply = self.call_brain_with_fallback(&ctx).await?;
            if reply.tool_calls.is_empty() {
                // 生成最终自然语言回复（含远端任务完工后的主动对比汇报），落库并推送至飞书/TG/App
                self.gateway.deliver_assistant_reply(conv_id, &reply.text).await?;
                self.context_builder.compact_if_over_watermark(conv_id).await?;
                break;
            }
            let tool_results = self.execute_control_tools(&reply.tool_calls).await;
            ctx.extend_tool_round(reply, tool_results);
        }
        Ok(())
    }
}
```

凭借 **`SupervisorTrigger::RunCompleted` 异步唤醒机制**，当用户在飞书交代一项耗时两小时的任务后，主管调用 `dispatch_mission` 派单完毕即刻休眠释放连接；两小时后执行机通过 Outbox 推回精简 `RunReceipt`，事件总线自动唤醒主管核对用户最初交代的关注点，生成一段自然语言总结主动推送回飞书与手机客户端。

### 4.2 Gateway 会话的三层解耦与三种绑定模式

系统将入口与底层执行拆为三层映射：**渠道入口标识（`channel_thread_id`）→ 逻辑会话（`conversation_id`）→ 活跃执行绑定（`Active Binding` 四元组：`[workspace_id, node_id, workdir, slot_id + native_session_id]`）**，支持三种接管方式：

| 绑定模式 | 典型入口 | 路由与接管行为 |
| :--- | :--- | :--- |
| **总管协调模式（Orchestrator）** | 飞书/Telegram 机器人主私聊窗口、App 首页主对话 | 消息唤醒上述 `SupervisorEngine`；日常检索或状态查询主管直接调 `list_fleet_and_runs` 查账本回答；需要动用设备执行重任务时，调 `dispatch_mission` 编译精简 `MissionPacket` 派发给目标 `Agent Slot` |
| **频道直通模式（Pinned Binding）** | Telegram Forum Topic、Discord 子频道、飞书群话题 | 用户通过配置或 `bind_channel` 将话题固定到某台设备的指定目录与 `Agent Slot`；后续该话题内所有消息触发阶段 1 短路层，由主管 `0 Token` 直接透传至目标节点的原生 CLI 会话 |
| **会话内动态切流（Handoff）** | 同一对话内输入 `@codex`、`/switch` 或触发限流转移 | 保持顶层 `conversation_id` 连续不变，原工位输出结构化交接摘要后，将 `Active Binding` 原子切换至新机器或新 `Agent Slot` 继续推进 |

### 4.3 最小调度颗粒度：`Agent Slot`（智能体工位）

由于同一台机器可能安装了多个不同的 CLI Agent（如 Claude Code、Codex、Gemini CLI、本地开源模型），甚至同一种 CLI 绑定了多个不同额度的订阅账号，调度的最小单元定义为 **`Agent Slot`**：

* **独立凭证目录硬隔离**：借鉴 [stablyai/orca 多账号隔离经验](https://github.com/stablyai/orca/issues/20512)，执行节点 `tarlad` 为每个工位维护独立配置目录（启动进程时注入专属 `CLAUDE_CONFIG_DIR` 或 `CODEX_HOME`），既不触碰用户全局 `~/.claude`，也彻底杜绝同一机器多终端并发时互相覆盖 OAuth 登录态。
* **异构角色分工策略**：结合 [fable-delegator 委派模式](https://github.com/contains-studio/fable-delegator)与 [agora 跨模型互审机制](https://github.com/dgiunchi/agora)，将推理上限高的旗舰窗口（如 Claude Max 5 小时滚动配额）优先排给 `Architect` 规划；将多轮编译改错的体力活排给配额耐用的主力 `Implementer` 工位（如 Codex Pro）；将代码审查强制排给不同模型家族的 `Reviewer` 工位；将长会话摘要压缩与记忆提纯排给零边际成本的本地模型或免费配额工位（`Distiller`）。
* **配额窗口与认证状态精确遥测**：执行机心跳定期上报每个工位的 `(slot_id, account_id, rolling_5h_used_pct, weekly_used_pct, resets_at, health_status)`，严格区分 `healthy`、`rate_limited`（限流冷却至 `resets_at` 自动解封）与 `auth_expired`（OAuth 过期，挂起工位并推送至首页“等我处理”）。

### 4.4 四步漏斗调度器核心数据结构与算法（Rust 规范）

主管调度器在执行 `dispatch_mission` 时采用确定性的 **Filter → Score → Bind → Turn-Boundary Failover** 四步漏斗：

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum AgentRole {
    Architect,
    Implementer,
    Reviewer,
    Explorer,
    Distiller,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BillingTier {
    PrepaidSubscription, // 已付费包月订阅（沉没成本，最高优先消耗）
    FreeQuota,           // 平台免费额度（次优消耗）
    LocalZeroCost,       // 本机开源推理（零边际账单）
    PayAsYouGoApi,       // 按量计费 API（仅作最后兜底）
}

#[derive(Debug, Clone, PartialEq)]
pub enum SlotHealth {
    Healthy,
    CoolingDown { resets_at_unix: i64 },
    AuthExpired { message: String },
    NodeOffline,
}

#[derive(Debug, Clone)]
pub struct AgentSlotSnapshot {
    pub slot_id: String,
    pub node_id: String,
    pub driver: String,
    pub model_family: String,
    pub supported_roles: Vec<AgentRole>,
    pub node_capabilities: Vec<String>,
    pub billing_tier: BillingTier,
    pub window_used_pct: f32,
    pub window_resets_at: Option<i64>,
    pub active_runs: u32,
    pub health: SlotHealth,
}

#[derive(Debug, Clone)]
pub struct TaskRequirement {
    pub workspace_id: String,
    pub role: AgentRole,
    pub required_caps: Vec<String>,
    pub exclude_model_family: Option<String>,
}

pub fn select_best_slot<'a>(
    req: &TaskRequirement,
    candidates: &'a [AgentSlotSnapshot],
    now_unix: i64,
) -> Option<&'a AgentSlotSnapshot> {
    candidates
        .iter()
        .filter(|slot| {
            let health_ok = match &slot.health {
                SlotHealth::Healthy => true,
                SlotHealth::CoolingDown { resets_at_unix } => now_unix >= *resets_at_unix,
                _ => false,
            };
            let quota_safe = slot.window_used_pct < 95.0;
            let caps_matched = req
                .required_caps
                .iter()
                .all(|cap| slot.node_capabilities.contains(cap));
            let family_allowed = req
                .exclude_model_family
                .as_ref()
                .map_or(true, |ex| &slot.model_family != ex);
            health_ok && quota_safe && caps_matched && family_allowed
        })
        .max_by_key(|slot| {
            let role_score: i64 = if slot.supported_roles.first() == Some(&req.role) {
                400
            } else if slot.supported_roles.contains(&req.role) {
                250
            } else {
                50
            };
            let billing_score: i64 = match slot.billing_tier {
                BillingTier::PrepaidSubscription => 300,
                BillingTier::LocalZeroCost => 260,
                BillingTier::FreeQuota => 220,
                BillingTier::PayAsYouGoApi => 80,
            };
            let urgency_bonus: i64 = slot
                .window_resets_at
                .map(|reset| {
                    let hours_left = ((reset - now_unix).max(0) / 3600).min(168);
                    (168 - hours_left) / 2
                })
                .unwrap_or(0);
            let headroom_score: i64 = (100.0 - slot.window_used_pct) as i64;
            let load_penalty: i64 = (slot.active_runs as i64) * 120;

            role_score + billing_score + urgency_bonus + headroom_score - load_penalty
        })
}
```

运行中途遭遇 `429` 限流时，遵循 [stablyai/orca 额度准入边界](https://github.com/stablyai/orca/issues/21747)与 [protonmob/shiftover 续接方案](https://glama.ai/mcp/servers/protonmob/shiftover)：等待当前回合停在安全点后将该工位标为 `CoolingDown`，优先在同机备用账号原地 `--resume`，否则生成交接包切往次优候补工位。

### 4.5 异常掉线与主管迁移约定

* **主管节点掉线**：各执行机上已启动的 `run_id` 继续独立运行并将进度写入本地待发送队列；若用户手机已与某台执行机完成配对，App 自动降级为直连该执行机查看现场或接管终端，并在顶部显著提示“已直连执行节点，主管离线（全局记忆写入与跨机新任务暂挂）”。
* **执行节点掉线**：主管在超时未收到节点心跳后，将该节点上的活跃运行标为 `unconfirmed` 并保留原 `run_id`，严禁自动向其他机器补发相同写操作任务。
* **计划迁移与显式恢复主管**：计划内迁移时，旧主管停止接收新请求，将 `state.db` 快照与最后确认的各节点事件游标移交新主管，并将 `supervisor_term` 加一广播给所有执行节点。若旧主管意外损坏需强行启用新主管，用户执行显式恢复命令递增 `supervisor_term`；此后即便旧主管恢复联网，执行节点检测到其携带过期的 `supervisor_term` 也会立即拒收指令并促使其自动降级。

## 5. 无限长会话上下文治理与双向“上下文防火墙”

如果用户与主管连续对话数月，或者把主管的完整聊天历史直接塞给下游每台执行机器的 Agent，必然会导致**上下文爆炸、推理变傻（Context Rot）、Token 翻倍烧干以及 Prefix Cache 频繁失效**。`tarla` 通过以下三道机制彻底根治长会话膨胀：

### 5.1 主管自身长会话防爆炸：无损摘要有向无环图（Lossless Summary DAG）+ 水位线批次压缩

结合 [Hermes 上下文压缩与缓存机制](https://hermes-agent.nousresearch.com/docs/developer-guide/context-compression-and-caching)及 [Lossless Context Management (LCM) 无损分层摘要架构](https://github.com/Martian-Engineering/volt)（参见 [LCM 论文 arXiv:2605.04050](https://arxiv.org/html/2605.04050v1)），主管对一条持续数月的 `conversation_id` 采取**“底层数据库全量不可变存档 + 活跃窗口分层组装”**：

1. **有界活跃窗口组装公式**：每次唤醒 `SupervisorEngine` 时，送入模型的绝不是整张 `messages` 表，而是严格控制在 `supervisor_active_budget_tokens`（如 24K Tokens）以内的四段式结构：
   * **段 1（静态前缀）**：工作空间 `SOUL.md` 人设 + 当前冻结记忆快照（恒定不变，100% 命中模型 Prefix Cache）；
   * **段 2（高层摘要 DAG 节点）**：由历史对话逐级浓缩成的结构化摘要树（每节点保留指向原始消息区间 `start_seq..end_seq` 的无损外键指针）；
   * **段 3（受保护近期原文尾部）**：最近 `protected_verbatim_turns`（如最近 8 轮）的一字不差原始对话，保证当前讨论语境零折损；
   * **段 4（按需无损回溯工具）**：通过主管的 `session_recall` 元工具，仅在用户追问数月前某个精确细节而摘要未涵盖时，精准展开对应那几条原始消息。
2. **水位线批次折叠（保护 Prefix Cache）**：正如 [Hermes Context Engine 文档](https://hermes-agent.nousresearch.com/docs/developer-guide/context-engine-plugin)所验证，每轮对话动态改写前文做微压缩会导致前缀哈希每轮变化而丧失 Prompt Cache 折扣。因此 `tarla` 在水位低于 `75%` 时**只做末尾追加（Append-Only）**；仅当超过 `75%` 水位线时，在回合结束后的空闲间隙唤醒零成本的 `Distiller` 工位（或采用类似 [Hermes VCC 确定性提取器](https://hermes-agent.nousresearch.com/marketplace/hermes-vcc-engine)）一次性将中段旧消息折叠为一个新的 DAG 摘要节点。

### 5.2 跨 Agent 双向“上下文防火墙”（Context Firewall）：拒绝传递累计冗余历史

参考 [Claude Code 子智能体上下文隔离规范](https://code.claude.com/docs/en/agent-sdk/subagents)，主管与下游各设备的执行 Agent 之间设立严格的**双向隔离墙**：

| 方向 | 传输载体 | 严格体积上限 | 包含的内容与裁剪规则 | 避免的冗余灾难 |
| :--- | :--- | :--- | :--- | :--- |
| **下行派单裁剪（主管 → 执行工位）** | **`MissionPacket`（任务委派简报）** | `800 ~ 1,800 Tokens` | ① 本次明确目标与验收标准；② 从主管会话 DAG 与 S2 记忆中按相关性检索出的 **3~5 条核心历史决策**（剔除所有闲聊、寒暄与无关旧话题）；③ 目标工作区标识与待复核文件路径；④ 该项目作用域下的精简长期记忆快照 | 哪怕你跟主管聊了 1,000 轮，派给工作站 Codex 或 Mac Claude 的开局上下文依然只有不到 2K Tokens，注意力极度集中且秒级响应 |
| **上行结果折叠（执行工位 → 主管）** | **`RunReceipt`（运行结构化回执）** | `200 ~ 500 Tokens` | ① 最终状态（成功/失败/挂起）；② 核心结论与关键变更摘要；③ 产物路径、`git diff` 统计及指标对比；④ 指向执行机本机 `S0` 完整日志的 `run_id` 引用句柄 | 执行 Agent 在本机读了 50 个源码文件、跑了 15 轮编译排错产生的 **80K+ Tokens 脏轨迹全部留在执行机本地 `S0`**，绝不倒灌污染主管会话窗口 |

```rust
/// 主管派发给具体设备 Agent Slot 的有界任务委派简报（下行防火墙载荷）
#[derive(Debug, Clone)]
pub struct MissionPacket {
    pub run_id: String,
    pub workspace_id: String,
    pub objective: String,                  // 本次任务的一句话核心目标
    pub acceptance_criteria: Vec<String>,   // 明确可验证的完成标准
    pub distilled_context: Vec<String>,     // 仅摘录与本任务直接相关的历史结论（<= 5 条）
    pub project_memory_snapshot: String,    // 冻结的项目长期记忆片段（保 Prefix Cache）
    pub required_fact_keys: Vec<String>,    // 要求执行机开跑前本机复核的环境事实键
}

/// 执行机完成后回注给主管会话的精简回执（上行防火墙载荷）
#[derive(Debug, Clone)]
pub struct RunReceipt {
    pub run_id: String,
    pub final_state: String,                // "succeeded" | "failed" | "cancelled"
    pub executive_summary: String,          // <= 500 Tokens 的结果与变更摘要，直接唤醒主管汇报
    pub artifact_refs: Vec<String>,         // 产物或报告引用，详情按需从执行机 S0 懒加载
    pub s0_transcript_pointer: String,      // 如 "node:ubuntu-ws/run:0193.../transcript.jsonl"
}
```

## 6. S0 / S1 / S2 三层冷热分级记忆与跨机同步机制

根据 [SQLite WAL 官方文档](https://www.sqlite.org/wal.html)约束，多个节点的数据库绝不可放在网络文件系统上共享写入。为了让多台设备既能拥有毫秒级本地读取速度与离线自治能力，又能跨设备无感共享长期经验，`tarla` 将全系统状态与记忆划分为 **S0 / S1 / S2 三个冷热与权威等级**，分别采用最匹配的同步协议：

### 6.1 S0 / S1 / S2 三层分级同步架构总表

| 同步层级 | 涵盖的数据内容 | 权威归属方 | 各节点存储策略 | 跨节点同步协议与冲突消解机制 |
| :--- | :--- | :--- | :--- | :--- |
| **S0 原始执行现场层**（重度冷/温大文件） | 原生 CLI 完整 JSONL 对话轨迹、工具原始海量输出、`herdr-core` PTY 屏幕缓冲、本地工作区未提交代码树 | **实际执行该任务的 `Executor Core` 本机独占权威** | **仅存执行节点本地磁盘**，绝不向主管或其他节点全量广播复制 | **按需流式懒加载（Lazy Pull）+ 显式交接包（Session Handoff）**：App 点开任务详情或主管调 `inspect_run_log` 时才按偏移量分页拉取；若需换机器接力未完任务，由源节点打包“进度摘要 + 未决问题 + `git diff` 补丁”定向发给目标节点拉起新会话 |
| **S1 全局路由与任务运行态层**（高频控制流） | 跨渠道会话历史（`messages`）、`conversation_id` 活跃绑定、工位配额心跳、`run_id` 状态变迁、待处理审批单 | **主管 `Supervisor Core` 维护全局序列与账本权威** | 执行机维护本机 `local_runs` 与断网待发队列 `outbox_events`；主管维护全局全集 | **Outbox 幂等事件上行 + 实时状态流下行**：执行机状态变化先落本机 `outbox_events`（UUIDv7），联网时推送给主管按 `event_id` 幂等去重并回填 `acked_at`；主管重启后主动携带最后已知游标向各执行机核对未完结的 `run_id` |
| **S2 提纯长期记忆与环境摘要层**（高价值轻量知识） | 用户全局偏好、跨机项目架构约定、排障经验卡片（单条数百字节）、各机器环境事实调度摘要 | **工作空间全局共享，主管裁决版本号，全节点最终一致** | **工作空间内每台 `tarlad` 本地 SQLite 均保有一份完整的 S2 镜像副本** | **增量游标复制（`sync_seq`）+ HLC 混合逻辑时钟 + 乐观锁候选池**：任意节点在 `OSC 7501 done/idle` 后异步提纯出新记忆提案（携带 `base_version` 与 `hlc_ts`）上报主管；无版本冲突则由主管递增单调 `sync_seq` 广播并供离线节点重连后按 `last_sync_seq` 增量拉取；若落后于人工新修正则转入 `memory_candidates` 投递至首页“等我处理” |

### 6.2 为什么 S2 长期记忆采用“主管定序 + 全节点本地镜像”？

1. **笔记本合盖离线出门依然拥有完整记忆**：因为 S2 提纯后的长期记忆纯文本总量极小（通常仅数 MB），每台配对设备本地 `state.db`（或本地 [Mnemosyne](https://github.com/mnemosyne-oss/mnemosyne) 实例）里都有一份增量同步好的最新镜像。哪怕笔记本在外网隔离环境下连不上家里主管，本机 `tarlad` 依然能 1 毫秒从本地镜像检索项目约定并注入本机 CLI Agent；重新联网后再将离线期间产生的记忆提案提交主管对账合并。
2. **A 机踩坑、B 机秒级复用且不破坏 Prefix Cache**：工作站完成一轮任务触发 `OSC 7501` 空闲信号后，后台零成本 `Distiller` 工位提纯出一条排障经验上报主管，主管赋予新 `sync_seq` 推送到 Mac 本地镜像；下次在 Mac 上新开任务时，预取器直接从本机镜像读出该经验拼入开局冻结快照，无需跨网等待远程查询，也不会在运行中途插改已有会话前缀。
3. **人工修正永远优先于滞后的机器自动复盘**：长耗时任务启动时领取的记忆版本是 `v4`；若运行期间你在手机上把某条项目约定手动改成了 `v5`，等几小时后执行机带着 `base_version = 4` 的自动复盘建议回来时，主管检测到 `base_version < current_version`，绝不静默覆盖你的手动修改，而是写入 `memory_candidates` 并在首页“等我处理”展示差异对比。

### 6.3 可插拔 `MemoryBackend` 接口契约

参考 [Hermes 记忆插件单激活后端规范](https://github.com/NousResearch/hermes-agent/blob/main/website/docs/developer-guide/memory-provider-plugin.md)，`tarla-memory` 定义统一的 Rust `MemoryBackend` Trait（包含 `prefetch_frozen_snapshot`、`propose_mutation`、`pull_delta_since(sync_seq)` 与 `search` 四个核心方法）：

* 默认推荐对接零外部依赖的本地 SQLite 记忆引擎 **[Mnemosyne](https://github.com/mnemosyne-oss/mnemosyne)** 或内置 SQLite FTS5 + 向量扩展，由 S2 增量日志负责跨节点镜像同步；
* 亦支持在特定工作空间切换为 **[Honcho](https://hermes-agent.nousresearch.com/docs/user-guide/skills/optional/autonomous-ai-agents/autonomous-ai-agents-honcho)** 或 **[Mem0](https://docs.mem0.ai/integrations/hermes)** 等云端托管记忆后端。

### 6.4 完整 SQLite Schema 建表清单（含长会话 DAG 与 S2 增量同步日志）

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 1. 统一逻辑会话与不可变原始消息归档表（S1 主管权威）
CREATE TABLE conversations (
    conversation_id TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    title           TEXT,
    binding_mode    TEXT NOT NULL DEFAULT 'orchestrator', -- orchestrator | pinned
    pinned_node_id  TEXT,
    pinned_slot_id  TEXT,
    pinned_workdir  TEXT,
    active_tokens   INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE messages (
    seq_no          INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    channel_type    TEXT NOT NULL, -- feishu | telegram | app | cli
    source_msg_id   TEXT NOT NULL,
    sender_role     TEXT NOT NULL, -- user | assistant | run_receipt
    content         TEXT NOT NULL,
    token_estimate  INTEGER NOT NULL DEFAULT 0,
    compacted_into  TEXT,          -- 若已被折叠进摘要 DAG，指向对应 dag_node_id（原文永不物理删除）
    run_id          TEXT,
    created_at      INTEGER NOT NULL,
    UNIQUE(channel_type, source_msg_id)
);

-- 2. 长会话无损分层摘要 DAG 表（支撑无限长会话与按需无损展开）
CREATE TABLE context_dag_nodes (
    dag_node_id     TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    depth_level     INTEGER NOT NULL DEFAULT 1, -- 1: 原始消息块摘要, 2+: 高阶合并摘要
    start_seq_no    INTEGER NOT NULL,           -- 覆盖的原始消息起始序号（支持无损回溯）
    end_seq_no      INTEGER NOT NULL,           -- 覆盖的原始消息结束序号
    parent_dag_id   TEXT REFERENCES context_dag_nodes(dag_node_id),
    summary_text    TEXT NOT NULL,
    key_entities    TEXT NOT NULL,              -- 保留精确文件路径、命令与决策关键词供检索
    token_estimate  INTEGER NOT NULL,
    created_at      INTEGER NOT NULL
);

-- 3. S2 提纯长期记忆镜像表、乐观锁候选表与增量同步游标索引
CREATE TABLE memories (
    memory_id       TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    scope           TEXT NOT NULL,              -- global | project:<slug>
    content         TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    hlc_timestamp   TEXT NOT NULL,              -- 混合逻辑时钟 (物理时间 + 逻辑计数 + node_id)
    updated_by      TEXT NOT NULL,              -- user_manual | run:<run_id>
    is_deleted      INTEGER NOT NULL DEFAULT 0,
    sync_seq        INTEGER NOT NULL,           -- 主管分配的工作空间单调递增同步游标
    updated_at      INTEGER NOT NULL
);

CREATE INDEX idx_memories_sync_seq ON memories(workspace_id, sync_seq);

CREATE TABLE memory_candidates (
    candidate_id    TEXT PRIMARY KEY,
    memory_id       TEXT REFERENCES memories(memory_id),
    workspace_id    TEXT NOT NULL,
    proposed_text   TEXT NOT NULL,
    base_version    INTEGER NOT NULL,
    hlc_timestamp   TEXT NOT NULL,
    source_node_id  TEXT NOT NULL,
    source_run_id   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | merged | dismissed
    created_at      INTEGER NOT NULL
);

-- 4. 机器与工作区环境事实表（执行机本机权威，通过 S2 同步轻量能力摘要）
CREATE TABLE workspace_facts (
    node_id         TEXT NOT NULL,
    fact_key        TEXT NOT NULL,
    local_path      TEXT NOT NULL,
    metadata_json   TEXT NOT NULL,
    verified_at     INTEGER NOT NULL,
    PRIMARY KEY (node_id, fact_key)
);

-- 5. S1 任务运行账本与 S0 本机现场指针表（两阶段幂等认领）
CREATE TABLE runs (
    run_id              TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    conversation_id     TEXT NOT NULL,
    supervisor_term     INTEGER NOT NULL,
    target_node_id      TEXT NOT NULL,
    target_slot_id      TEXT NOT NULL,
    desired_state       TEXT NOT NULL, -- running | cancelled
    observed_state      TEXT NOT NULL, -- dispatch_pending | running | blocked | succeeded | failed | unconfirmed
    mission_packet_json TEXT NOT NULL, -- 派单时的裁剪后任务简报（<= 1800 Tokens）
    hc_terminal_id      TEXT,
    native_session_ref  TEXT,
    s0_log_local_path   TEXT,          -- 仅执行机本机有效的大体量原始 JSONL 轨迹路径
    memory_snapshot_ver INTEGER,
    receipt_summary     TEXT,          -- 上行回注的精简结果回执（<= 500 Tokens）
    artifacts_json      TEXT,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
);

-- 6. S1 执行侧断网待发送队列（Outbox 幂等补传）与审批单表
CREATE TABLE outbox_events (
    event_id        TEXT PRIMARY KEY,  -- UUIDv7
    run_id          TEXT,
    event_type      TEXT NOT NULL,     -- state_changed | approval_requested | run_receipt | s2_memory_propose
    payload_json    TEXT NOT NULL,
    created_at      INTEGER NOT NULL,
    acked_at        INTEGER
);

CREATE TABLE approvals (
    approval_id     TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    tool_or_cmd     TEXT NOT NULL,
    risk_reason     TEXT NOT NULL,
    resolution      TEXT NOT NULL DEFAULT 'pending', -- pending | approved | rejected
    resolved_by     TEXT,
    created_at      INTEGER NOT NULL,
    resolved_at     INTEGER
);
```

## 7. 上游工具源码边界、平滑升级与“宿主原生工具零冲突”规范

### 7.1 源码关系与开源协议合规策略

面对跨语言且开源许可证各异的上游组件，统一遵循**“MIT 核心逻辑用 Rust 原生重写内化，底层基础设施走独立进程私有边车（Managed Sidecar）”**的清晰边界：

| 上游组件 | 上游语言与协议 | 在 `tarla` 中的集成形态 | 架构与合规收益 |
| :--- | :--- | :--- | :--- |
| **[Hermes Agent](https://github.com/NousResearch/hermes-agent/blob/main/LICENSE)** | Python / `MIT` | **不捆绑 Python 运行时**；借鉴其多 Profile、Gateway 归一化、上下文水位压缩、`MemoryProvider` 与 `SKILL.md` 规范，用 Rust 原生重写进 `tarla` 主仓库（附带协议致谢） | 彻底消除跨语言打包体积膨胀，单二进制极速启动，100% 掌控核心业务状态机 |
| **[herdr-core](https://github.com/herdrdev/herdr-core)** | Rust / `Apache-2.0` | 作为**受管私有边车二进制**独立进程运行，`tarlad` 通过本机私有 Unix Socket / Named Pipe 走 OpenRPC 协议调用 | 终端持有进程（Holder）与 `tarlad` 升级周期解耦，即使重启 `tarlad` 也不会杀死正在运行的底层终端 |
| **[EasyTier](https://github.com/EasyTier/EasyTier?utm_source=selfhostedworld.com&utm_medium=referral&ref=selfhostedworld.com)** | Rust / `LGPL-3.0` | **严禁静态编译进主程序**；作为独立子进程边车（`easytier-core`）拉起，通过本地回环 SOCKS5 与 RPC 端口通信 | 严格遵守 `LGPL-3.0` 进程隔离边界，避免 Rust 默认静态链接触发主仓库传染性开源义务 |
| **[Tailscale](https://github.com/tailscale/tailscale-rs/blob/main/LICENSE)** | Go / `BSD-3-Clause` | 作为可选受管私有边车（`tailscaled` 用户态模式）按需下载拉起，或按配置直接复用宿主已有连接 | 免去 Go 与 Rust 混合交叉编译负担，升级时直接原子替换官方预编译静态二进制 |

### 7.2 与用户电脑已安装原生工具 100% 零冲突启动参数表

当用户电脑上原本已经安装了原生的 [Herdr 桌面端](https://herdr.dev/)、正连着公司内网的原生 Tailscale 客户端、或原生 EasyTier 管理器时，`tarlad` 通过强制注入私有路径、用户态无网卡模式与随机动态端口，确保 **零网卡争抢、零端口冲突、零配置污染**：

| 受管组件 | 原生软件默认占用的系统资源 | `tarlad` 拉起私有边车时的强制隔离参数 | 平台特殊注意事项 |
| :--- | :--- | :--- | :--- |
| **`herdr-core`** | `/run/user/<uid>/herdr-core/` 默认 Socket 与状态目录 | 注入环境变量 `HC_SOCKET=~/.tarla/workspaces/<ws>/runtime/hc.sock`、`XDG_STATE_HOME=~/.tarla/workspaces/<ws>/runtime/hc-state`、`XDG_CONFIG_HOME=...`（见 [herdr-core 端点隔离规则](https://github.com/herdrdev/herdr-core/blob/master/docs/security.md)） | macOS 下打包桌面版时，须为守护辅助程序配置独立 Developer ID 签名与 `SMAppService` 注册，规避 [herdr-core Issue #83](https://github.com/herdrdev/herdr-core/issues/83) 披露的 `launchd` 裸二进制局域网 TCC 权限拦截问题 |
| **`easytier-core`** | 系统内核 TUN 虚拟网卡及默认 `15888` 本地管理端口 | 传入 `--no-tun --socks5 127.0.0.1:0 --rpc-portal 127.0.0.1:0`（见 [EasyTier 完整配置说明](https://easytier.cn/guide/network/configurations.html)），由 `tarlad` 读取实际绑定的空闲端口写入 `runtime/` | 纯用户态 `smoltcp` 协议栈运行，无需 `root` / 管理员提权，单机同时开启多个工作空间的隔离虚拟网亦互不冲突 |
| **`tailscaled`** | 系统内核 TUN 网卡及 `/var/run/tailscale/tailscaled.sock` | 传入 `--tun=userspace-networking --state=~/.tarla/workspaces/<ws>/runtime/ts.state --socket=~/.tarla/workspaces/<ws>/runtime/ts.sock --socks5-server=127.0.0.1:0`（见 [Tailscale 多实例并存指南](https://tailmux.app/guides/multiple-tailnets-linux)） | 不改动宿主路由表与 DNS；若用户已在 `workspace.toml` 显式开启 `reuse_host_network`，则直接走宿主虚拟网 IP，不额外启动边车进程 |
| **原生编程 CLI（Claude / Codex 等）** | 用户全局命令路径与 `~/.claude`、`~/.codex` 默认目录 | 直接调用用户已安装的 CLI 二进制，但按选中的 `Agent Slot` 注入独立的 `CLAUDE_CONFIG_DIR` 或 `CODEX_HOME`（或显式勾选只读复用默认单账号），并通过命令行参数挂载单次 `MissionPacket` | 绝不篡改用户全局 `settings.json` 或 Hooks 文件，用户自己开终端手敲 `claude` 体验完全不受影响 |

## 8. Rust Workspace 模块划分与首版（V1）落地路线图

### 8.1 推荐 Cargo Workspace 工程结构

```text
tarla/
├── Cargo.toml
├── crates/
│   ├── tarlad/             # 无头守护进程入口（组装主管角色、执行角色、本地 IPC 与远程 RPC 服务）
│   ├── tarla-cli/          # 命令行客户端（对话、任务查看、审批处理、终端接管、节点配对）
│   ├── tarla-protocol/     # 业务 API、MissionPacket/RunReceipt 防火墙契约、Outbox 事件与接管票据签名
│   ├── tarla-workspace/    # 多工作空间目录沙箱、SOUL.md 人设加载、SKILL.md 解析与配置管理
│   ├── tarla-supervisor/   # 原生主管微循环（0-Token 短路层、6 个控制面元工具、RunCompleted 唤醒与 BrainBackend 回退链）
│   ├── tarla-context/      # 长会话无损摘要 DAG 压缩引擎、水位线监控与 MissionPacket 上下文裁剪编译器
│   ├── tarla-scheduler/    # Agent Slot 注册表、配额健康追踪与四步漏斗调度器（Filter/Score/Failover）
│   ├── tarla-memory/       # S0/S1/S2 三层状态存储、HLC/sync_seq 增量镜像同步、乐观锁候选单与 Mnemosyne 适配
│   ├── tarla-gateway/      # Rust 原生重写的飞书、Telegram、Discord、Slack 渠道归一化与审批卡片适配器
│   └── tarla-sidecar/      # 受管边车管理器（herdr-core OpenRPC 客户端、EasyTier/Tailscale 用户态拨号器）
└── apps/
    └── tarla-app/          # Flutter 跨平台移动与桌面客户端（围绕“对话/进行中/等我处理/记忆”四入口 + xterm.dart）
```

### 8.2 首版（V1）分阶段实施与验收清单

1. **阶段一：单机无头 `tarlad` + 原生 `tarla-supervisor` + 长会话 DAG 最小闭环**
   * 完成 `workspaces/<ws>/` 目录隔离、SQLite 全套核心表迁移与受管私有 `herdr-core` 边车零冲突封装。
   * 跑通 `tarla-supervisor`（6 个控制面元工具 + `BrainBackend` 回退链 + `RunCompleted` 异步唤醒）、`tarla-context`（无损摘要 DAG + `MissionPacket` / `RunReceipt` 双向裁剪）与单机 `Agent Slot` 注册。
   * **阶段验收标准**：单台无界面主机连续对话上百轮后上下文体积稳定在预算上限内，主管可异步派发任务并在完工后主动汇报，且与宿主原生工具零冲突。
2. **阶段二：跨机双核配对、四步漏斗调度与 S0/S1/S2 三层同步闭环**
   * 接入受管 `EasyTier --no-tun` 与 `Tailscale` 用户态拨号器，打通不同工作空间的网络硬隔离。
   * 实现两阶段 `run_id` 幂等认领、S1 `outbox_events` 断网补传、以及 S2 长期记忆基于 `sync_seq` + `HLC` 的全节点本地镜像增量同步与乐观锁候选池。
   * **阶段验收标准**：A 节点任务完成后提纯的 S2 经验毫秒级同步至 B 节点本地镜像；B 节点断网离线时仍可本机检索该记忆；中途重启主管不丢状态、不重派任务。
3. **阶段三：飞书/Telegram 原生 Gateway 与围绕“人和事”的四入口 App**
   * 上线 `tarla-gateway` 飞书与 Telegram 适配器，跑通跨入口同一 `conversation_id` 接续对话及频道 `Pinned` 直通绑定。
   * 交付桌面与手机客户端首页四个一级入口（对话、进行中的任务、等我处理、记忆），并在任务详情页打通基于限时 `attach_ticket` 的 P2P 实时终端接管。
