# **V2.0 · 二代事件微内核与原生编排详细技术方案及实施路线（herdr-core 零协议借脑、Dora/ROS 2 拓扑与 Codync 式跨端长会话同步）**

**所属文稿集**：[Tarla 产品架构演进文稿集（V1–V3）](https://drive.google.com/drive/folders/1WKb4fOdxGpK-fgOiSOJUMe7Kg94oPBDE)（总纲参见 [《00 · Tarla 产品整体演进全景图》](https://docs.google.com/document/d/1t_ettUkBaEK-2LcWKWbjshdzxkdn1Zv5CZ4uVvfqR5U/edit)，基线底座参见 [《V1.0 · 初代基线详细技术架构与工程落地规范》](https://docs.google.com/document/d/1QOCvgRNDXdq7JMIgz2sRLI6Bzbv9_wUbf0Qxn7NpleM/edit)，远期界面方案参见 [《V3.0 · 三代平台化与可编程界面方案（待批草案）》](https://docs.google.com/document/d/1f7ZxTRlolyZlL04ELndMbxe42aio-f64mWN8uXa92Lk/edit)）。  
**当前状态**：**当前主力推进目标版本（已定稿）**——基于 V1.0 已落地的 10 个核心 Rust 子 Crate 演进升级，彻底废弃早期草案中的 SlotPool、Master MCP 与 ACP 方案，确立可直接编码落地的完整技术规格与六阶段实施路线。

## **1\. V2.0 核心设计定型：五大关键技术取舍**

在 V1.0 完成分布式 SQLite 账本与私有边车底座后，V2.0 针对“主控循环耦合重、跨端会话易断、缺少事件级联协同与异构编排协议臃肿”四大痛点，做出了五项决定性的架构收敛：

| 核心决策问题 | 被否决/淘汰的旧路线及原因 | V2.0 最终定稿技术路线 |
| :---- | :---- | :---- |
| **1\. 如何实现异构 Agent 编排与 Master“借脑”？** | 淘汰 V1.0 SlotPool（强绑 Claude CLI 私有标志位）；同时否决 **MCP**（工具 Schema 占用大量上下文、多轮 RPC 慢）与 **ACP**（适配器链路重、各家 CLI 支持参差不齐） | **直接释放 [herdr-core](https://github.com/herdrdev/herdr-core) 原生“Agent 调 Agent”编排能力 \+ 极简 tarla ctl CLI**：任意 CLI 运行在真实 hc 终端模板中，直接用 hc run \--bg、hc send、hc wait \--children、\<hc from="..."\> 父子通知与 JSON 行输出完成零协议开销调度 |
| **2\. 如何引入机器人节点事件驱动网络？** | 否决直接运行原生 \`dora-daemon\` 进程（Zenoh 多播与 Apache Arrow 共享内存无离线持久化，且无法穿越单端口用户态 SOCKS5 网格） | **用纯 Rust 提取 [Dora-rs](https://github.com/dora-rs/dora) 声明式 \`botflow.yml\` 数据流图与 [ROS 2 Actions](https://design.ros2.org/articles/actions.html) 七态机**，直接构建在 Tarla 已有的 SQLite S1 Outbox 与受管双网底座之上 |
| **3\. 如何做到跨平台同步与无感切换底层 Client？** | 淘汰 V1.0 将对话生硬绑定在固定机器工位或单一 CLI 上的静态映射 | **吸收 [Codync](https://github.com/leepokai/Codync) 的跨端同步与长会话保持机制（剥离其 ACP 层）**：确立 Conversation（SQLite 永久对话流）≠ Terminal（可回收/可换脑的 hc 终端），引入全局单调游标 rev（since: rev）与纪元锁定双层记忆 |
| **4\. 谁掌握全网最高控制权与信息权？** | 调整 V1.0 仅以固定桌面机为中心的模式（桌面机未必随身，但在手机后台跑重型推理循环又受系统休眠限制） | **手机担任随身最高权限遥控与审批端（Master Control）**，算力节点上的 tarlad \+ herdr-core 承载 Master Bot 执行载体，既有随身最高裁决权又无需自研复杂推理循环 |
| **5\. 如何防止多 Bot 自治失控或破坏代码现场？** | 淘汰仅靠单步超时与进程退出码的粗放容错 | **合入主流开源 Personal Agent 五大实战护栏**：\!acted/acted 副作用门控、Check-Before-Done 异构复核、8,000 字符输出截断、SHA-256 冲突感知回滚快照与 \--operation-id 零明文凭据代理 |

## **2\. 四层运行实体模型与单机多开 \`herdr-core\` 进程隔离**

V2.0 废除 V1.0 粗粒度的 \`Agent Slot\` 概念，将全网拓扑拆分为正交的四层实体，并在宿主机内支持按安全域多开 \`herdr-core\` 守护进程：

```
~/.tarla/workspaces/<ws>/
├── workspace.toml                    # 节点基础配置、多核声明与跨网对等节点
├── botflow.yml                       # 声明式多机 Bot 角色、事件订阅端口与验证策略
├── state.db                          # 本地 SQLite WAL（新增全局 rev 游标与 Bot 会话账本）
└── runtime/
    ├── cores/                        # V2.0 新增：单机多开 herdr-core 进程隔离组
    │   ├── code_core/                # 研发代码域独立守护进程
    │   │   ├── hc.sock               # 0700 私有 Socket（超 96 字节自动回退短路径）
    │   │   ├── hc-state/             # 专属 XDG_STATE_HOME (HC_SESSION=tarla-<ws>-code_core)
    │   │   └── hc-config/herdr-core/
    │   │       ├── config.toml       # 配置 notes_quiet=3, draft_wait=60
    │   │       └── templates/        # 动态生成的角色模板 (*.md)
    │   └── ops_core/                 # 运维与高危部署域独立守护进程（崩溃/挂死互不影响）
    │       ├── hc.sock
    │       └── hc-state/
    └── checkpoints/<run_id>/         # V2.0 新增：SHA-256 冲突感知文件快照基线
```

| 架构层级 | 核心实体 | 核心职责 | 物理隔离与安全机制 |
| :---- | :---- | :---- | :---- |
| **第 1 层：设备节点层** | Node | 手机随身总控端（master\_control）与 Linux/macOS/Windows 常驻算力节点（executor） | 独立本地 state.db、双向配对密钥、EasyTier \--no-tun / Tailscale 用户态 SOCKS5 网格 |
| **第 2 层：进程容器层** | Herdr Core Group | 同一台算力机按风险域划分的多个独立 herdr-core 实例（如 code\_core、ops\_core、master\_core） | 独立 HC\_SOCKET（遵循 [herdr-core 安全规范](https://github.com/herdrdev/herdr-core/blob/master/docs/security.md)）+ 独立 HC\_SESSION \+ 独立进程树 |
| **第 3 层：模板与引擎层** | Agent Template | 定义在 templates/\<name\>.md 中的任意 CLI 启动契约（Claude Code、Codex、Gemini CLI、Pi、OpenCode 或自定义程序） | 按账号隔离 CLAUDE\_CONFIG\_DIR / CODEX\_HOME / GEMINI\_CLI\_HOME，完全解耦特定厂商绑定 |
| **第 4 层：逻辑角色层** | Bot | 用户心智与 botflow.yml 中的一等公民（如 chief、architect、implementer、reviewer），拥有永久对话流与事件端口 | 绑定有序候选模板列表 targets: \[...\]，支持跨机器故障转移与中途无感切换底层引擎 |

## **3\. \`Master \+ herdr-core\` 零 MCP / 零 ACP 异构“借脑”与编排规范**

### **3.1 对现有 \`tarla-sidecar/src/herdr.rs\` 的关键改造点**

当前仓库 crates/tarla-sidecar/src/herdr.rs 仅封装了基础的 terminal.run（且硬编码了 "no\_parent": true）与轮询式 terminal.get。V2.0 在 Herdr 客户端中完整解锁 [herdr-core OpenRPC 规范](https://github.com/herdrdev/herdr-core/blob/master/schema/README.md) 的四大原生编排原语：

1. **开放父子终端委派树**：支持在 terminal.run 中传入 parent、outlive\_parent、notify 与模板名，使主控终端拉起的辅助 Agent 自动挂载为其子终端，并在子终端完成、报错或请求输入时自动向父终端投递 \<hc from="t3 reviewer"\>...\</hc\> 结构化通知。

2. **接入 events.subscribe 实时有序事件流**：取代低效轮询，常驻订阅单调递增 seq 事件流（内置 15 秒心跳与 missed 快照重同步），实时捕获 created、finished、status（OSC 7501 的 idle / working / done / blocked / error）、**turn（{turn, state} 每轮对话结束计数器）**、labels 以及 doc 变更。  
3. **托管生成 [Agent Templates](https://github.com/herdrdev/herdr-core/blob/master/docs/templates.md)**：tarlad 启动时根据 botflow.yml 自动在对应 hc-config/herdr-core/templates/ 下生成标准 .md 模板文件（每次运行实时读取），无需任何 ACP 桥接即可统一纳管所有支持或不支持 OSC 7501 的 CLI。  
4. **启用 doc.get / doc.set 本地版本化黑板**：利用 herdr-core 内置的乐观锁 JSON 文档（单文档上限 1 MiB，带 version 并发校验），在同一 Core 内的父子 Agent 之间零延迟共享任务计划、评审清单与结构化回执草稿。

### **3.2 异构客户端可热插拔的角色模板规范（彻底解除 Claude 强绑定）**

无论是担任集群大脑的 \`chief\`（Master Bot），还是下游执行 Bot，都可以随时在 \`claude\`、\`codex\`、\`gemini\`、\`pi\` 等客户端之间切换，只需更改模板头部的 \`command:\` 一行。以下为 \`tarlad\` 自动生成的 \`master-chief.md\` 模板规格（遵循 [herdr-core 模板语法](https://github.com/herdrdev/herdr-core/blob/master/docs/templates.md)）：

```
---
description: Workspace Master Orchestrator with native hc & tarla-ctl delegation
command: codex
submit: enter
stop: ctrl+c
paste_gap_ms: 80
---
你是当前工作空间的主控协调 Bot（chief）。严禁直接修改业务仓库源码，你的职责是拆解目标、调度异构子 Bot 并在验收后向用户汇报。
1. 本机快速协同：直接运行 `hc --skill` 查看终端编排原语，使用 `hc run --bg --name <name> <template> "<task>"` 拉起本机异构协作者，使用 `hc wait --children any` 等待回执，或读写 `hc` 文档黑板。
2. 跨机与持久任务调度：运行 `tarla ctl --json <subcommand>` 查询全网节点/Bot 状态、跨机派发持久化 Action、发布 Botflow 事件或检索修订 S2 长期记忆。
```

对于尚未内置 \`OSC 7501\` 状态上报的第三方 CLI 或自定义脚本，模板头部显式声明 \`ready: quiet\` 等待启动输出静默后再注入首条任务，配合 \`tarla ctl\` 回调完成状态闭环。

### **3.3 双路径协同架构与极简 \`tarla ctl\` JSON 命令行契约（替代 MCP 工具集）**

为消除 MCP 动辄数千 Token 的工具 Schema 占用，V2.0 采用 CLI 原生的“需要时才通过 \`--help\` 查看、单行命令直接返回紧凑 JSON”模式，分为本机快路径与跨机持久路径：

| 协同场景 | 执行通道 | 具体调用原语与数据流 | 核心优势 |
| :---- | :---- | :---- | :---- |
| **同机同核多 Agent 协作（快路径）** | \`hc\` 原生 CLI（见 [commands.md](https://github.com/herdrdev/herdr-core/blob/master/docs/commands.md)） | Master 直接执行 \`hc run \--bg \--name rev gemini-reviewer "核查刚生成的补丁"\` → \`hc wait \--children all\` → 自动接收 \`\<hc from="t4 rev"\>\` 报告或 \`hc read rev \--lines 80\` | 零网络开销、零额外序列化层，子进程生命周期随父任务自动收口 |
| **跨设备 / 可休眠节点编排（持久路径）** | \`tarla ctl \--json\` \+ \`tarlad\` S1 总线 | Master 执行 \`tarla ctl send-goal \--bot implementer \--obj "..." \--accept "..."\` → 本机 \`tarlad\` 过上下文防火墙并签名写入 SQLite S1 → 经穿透网唤醒远端 \`tarlad\` → 远端 \`tarlad\` 在目标 \`hc\` Core 中拉起或复用 Bot 终端并监听 \`events.subscribe\` 回传七态进度 | 解决 \`hc machine add\` 纯 SSH 通道在手机锁屏或笔记本休眠时断连失联的短板 |

**tarla ctl 五大精简子命令（全量输出紧凑单行 JSON，注入 TARLA\_CTL\_ROLE=master\|worker 严格隔离权限）**：

5. tarla ctl bots \[--include-offline\]：（仅 Master）列出全网在线节点、各 herdr-core 实例健康度、各 Bot 当前引擎/配额余量及队列深度。  
6. tarla ctl send-goal \--bot \<id\> \--obj \<text\> \[--accept \<rule\>...\] \[--op-id \<key\>\]：（仅 Master）向指定 Bot 派发持久化 Action，立即返回 {"run\_id":"...","state":"accepted"}。  
7. tarla ctl cancel-goal \<run\_id\> \[--reason \<text\>\]：（仅 Master）触发目标任务安全进入 canceling 状态。  
8. tarla ctl emit \<port\> \--payload \<json\>：（Master 与 Worker 共用）从当前 Bot 输出端口向 botflow.yml 总线发射结构化事件，或由 Worker 提交阶段 feedback / blocked\_approval 审批请求。  
9. tarla ctl memory \<search\|propose\|recall\> ...：检索冻结 S2 记忆、提交带 base\_version 的新记忆提案，或分页回查历史原文。

### **3.4 上游已知工程边界的确定性规避规则**

* **规避人机输入草稿串扰（[herdr-core Issue \#75](https://github.com/herdrdev/herdr-core/issues/75)）**：由于人类在交互式终端敲字停顿超过 notes\_quiet \= 3 秒时，后台 hc send 或父级通知可能混入未发出的半截草稿，V2.0 规定所有受管 Bot 终端对外部客户端默认仅开放 observe 只读模式；当用户显式申请 interactive 人工接管租约期间，tarlad 将该终端置为 human\_lease\_active，暂存一切后台事件注入直至用户释放接管。  
* **规避 Windows ConPTY 前台进程差异（[herdr-core Issue \#70](https://github.com/herdrdev/herdr-core/issues/70)）**：状态机一律以 OSC 7501 报告、events.subscribe 的 turn 事件与 tarla ctl 回执为准，绝不依赖 foreground.name 推断 Agent 是否空闲。

## **4\. 吸收 [Codync](https://github.com/leepokai/Codync) 的跨端同步与无限长会话保持架构**

剥离 [Codync](https://github.com/leepokai/Codync) 的 ACP 适配层后，将其经过大规模多端验证的会话持久化与增量同步模型完整融入 Tarla V2.0：

### **4.1 \`Conversation（永久逻辑对话流）≠ Terminal Session（可轮换执行终端）\`**

在 V2.0 中，用户面向的是具名 Bot（如 \`chief\`、\`rust-impl\`）背后那条永久保存在 SQLite 中的逻辑对话流，而底层的 \`hc\` 终端只是随时可以销毁重建或更换引擎的瞬态容器：

```rust
pub struct BotRuntimeBinding {
    pub bot_id: String,
    pub conversation_id: String,        // 永久不变的顶层逻辑对话流 ID
    pub active_node_id: String,         // 当前承载算力节点
    pub active_core_group: String,      // 所属 herdr-core 实例组（如 code_core）
    pub active_template: String,        // 当前激活模板（如 claude-arch / codex-dev / gemini-pro）
    pub hc_terminal_addr: Option<String>, // 当前存活的临时 hc 终端地址（如 Some("t7")，空闲回收后为 None）
    pub compaction_epoch: u64,          // 当前前缀冻结纪元号，用于锁定 LLM Prefix Cache
    pub idle_reap_after_secs: u64,      // 默认 1800 秒（30 分钟）无活跃任务自动回收底层终端释放内存
}
```

**跨引擎热切换（Hot-Swap Client）标准流程**：

10. 当用户在手机上把 \`chief\` 或 \`implementer\` 的引擎从 \`claude\` 切换为 \`codex\`，或原引擎触发配额上限自动转移时，\`tarlad\` 安全关闭旧 \`hc\` 终端（若有），保留同一 \`conversation\_id\`。  
11. 下一条消息或事件到达时，\`tarlad\` 调用 \`hc run \--bg\` 启动新的 \`codex\` 角色模板，自动从 SQLite 提取 \`\[角色说明 \+ 当前纪元冻结双层记忆 \+ Tarla 无损摘要 DAG 节点 \+ 最近 8 轮受保护原文\]\` 生成开局衔接包注入新终端。整个过程对手机、桌面与飞书/Telegram 用户完全无感。

### **4.2 全局单调递增游标 \`rev\`（\`since: rev\`）跨平台零丢帧同步**

为彻底解决手机切网、桌面休眠唤醒后多接口轮询漏状态的问题，V2.0 在工作空间 SQLite 引入一张全局单调递增变更账本 \`sync\_ledger(rev INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, entity\_id TEXT, payload\_json TEXT, created\_at INTEGER)\`：

* 任何对话消息新增、流式增量完成、Action 七态流转、\`botflow\` 事件触发、审批单生成或处理、记忆 \`sync\_seq\` 更新，均在同一写事务内分配唯一递增的 \`rev\`。  
* 客户端提供两个极简同步端点：\`GET /v1/sync?since=\<last\_rev\>\&limit=200\`（重连毫秒级批量追平离线差异）与 \`GET /v1/events?since=\<last\_rev\>\`（SSE 实时流推送后续新 \`rev\`）。  
* 握手接口 \`GET /v1/hello\` 返回 \`{"protocol\_generation": 2, "min\_app\_version": "0.2.0", "host\_version": "0.2.0", "latest\_rev": 1042}\`，兼容移动端应用商店审核周期滞后带来的跨版本协同（借鉴 [Codync 版本兼容设计](https://github.com/leepokai/Codync)）。

### **4.3 纪元锁定（Epoch-Pinned）双层确定性记忆与 Prefix Cache 保护**

在 V1.0 S2 记忆基础上，引入双层严格 Token 配额与确定性写入安检：

12. **双层配额防膨胀**：工作空间全局共享核心记忆上限 \`400\` Tokens；每个逻辑 Bot 独立专属记忆上限 \`1,200\` Tokens（使用率达 \`90%\` 水位时，后台自动归并沉淀为按需检索的专题记忆条目）。  
13. **纪元内前缀字节级冻结**：在同一个 \`(hc\_terminal\_addr, compaction\_epoch)\` 周期内，注入给 Agent 的系统前缀字节流严格保持恒定，确保吃满 90% 以上 Prompt Prefix Cache 折扣；仅在跨越 75% 上下文水位触发 DAG 压缩（\`compaction\_epoch \+ 1\`）或重建新 \`hc\` 终端时才刷新记忆快照。  
14. **确定性写入安检（Rule-Based Memory Check）**：任何记忆提案入库前先经过零成本纯 Rust 正则/字符集安检——直接拦截包含 Unicode 双向覆盖符/零宽隐藏字符、典型系统提示词越狱指令或 \`sk-\`/\`ghp\_\`/\`AKIA\` 等疑似私钥凭证的文本。

### **4.4 本地零模型调用配额探测与多 Bot 协作防环硬限制**

* **零 Token 本地额度遥测**：通过在 CLI 状态栏钩子（参考 [Codync \`statusline\` 包装机制](https://github.com/leepokai/Codync)）或本地遥测端点直接采集各引擎 5 小时滚动窗口与周配额，不发任何消耗 Token 的探测请求；配额达到 \`95%\` 自动切往 Bot \`targets\` 列表中的下一个异构模板。  
* **多 Bot 会话轮次硬熔断**：当多个 Bot 在同一协作频道或互助链路中对话时，单次人类外部输入最多允许 **3 轮交互、累计不超过 10 条 Bot 回复**；且从第 2 轮起仅唤醒被显式 \`@bot\_id\` 点名的目标 Bot，禁止无指向广播引发无限客套循环。

## **5\. Dora 风格 \`botflow.yml\` 事件图与 ROS 2 \`Action\` 七态机规格**

### **5.1 \`EventEnvelope\` 信封与三级端口拥塞控制**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,          // UUIDv7 全局唯一幂等 ID
    pub rev: i64,                  // 对应工作空间全局同步游标
    pub workspace_id: String,
    pub source_topic: String,      // "<bot_id>/<output_port>" 或 "sys/<builtin>"
    pub correlation_id: String,    // 根因果链追踪 ID
    pub hop_count: u8,             // 每次自动级联 +1，达到 max_event_hops (默认 6) 硬熔断转人工
    pub hlc_timestamp: String,
    pub payload: serde_json::Value // 经 8,000 字符门控与防火墙裁剪后的结构化载荷
}
```

每个 Bot 输入端口支持三种声明式队列策略：

* **\`coalesce\_latest\` \+ \`debounce\_ms\`**：目标 Bot 正在运行或处于防抖窗口内时，同端口高频事件仅保留最新一帧并累计 \`coalesced\_count\`，根治连续保存文件触发十几次重复编译审查。  
* **\`backpressure\`**：写入本地 SQLite 队列严格按序串行消费，用于用户指令与关键审批结论。  
* **\`drop\_oldest\`**：超出有界环形缓冲时淘汰最旧遥测帧。

### **5.2 对标 [ROS 2 Actions](https://design.ros2.org/articles/actions.html) 的七态状态机与 \`herdr-core\` 信号映射**

| Action 状态 | 业务语义 | \`herdr-core\` / \`tarlad\` 触发源 | 允许流转的下一状态 |
| :---- | :---- | :---- | :---- |
| **\`accepted\`** | 执行节点完成两阶段 \`claim\_id\` 落盘，等待模板启动许可 | 本地 \`service\_claims\` 提交成功 | \`executing\`, \`canceling\`, \`canceled\` |
| **\`executing\`** | Agent 在私有 \`hc\` 终端内运行，实时推送 \`turn\` 与阶段进展 | \`events.subscribe\` 收到 \`OSC 7501 working\` 或首条任务送达 | \`blocked\_approval\`, \`canceling\`, \`succeeded\`, \`aborted\`, \`unconfirmed\` |
| **\`blocked\_approval\`** | 触发敏感操作审批或交互阻塞，推送至手机随身端拍板（270 秒审批计时暂停超时钟） | \`OSC 7501\` 报告 \`blocked\` 或调用 \`tarla ctl\` 请求审批 | \`executing\`, \`canceling\`, \`aborted\`, \`canceled\` |
| **\`canceling\`** | 收到取消指令，调用 \`hc interrupt\` / \`terminal.kill\` 终止进程树并回收锁 | \`desired\_state \= cancelled\` | \`canceled\`, \`succeeded\`, \`aborted\` |
| **\`succeeded\`** | 执行完成并通过可选的 \`Check-Before-Done\` 异构审查，生成有界 \`RunReceipt\` | \`OSC 7501 done\` / 零退出码且验证模板返回通过 | 终态（不可变） |
| **\`aborted\`** | 进程非零退出、验证未通过、触发 \`\!acted\` 无法恢复的错误或前置事实缺失 | \`OSC 7501 error\` 或 \`exit\_code \!= 0\` | 终态（不可变） |
| **\`canceled\`** | 启动前撤回许可、取消信号生效退出或管理员执行 \`abandon\` | 进程确认终止且锁已释放 | 终态（不可变） |

（注：执行节点网络失联期间，主管观测视图叠加 \`unconfirmed\` 标记，重连后按本地单调 S1 日志精确对账恢复真实状态。）

### **5.3 完整 \`botflow.yml\`（v0.2 零 MCP / 原生 \`herdr-core\` 定稿版）示例**

```yaml
version: "0.2"
workspace: work

policy:
  max_event_hops: 6
  max_room_rounds: 3
  max_room_replies: 10
  result_gate_max_chars: 8000
  mission_packet_max_bytes: 7200
  run_receipt_max_bytes: 2000

nodes:
  iphone-pro:
    role: master_control
    platform: ios

  mac-studio:
    role: supervisor_and_executor
    herdr_cores:
      master_core:
        session: tarla-work-master_core
      code_core:
        session: tarla-work-code_core
      ops_core:
        session: tarla-work-ops_core
    templates:
      codex-master:
        core: master_core
        command: "/opt/homebrew/bin/codex"
        workdir: "/Users/collaalloc/Projects/Tarla"
      claude-arch:
        core: code_core
        command: "/opt/homebrew/bin/claude"
        env_profile: "claude-work-max"
        workdir: "/Users/collaalloc/Projects/Tarla"
      codex-impl:
        core: code_core
        command: "/opt/homebrew/bin/codex"
        env_profile: "codex-pro-main"
        workdir: "/Users/collaalloc/Projects/Tarla"
      gemini-rev:
        core: code_core
        command: "/opt/homebrew/bin/gemini"
        workdir: "/Users/collaalloc/Projects/Tarla"

bots:
  - id: chief
    name: "随身总控 Master"
    control_authority: iphone-pro
    targets: ["mac-studio/codex-master", "mac-studio/claude-arch"]
    idle_reap_secs: 1800
    inputs:
      user_intent:
        source: sys/user_message
        queue_policy: backpressure
      final_verdict:
        source: reviewer/approved
        queue_policy: backpressure
    outputs:
      - spec_ready

  - id: implementer
    name: "核心研发 Bot"
    targets: ["mac-studio/codex-impl", "mac-studio/claude-arch"]
    checkpoint_files: true
    verify_before_done: "mac-studio/gemini-rev"
    inputs:
      task_spec:
        source: chief/spec_ready
        queue_policy: backpressure
      fix_request:
        source: reviewer/changes_requested
        queue_policy: coalesce_latest
        debounce_ms: 2000
    outputs:
      - patch_ready

  - id: reviewer
    name: "异构代码审查 Bot"
    targets: ["mac-studio/gemini-rev", "mac-studio/claude-arch"]
    constraints:
      different_command_from_upstream: true
    inputs:
      patch:
        source: implementer/patch_ready
        queue_policy: coalesce_latest
        debounce_ms: 1500
    outputs:
      - approved
      - changes_requested
```

## **6\. 合入主流 Personal Agent 的五大工程安全护栏**

15. **1\. 区分 \!acted 与 acted 的双层故障转移门控（源自 [OpenBot](https://github.com/nightly-labs/openbot)）**：

tarlad 在每次 Action 启动前记录目标工作树状态与外部操作计数：若当前轮次尚未修改任何受追踪文件且未调用外部写接口（\!acted）就遭遇 429 限流或进程崩溃，微内核立即静默切换 targets 中的下一个备用模板重放任务；一旦检测到文件已变更或已发出外部副作用（acted），立即停止自动重放，将状态置为 blocked\_approval 并携带差异摘要推送手机由用户拍板。

16. **2\. Check-Before-Done 异构交叉验收机制（源自 [OpenMausBot](https://github.com/milind-soni/OpenMausBot) 与 [Bloks](https://github.com/hamedgitty/bloks)）**：

当 Bot 配置了 verify\_before\_done（如 implementer 指定 gemini-rev）时，主执行终端报 done 后并不会直接将 Action 标为 succeeded，而是由本机 tarlad 自动在同核调用 hc run \--bg 拉起只读审查模板核对 acceptance\_criteria 与 git diff；只有验证通过才流转至 succeeded，否则将审查意见直接通过 hc send 打回原终端继续修复（最多重试 2 次）。

17. **3\. 8,000 字符结果截断门控与长输出落盘（源自 [OpenBot](https://github.com/nightly-labs/openbot) 与 [OpenMausBot](https://github.com/milind-soni/OpenMausBot)）**：

无论是 hc read 抓取子终端输出、跨节点传递事件 payload 还是命令回执，一旦单次文本超过 8,000 字符，微内核自动将完整原文写入 runtime/spills/\<event\_id\>.log，仅向上游 Master 传递“前 2,000 字符 \+ 末尾 2,000 字符 \+ 按行/关键词读取该文件的指针”，彻底杜绝海量编译日志撑爆主控窗口。

18. **4\. SHA-256 冲突感知文件回滚快照（源自 [Bloks](https://github.com/hamedgitty/bloks)）**：

开启 checkpoint\_files: true 的写任务在运行前后记录变更文件的 (path, pre\_sha256, post\_sha256) 及原始备份；当用户在手机或 CLI 触发回滚撤销某次 Action 时，tarlad 逐文件校验当前磁盘哈希是否仍等于 post\_sha256——若发现某文件在 Bot 完工后又被用户手动编辑过，自动跳过覆盖该文件并列出冲突路径，保护人工手写代码零丢失。

19. **5\. 带 \--operation-id 幂等锁的零明文凭据代理（源自 [Linger](https://github.com/simplexai-labs/linger) 与 [OpenBot](https://github.com/nightly-labs/openbot)）**：

外部 Webhook、云 API、Git 推送令牌等敏感密钥一律保存在 tarlad 受保护的宿主 Keychain / 凭据库中，**绝不以明文环境变量注入 hc 终端**（防止 Prompt 注入通过 env 打印窃取）；Bot 需执行外部副作用操作时，调用 tarla ctl 携带确定性 \--op-id，由 tarlad 在 SQLite idempotent\_ops 表查重加锁后代填凭据执行，网络抖动重传永远只生效一次。

## **7\. V2.0 SQLite 增量迁移与六阶段工程实施路线图**

### **7.1 SQLite 增量新增核心表（叠加在 V1.0 \`schema.sql\` 之上）**

```sql
-- 1. 全局单调递增变更游标表（支撑 Codync 式 since: rev 跨端增量同步）
CREATE TABLE sync_ledger (
    rev          INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id TEXT NOT NULL,
    event_kind   TEXT NOT NULL, -- message | action_state | botflow_event | approval | memory
    entity_id    TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at   INTEGER NOT NULL
);

-- 2. 逻辑 Bot 永久会话与瞬态 hc 终端解耦绑定表
CREATE TABLE bot_bindings (
    bot_id            TEXT PRIMARY KEY,
    workspace_id      TEXT NOT NULL,
    conversation_id   TEXT NOT NULL REFERENCES conversations(conversation_id),
    active_node_id    TEXT NOT NULL,
    active_core_group TEXT NOT NULL,
    active_template   TEXT NOT NULL,
    hc_terminal_addr  TEXT,
    compaction_epoch  INTEGER NOT NULL DEFAULT 1,
    human_lease_until INTEGER,
    last_active_at    INTEGER NOT NULL
);

-- 3. Botflow 事件持久队列（含防抖合并与跳数熔断记录）
CREATE TABLE botflow_inbox (
    inbox_id        TEXT PRIMARY KEY,
    target_bot_id   TEXT NOT NULL,
    input_port      TEXT NOT NULL,
    correlation_id  TEXT NOT NULL,
    hop_count       INTEGER NOT NULL,
    coalesced_count INTEGER NOT NULL DEFAULT 1,
    deliver_after   INTEGER NOT NULL, -- 用于 debounce_ms 延迟投递
    payload_json    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued' -- queued | delivered | tripped_breaker
);

-- 4. SHA-256 冲突感知检查点表与外部副作用幂等锁表
CREATE TABLE file_checkpoints (
    run_id       TEXT NOT NULL,
    rel_path     TEXT NOT NULL,
    pre_sha256   TEXT,
    post_sha256  TEXT,
    backup_blob  BLOB,
    PRIMARY KEY (run_id, rel_path)
);

CREATE TABLE idempotent_ops (
    op_id        TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    run_id       TEXT NOT NULL,
    status       TEXT NOT NULL, -- inflight | succeeded | failed
    result_json  TEXT,
    created_at   INTEGER NOT NULL
);
```

### **7.2 面向本地仓库的六阶段具体改造与验收路线图**

以当前 \`/Users/collaalloc/Projects/Tarla\` 的 10 个子 Crate 为起点，按依赖自底向上分 6 个里程碑推进（期间不触碰待批的 V3.0 界面工程）：

| 阶段 | 目标模块与新增/修改文件 | 具体工程实施内容 | 阶段验收标准 |
| :---- | :---- | :---- | :---- |
| **Phase 0：V1.0 核心基线隐患清零** | \`tarla-memory/src/lib.rs\`、\`tarlad/src/launch.rs\`、\`tarla-context/src/{lib.rs,firewall.rs}\`、\`tarla-cli/src/admin.rs\` | ① 校验 \`memory\_id\` 安全字符集并用 SQL 直查 \`user\_manual\` 替代字符串反解析；② 修复 \`--executor-launch-helper\` 信号转发与等子进程彻底退出后再释放 \`.tarla-slot.lock\`；③ 折叠块保留全部回执句柄，中文防火墙字节预算按 \`4 bytes/token\` 对齐；④ 修复 \`supervisor restore\` 保留节点原执行角色 | 现有全套 \`cargo test \--workspace\` 与 \`tests/stage1\_core.py\`、\`stage1\_history.py\` 全部回归通过，新增边界单测覆盖上述 4 类缺陷 |
| **Phase 1：升级 \`tarla-sidecar\` 解锁多核与 \`herdr-core\` 原生编排** | \`crates/tarla-sidecar/src/{herdr.rs, templates.rs, events.rs}\` | ① 支持单工作空间按 \`core\_group\` 管理多个独立 \`Herdr\` 实例（\`HC\_SESSION=tarla-\<ws\>-\<group\>\`）；② 去掉硬编码 \`no\_parent: true\`，支持父子终端树、\`.md\` 角色模板自动渲染落盘、\`doc.get\`/\`doc.set\` 黑板读写；③ 实现 \`events.subscribe\` 异步长连接流客户端（解析 \`status\`、\`turn\`、\`finished\`、\`doc\` 事件与 \`missed\` 重同步） | 单机同时拉起 \`code\_core\` 与 \`ops\_core\` 互不干扰；父终端通过模板拉起异构子终端并在子终端完成时收到 \`\<hc from="..."\>\` 通知与 \`turn\` 事件 |
| **Phase 2：落地 \`Codync\` 式跨端 \`rev\` 游标与逻辑会话解耦** | \`crates/tarla-memory/src/schema.sql\`、\`crates/tarla-context\`、\`crates/tarlad/src/{sync.rs, bot\_session.rs}\` | ① 新增 \`sync\_ledger\` 与 \`bot\_bindings\` 表，打通 \`GET /v1/sync?since=\<rev\>\` 增量拉取与 \`GET /v1/events?since=\<rev\>\` SSE 推送流；② 实现 \`Conversation ≠ Terminal\` 生命周期（30 分钟空闲回收 \`hc\` 终端 \+ 跨 CLI 引擎切换时的 \`\[角色模板 \+ 纪元冻结双层记忆 \+ DAG 摘要 \+ 近期原文\]\` 无感冷启动续接）；③ 加入记忆确定性规则安检与本地零 Token 配额探测 | 同一个 \`conversation\_id\` 先用 \`claude\` 聊 5 轮，手动或模拟限流切换为 \`codex\` 后无需人工复述上下文即可继续推进；模拟手机断网重连 \`since: rev\` 零丢帧追平 |
| **Phase 3：落地 \`Master \+ hc\` 零协议借脑与 \`tarla ctl\` CLI** | \`crates/tarla-cli/src/ctl.rs\`、\`crates/tarlad/src/control.rs\`、移除旧 \`slot\_pool\` 强耦合逻辑 | ① 在 \`tarla\` CLI 中新增紧凑 JSON 输出的 \`tarla ctl\` 子命令族（\`bots\`、\`send-goal\`、\`cancel-goal\`、\`emit\`、\`memory\`），按运行令牌区分 \`master\` 与 \`worker\` 权限；② 打通 \`chief\` Master Bot 在专属 \`master\_core\` 终端内结合 \`hc \--skill\`（本机快路径）与 \`tarla ctl\`（跨机 S1 持久路径）的完整调度闭环；③ 增加 \`human\_lease\_active\` 互斥门控规避 Issue \#75 草稿串扰 | 无需任何 MCP Server 或 ACP Adapter，分别将 \`chief\` 配置为 Codex CLI 和 Claude Code 均可自主查看全网节点、本机拉起子终端并向远端节点派发持久任务 |
| **Phase 4：新建 \`crates/tarla-botflow\` 实现事件总线与七态机** | 新增 \`crates/tarla-botflow\`（接入 \`tarlad\` 与 \`tarla-protocol\`） | ① 实现 \`botflow.yml\` v0.2 解析校验器；② 实现 \`botflow\_inbox\` 调度器（支持 \`coalesce\_latest\` \+ \`debounce\_ms\`、\`backpressure\`、\`drop\_oldest\`、\`max\_event\_hops: 6\` 跳数熔断及多 Bot 3 轮/10 条回复熔断）；③ 将 \`runs\` 状态机升级为标准 ROS 2 Action 七态流转（含 \`blocked\_approval\` 手机远程批复唤醒） | 部署 \`chief \-\> implementer \-\> reviewer\` 三节点闭环：连续触发 10 次补丁事件仅合并执行最新 1 次审查；人为构造双向死循环在第 6 跳精准熔断告警 |
| **Phase 5：合入五大 Personal Agent 安全护栏与全链路实网终验** | \`crates/tarlad/src/{guardrails.rs, checkpoints.rs, broker.rs}\`、\`tests/v2\_botflow\_e2e.py\` | ① 实现 \`\!acted\` vs \`acted\` 故障转移门控；② 实现 \`verify\_before\_done\` 自动拉起异构审查模板闭环；③ 实现 \`8,000\` 字符输出落盘截断门控；④ 实现 \`file\_checkpoints\` SHA-256 冲突感知回滚；⑤ 实现 \`--op-id\` 幂等凭据代理，编写跨双机多核端到端验收套件 | 模拟执行中途 429（\`\!acted\` 自动换脑重跑、\`acted\` 安全挂起）、验证未通过自动打回返工、人工修改文件后回滚零误伤覆盖，全部自动化测试绿灯通过 |

* 