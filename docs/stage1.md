# 阶段一 · 单机无头主管

一台 Linux 主机即可从命令行对话、派发任务、查看执行日志，并在原会话收到完成回报。采用 `supervisor_and_executor` 角色时，主管与执行器共用一个工作空间。阶段一按[工程规范 §8.2](Tarla_Architecture_Spec.md#82-首版v1分阶段实施与验收清单)验收。

## 从第一条消息到第一份回报

1. 在仓库根目录运行以下命令。`work` 是工作空间名，`office` 是本机节点名。初始化后配置保存在 `~/.tarla/workspaces/work/workspace.toml`。

   ```sh
   cargo build --workspace --locked
   export PATH="$PWD/target/debug:$PATH"
   tarla --workspace work init --node office --role supervisor_and_executor
   ```

2. 安装经校验的 `herdr-core` 0.1.19，在配置文件中登记一个本地执行工位，并为普通对话指定一个支持工具调用的模型。可直接采用 README 的[模型、工位与安装示例](../README.md#启用模型和执行工位)。如果借用本机已登录的 Claude 订阅，见[工位配置](subscription-slots.md)。更改配置后重启守护进程。

3. 在第一个终端运行 `tarlad --workspace work`。另开一个终端，在仓库根目录设置相同的 `PATH`，然后执行：

   ```sh
   tarla --workspace work status
   tarla --workspace work slots
   tarla --workspace work chat project-a /help
   tarla --workspace work chat project-a '请派发任务，检查本工程是否可以编译，并在完成后报告结果。' --source-id build-check-01
   tarla --workspace work runs
   tarla --workspace work history project-a
   ```

   已派发的回复包含 `run_id`。重试同一条消息时复用相同的 `--source-id`。任务运行期间可用 `tarla --workspace work log <run-id>` 查看日志。完成后再次读取 `history project-a` 即可看到主动回报，无需再向主管发送消息。若要明确指定任务目标与验收条件，可直接执行 `tarla --workspace work dispatch project-a '检查工程编译' --accept '返回编译结果'`。

## 可重复验收

在 Linux 上构建 `tarla` 和 `tarlad`，将 `TARLA_TEST_HC` 设为已解包的 `herdr-core` 0.1.19 可执行文件的绝对路径。以下命令使用临时工作空间与确定性模型，无需付费模型密钥。

```sh
cargo test --workspace --locked
cargo build --workspace --locked
TARLA_TEST_HERDR="$TARLA_TEST_HC" cargo test --locked -p tarla-sidecar --test real real_herdr_holder_and_scoped_stream -- --ignored
python3 tests/stage1_core.py
python3 tests/stage1_history.py
python3 tests/dispatch_recovery.py
python3 tests/executor_recovery.py
```

| 验收范围 | 检查方法 |
| --- | --- |
| 独立工作空间与终端 | `work` 和 `life` 共用同一安装目录。检查各自的令牌、任务和记忆，比较宿主原生 Herdr 在测试前后的终端列表，并在工作空间私有 Herdr 中检查第 20 个终端 `t1b` 的读写与取消。 |
| 主管和执行工位 | 通过真实守护进程调用全部六个工具。第一个模型返回 HTTP 429，后备模型继续。执行程序等待放行期间，CLI 已收到任务编号；执行结束后，原会话自动出现回报。 |
| 长会话和原文 | 连续发送 156 轮对话，记录发给模型的实际请求大小。从旧中文长消息中提取决定，再用 `session_recall` 分页读回原文，并核对每条历史记录。分别检查派单简报和完成回执的大小。 |
| 重启和重放 | 检查重复消息、派单事务中断、不同会话的回报、执行器重启、同账号并发及取消。执行结果无法从旧记录确定时保留 `unconfirmed`，由执行机负责人核对现场后关闭。详见[执行机恢复说明](../crates/tarlad/README.md)。 |

[会话归档与模型窗口](conversation-history.md)说明了冻结前缀、旧库升级和原文分页的行为。近期对话在预算允许时保留全文。单条消息已经超过活动窗口时，模型会读到节选和原文位置，完整内容仍可按字节分页读取。模型请求以保守的 UTF-8 字节估算约束大小，专用验收同时测量实际序列化请求体。外部订阅账号的真实登录、在线配额和模型回答质量需要在对应账号下验证。
