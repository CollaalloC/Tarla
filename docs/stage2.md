# 阶段二 · 跨节点执行与恢复

一个工作空间只有一个活跃主管。执行机在本地保存原始日志和终端，主管保存任务状态、审批和回执，已配对的执行机各自保存可离线读取的长期记忆。阶段二可以完全从 CLI 复核。

## 用两台主机派发任务

1. 按 [README 的双机配对步骤](../README.md#配对两台主机) 在两端初始化同名工作空间。主管可用 `supervisor_only`，执行机使用 `executor_only`。分别为配对节点配置地址及独立的配对密钥。
2. 在执行机安装已校验的 Herdr 和 Agent CLI，并按[工位示例](../README.md#启用模型和执行工位)登记工位。选择 EasyTier 或 Tailscale 时，按[受管网络文档](managed-networks.md)配置每个工作空间。两端启动各自的 `tarlad`。
3. 从主管所在机器执行以下命令。`work` 是工作空间名，`calibration` 是会话名。

   ```sh
   tarla --workspace work nodes
   tarla --workspace work slots
   tarla --workspace work dispatch calibration "检查项目编译并记录遇到的问题" --request-id build-01 --accept "报告编译结果和已验证的原因"
   tarla --workspace work runs
   tarla --workspace work history calibration
   ```

   收到任务编号后可以用 `tarla --workspace work log <run-id>` 查看执行机日志。重复提交相同请求编号、会话和参数会返回同一任务。执行机从本地持久化的任务认领恢复。断线后可继续执行已经获准启动的任务，重新连接后补传状态和回执。

## 更换主管

在每个参与迁移的节点上预先配置新主管为独立配对节点，并让原主管与执行机至少成功连通一次。执行机会记住原主管的公钥。每次迁移由原主管签署继任者和下一任期；新主管上线后向已配对节点分发签署记录。

| 场景 | 操作 |
| --- | --- |
| 计划交接 | 停止原主管，执行 `tarla --workspace work supervisor handover --to backup --snapshot /private/work-transfer.db`。原主管配置转为执行节点。 |
| 提前制作恢复备份 | 停止原主管，执行 `tarla --workspace work supervisor backup --to backup --snapshot /private/work-backup.db`，然后重新启动原主管。同一任期的备份固定同一个继任者及签名身份。 |
| 激活继任者 | 在目标节点的全新同名工作空间执行 `tarla --workspace work supervisor restore --snapshot /private/work-transfer.db` 并启动服务。灾难恢复使用备份文件，确认原主管已下线后加 `--confirm-old-offline`。 |
| 已运行的执行节点 | 收到可验证的更高任期后，守护进程自动重新加载配置。旧主管恢复联网后按执行节点运行。可在每个节点执行 `tarla --workspace work status` 核对主管名和任期。 |
| 老版本或全新未配对节点 | 如果没有原主管信任记录，停止本机守护进程，经独立渠道核对新主管身份后，使用 `tarla --workspace work supervisor set-authority --node backup --term 2`。再启动并检查状态。 |

快照含会话、长期记忆和继任者签名材料，应由操作者私下传送。灾难恢复由操作者明确发起。在完全断网的分区中，节点获知新任期需要等到恢复通信，因此激活备份前应确认原主管停止服务。旧快照之后仅原主管持有、从未复制过的聊天内容，需要从原机或更新的备份恢复。

## 验收入口

| 路径 | 测试 |
| --- | --- |
| 三个独立网络栈、真实执行进程、断开镜像接口、重启主管、重连后离线读 | [三个 Linux 网络命名空间](../tests/stage2_network_hosts.py) |
| 主管丢失已确认事件后的认领、重放与终态恢复 | [双节点对账](../tests/s1_reconciliation.py) |
| 执行机自动提纯经验、主管裁决版本、其他节点镜像可读 | [任务经验验收](../tests/stage2_distillation.py) |
| 灾难恢复、在线执行机采用新任期、旧主管重新上线并降级 | [主管交接验收](../tests/authority_recovery.py) |
| EasyTier / Tailscale 私有网络与断线重连 | [EasyTier](../tests/mesh_acceptance.py) · [Tailscale](../tests/tailnet_acceptance.py) |
| 普通记忆修改的传播耗时与任务回执延迟 | [测量口径与复现命令](replication-latency.md) |

网络命名空间在同一台 Linux 主机上使用独立接口、路由表和回环地址。测试中各节点使用独立数据库。跨地域公网延迟取决于部署条件。
