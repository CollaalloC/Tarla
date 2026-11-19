# 工作空间的私有网络

下面的配置让两个无头节点通过各自的 EasyTier 边车交换带配对鉴权的业务请求。假设主管和执行节点的 EasyTier 地址分别为 `10.74.63.1` 和 `10.74.63.2`，两端的 `tarlad` 都在各自机器上监听 `127.0.0.1:7400`。端口可以不同，配对 URL 中填写对端实际监听的端口。

## EasyTier

使用 CLI 的 `install-tool` 将发布二进制与校验值登记在每台机器的 Tarla 私有目录。将以下内容填入双方已有的 `[network]` 段。示例的种子地址需替换为能够从两台机器访问的 EasyTier 对端。常驻种子应有固定监听地址。主管可以暂时离线，执行节点仍可检索自己的 SQLite 镜像。

```toml
[network]
mode = "managed_sidecar"
enable_easytier = true
enable_tailscale = false
easytier_network_name = "research"
easytier_network_secret_ref = "env:TARLA_RESEARCH_MESH_SECRET"
easytier_ipv4 = "10.74.63.1/24"
easytier_peers = ["tcp://seed.example.net:11010"]
```

在执行节点把 `easytier_ipv4` 改为 `10.74.63.2/24`。若省略这个字段，EasyTier 使用 DHCP。固定地址便于在配对记录中保存稳定的 URL。Tarla 将本实例标识写入工作空间的 `runtime/easytier-instance`，重启后继续使用。

两台机器中若有一台可被另一台直接访问，可以由它充当种子。例如主管的局域网地址为 `192.168.1.20`，就在主管的 `[network]` 中配置 `easytier_listen = "192.168.1.20:11010"` 和 `easytier_peers = []`，在执行机配置 `easytier_peers = ["tcp://192.168.1.20:11010"]`。两端重启后，执行机仍从这个固定入口加入网络。未指定监听地址时，EasyTier 使用系统分配的动态 TCP 端口。SOCKS 与管理端口始终独立绑定回环。

为两个守护进程设置相同的 `TARLA_RESEARCH_MESH_SECRET`。另设独立的 `TARLA_RESEARCH_PAIR` 供应用层鉴权。随后分别运行以下命令并启动对应守护进程。

```sh
# 主管机器
tarla --workspace work pair worker http://10.74.63.2:7400 --token-env TARLA_RESEARCH_PAIR
tarlad --workspace work --listen 127.0.0.1:7400
```

```sh
# 执行机器
tarla --workspace work pair chief http://10.74.63.1:7400 --token-env TARLA_RESEARCH_PAIR
tarlad --workspace work --listen 127.0.0.1:7400
```

已验证 EasyTier 2.7.0 的 no-TUN 入站行为。对端请求发送到虚拟 IPv4，EasyTier 将其交给本机同端口的监听器。Tarla 配置仅放行自己的 TCP 业务端口，从独立回环 SOCKS5 端口出站。控制端口、代理地址和 API 端口位于该工作空间的 `runtime/easytier.json`。托管实例通过标准输入读取组网配置，用户提供的密钥无需放入命令行。

私有密钥也可设为 `keychain:tarla/work/et_secret`。Linux 使用 libsecret 的 `secret-tool`，属性为 `service=tarla` 与 `id=tarla/work/et_secret`。macOS 使用服务名为 `tarla/work/et_secret` 的通用密码。Windows 使用目标名相同的通用凭据及 UTF-16LE 密码。

## Tailscale

配置 `enable_tailscale = true` 并安装固定版本的 `tailscaled`。Tarla 为每个工作空间创建专用状态文件、控制端点和回环 SOCKS5 地址。Linux 或 macOS 可用配套 `tailscale` 客户端完成这个私有实例的首次登录。

```sh
tailscale --socket="$HOME/.tarla/workspaces/work/runtime/ts.sock" up --accept-dns=false --accept-routes=false
```

从各节点的同一私有实例获取地址后，使用对端的 tailnet IP 和 `tarlad` 业务端口完成配对。Windows 控制管道地址可从 `runtime/tailscale.json` 的 `rpc` 字段取得。用户态 Tailscale 会把访问本机 tailnet IPv4 的 TCP 流量转发到回环同端口。请在本工作空间对应 tailnet 的 grants/ACL 中将通信限制到已配对节点及业务端口。如果同时启用两个网络，用 `preferred = "easytier"` 或 `preferred = "tailscale"` 选择对等请求的出站代理。

## 验证

`tests/mesh_acceptance.py` 启动两个真实的 `tarlad` 和两个 EasyTier 进程，检查经过虚拟 IP 的 S2 同步，并在执行节点重启后验证离线增量补传。Rust 集成测试另外验证旁路端口不能通过 EasyTier 访问。网络测试的本地运行命令如下。环境变量应指向与 CI 一致的上游二进制。

```sh
cargo build --locked --workspace
export TARLA_TEST_EASYTIER=/path/to/easytier-core
export TARLA_TEST_EASYTIER_CLI=/path/to/easytier-cli
cargo test --locked -p tarla-sidecar real_easytier -- --ignored
python3 tests/mesh_acceptance.py
```

`real_tailscale_loopback_socks` 验证 Tailscale 1.82.0 用户态守护进程启动及 SOCKS5 握手。测试连接独立 tailnet 并验证双节点登录需要有效的 Tailscale 身份。
