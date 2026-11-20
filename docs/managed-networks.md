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

### 同一台主机上的两个工作空间

为 `work` 和 `life` 配置不同的 EasyTier 网络名、网络密钥和节点配对密钥。使用 Tailscale 时，每个工作空间通过自己的控制套接字登记到各自的 tailnet。用户态 Tailscale 可将入站流量转到本机回环的相同端口。在共用主机上，为两个 `tarlad` 分配不同的监听端口，并在每个 tailnet 的访问策略中只放行本工作空间的节点和 API 端口。举例来说，使用 `tag:tarla-work` 标识业务端口为 7400 的办公节点时，可配置以下策略并为节点登记对应标签。

```json
{
  "tagOwners": {"tag:tarla-work": ["autogroup:admin"]},
  "acls": [
    {"action": "accept", "src": ["tag:tarla-work"], "dst": ["tag:tarla-work:7400"], "proto": "tcp"}
  ]
}
```

配对 URL 填写对应网络中的对端地址和业务端口。主机登录账户管理工作空间的私有状态和本机回环代理。对端发出的业务请求仍需提供对应凭据。

### 网络进程恢复

`tarlad` 在工作空间内保留一个稳定的回环 SOCKS5 监听端点。EasyTier 或 Tailscale 进程退出后，进行中的连接会断开。守护进程关闭旧连接，按 250 毫秒到 10 秒的退避间隔重启已校验的边车，再使用原有代理地址接受新请求。整个 `tarlad` 重新启动后，代理端口可以改变，客户端应重新读取本地运行记录。EasyTier 的实例标识和 Tailscale 的登记状态存放在各自工作空间中，恢复连接后继续由对等同步协议补传。

`runtime/easytier.json` 和 `runtime/tailscale.json` 记录当前状态 `ready` 或 `reconnecting`。`ready` 表示本地代理已完成 SOCKS 握手。Tailscale 首次登录、网络策略授权和对端重连可能仍在进行。获取上游诊断数据前，读取当前记录中的 `rpc`，EasyTier 的管理端口可能随边车重启改变。

## 验证

`tests/mesh_acceptance.py` 在两个安装目录上同时启动四个真实的 `tarlad`，让办公网和生活网使用重叠子网及各自的密钥。测试向生活网提供办公网的种子地址，检查同网授权访问、错误凭据、工作空间不匹配、跨网拨号和同机其他端口。测试还会重启两端的 `tarlad`，强制退出执行节点的 EasyTier 进程，并核对原代理地址和补传结果。

```sh
cargo build --locked --workspace
export TARLA_TEST_EASYTIER=/path/to/easytier-core
export TARLA_TEST_EASYTIER_CLI=/path/to/easytier-cli
cargo test --locked -p tarla-sidecar real_easytier -- --ignored
python3 tests/mesh_acceptance.py
```

`real_tailscale_loopback_socks` 检查 Tailscale 1.82.0 用户态守护进程启动及 SOCKS5 握手。`tests/tailnet_acceptance.py` 为办公网和生活网分别创建 Headscale 0.26.1 控制平面、私有 TLS CA、DERP 中继和仅放行业务端口的 ACL。四个用户态节点用一次性密钥登记，再检查独立状态恢复、有效与无效凭据、同主机上的端口隔离以及边车重启后的 S2 恢复。Linux 验收使用以下三个上游程序。

```sh
export TARLA_TEST_HEADSCALE=/path/to/headscale
export TARLA_TEST_TAILSCALED=/path/to/tailscaled
export TARLA_TEST_TAILSCALE_CLI=/path/to/tailscale
python3 tests/tailnet_acceptance.py
```

连入用户自己的 Tailscale 托管网络时，使用该 tailnet 的登录方式和管理员设定的 grants/ACL。
