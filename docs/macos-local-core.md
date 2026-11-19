# macOS 桌面端的本机后台服务

标准版 Tarla 通过远端地址和配对令牌访问现有服务。另有可选的 **Tarla Local** 构建，适用于 macOS 13 及以上。它把 `tarla` 与 `tarlad` 放入应用包的 `Contents/Helpers/`，用当前用户的 LaunchAgent 管理本机服务，同时保留连接其他工作空间的入口。

## 在 Mac 上使用

1. 将签名后的 **Tarla Local.app** 安装至「应用程序」，启动应用。
2. 点击右上角的电脑图标「管理本机服务」，选择「启用服务」。应用会初始化 `~/.tarla/workspaces/desktop-local/` 并通过 `SMAppService` 注册后台项目。已有工作空间和凭证会保留。
3. 如果 macOS 要求批准，在「系统设置 → 通用 → 登录项与扩展」允许 Tarla 在后台运行。返回应用并刷新状态，再选择「连接本机」。
4. 本机服务在退出窗口后继续运行。要停止并取消登录时启动，在同一窗口选择「停用服务」。此操作保留本机账本、配置和记忆。

LaunchAgent 使用当前用户权限并监听随机分配的 IPv4 回环端口。连接前，应用用一次性随机数验证监听端能否用工作空间密钥计算正确的 HMAC，随后才发送访问令牌。用户可在 `~/.tarla/workspaces/desktop-local/workspace.toml` 配置模型和工位。默认沙箱客户端和可管理本机文件的 Local 版使用不同的应用标识，可分别安装。

## 构建与签名

在安装 Xcode、Flutter 和 Rust stable 的 macOS 构建机上，从仓库根目录运行以下命令。`Developer ID Application` 身份应已安装在签名者自己的钥匙串。目标输出路径须为新路径。

```sh
(cd apps/tarla-app && flutter pub get && flutter build macos --release)
apps/tarla-app/macos/LocalCore/package.sh \
  apps/tarla-app/build/macos/Build/Products/Release/tarla_app.app \
  "$PWD/Tarla Local.app" \
  "Developer ID Application: Your Organization (TEAMID)"
```

脚本按源应用的架构编译两个 Rust 程序，分别签名后台程序和 CLI，然后签名框架与外层应用。Developer ID 构建启用 Hardened Runtime 和时间戳。后台程序含自己的签名标识与本地网络用途说明。Local 版通过独立的 `dev.tarla.client.local` 应用标识访问用户主动启用的本机工作空间。其 LaunchAgent 标识为 `dev.tarla.client.local.tarlad`。

供外部分发时，在发布者自己的 Apple 账号下进行公证并装订票据。将上述命令最后一个参数改为 `-` 可在开发机制作 ad-hoc 测试包。GitHub Actions 的 **macOS bundled local core** 流程构建标准版与 ad-hoc Local 版，检查签名、架构、LaunchAgent 配置，并实际启动内置 `tarlad` 验证动态端口和认证。Developer ID 信任链、公证、系统设置批准及局域网 TCC 提示仍需使用发布者签名和真实登录会话验收。

[Apple 的 Service Management 说明](https://developer.apple.com/documentation/servicemanagement/smappservice) · [系统服务打包与迁移](https://developer.apple.com/documentation/servicemanagement/updating-your-app-package-installer-to-use-the-new-service-management-api)
