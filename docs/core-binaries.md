# 下载并启动核心程序

打开仓库的 **Actions → Core binaries**，选择对应提交的成功运行，在 **Artifacts** 中下载适合目标主机的包。也可以从 **Run workflow** 为选定分支手动构建。每个包提供 `tarla`、`tarlad`、`tarla-gateway` 和逐个文件的 SHA-256 清单。

| 目标主机 | Actions 产物名 | 包格式 |
| --- | --- | --- |
| Linux x86_64（Ubuntu 22.04 构建） | `tarla-core-x86_64-unknown-linux-gnu` | `.tar.gz` |
| Linux ARM64（Ubuntu 22.04 构建） | `tarla-core-aarch64-unknown-linux-gnu` | `.tar.gz` |
| macOS Apple Silicon | `tarla-core-aarch64-apple-darwin` | `.tar.gz` |
| Windows x64 | `tarla-core-x86_64-pc-windows-msvc` | `.zip` |

解开 GitHub 下载的外层 ZIP 后，目录里有目标平台的压缩包及同名 `.sha256` 文件。Linux 可用 `sha256sum -c *.sha256` 核对压缩包。macOS 可用 `shasum -a 256 -c *.sha256`。Windows 用 PowerShell 的 `Get-FileHash .\tarla-core-x86_64-pc-windows-msvc.zip -Algorithm SHA256`，将返回值与 `.sha256` 内容比对。解开平台压缩包后，`SHA256SUMS` 列出三个二进制的校验值。

在 Linux 或 macOS 的解压目录执行以下命令。

```sh
./tarla --workspace work init --node office
./tarlad --workspace work
```

在 Windows 上，用 PowerShell 打开解压目录并执行以下命令。

```powershell
.\tarla.exe --workspace work init --node office
.\tarlad.exe --workspace work
```

另开终端，在同一目录运行 `./tarla --workspace work status`（PowerShell 使用 `.\tarla.exe`）。模型、执行工位、可选网络边车与独立网关的配置见[项目说明](../README.md)。
