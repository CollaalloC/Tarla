# Upstream components

Tarla's workspace, message normalization, context, memory and orchestration modules are original Rust implementations. [Hermes Agent](https://github.com/NousResearch/hermes-agent) inspired the profile, gateway, context and memory interfaces.

The repository contains adapters for separate, operator-provisioned processes. It does not redistribute upstream binaries. Consult the license accompanying the exact distribution you install.

| Component | Integration | Upstream |
| --- | --- | --- |
| herdr-core | Private local socket or named pipe for an executor's persistent terminals | [herdrdev/herdr-core](https://github.com/herdrdev/herdr-core) |
| EasyTier | Optional workspace-specific userspace mesh | [EasyTier/EasyTier](https://github.com/EasyTier/EasyTier) |
| Tailscale | Optional isolated tailscaled state and userspace SOCKS endpoint | [tailscale/tailscale](https://github.com/tailscale/tailscale) |
