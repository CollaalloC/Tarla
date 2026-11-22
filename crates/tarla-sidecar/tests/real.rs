//! Optional upstream compatibility tests. Set TARLA_TEST_HERDR, TARLA_TEST_EASYTIER
//! or TARLA_TEST_TAILSCALED to the corresponding locally installed binary.
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io, path::PathBuf, process::Command, time::Duration};
use tarla_sidecar::{start_network, Herdr, NetworkSpec, TerminalEvent, Toolchain};
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

fn fixture(var: &str, tool: &str) -> io::Result<(TempDir, Toolchain, PathBuf)> {
    let src = std::env::var_os(var).ok_or_else(|| io::Error::other(format!("set {var}")))?;
    let tmp = tempfile::tempdir()?;
    let bin = tmp.path().join("bin");
    let dir = bin.join(tool).join("test");
    fs::create_dir_all(&dir)?;
    let file = dir.join("tool");
    fs::copy(src, &file)?;
    let hash = format!("{:x}", Sha256::digest(fs::read(&file)?));
    fs::write(
        bin.join("toolchain-manifest.json"),
        serde_json::to_vec(&json!({"tools":{
        (tool):{"version":"test","file":"tool","sha256":hash}}}))?,
    )?;
    Ok((tmp, Toolchain::load(&bin)?, file))
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires upstream herdr-core"]
async fn real_herdr_holder_and_scoped_stream() -> io::Result<()> {
    let (tmp, tools, binary) = fixture("TARLA_TEST_HERDR", "herdr-core")?;
    let rt = tmp.path().join("rt");
    let core = Herdr::new(&rt)?;
    struct Stop(PathBuf, PathBuf, PathBuf, String);
    impl Drop for Stop {
        fn drop(&mut self) {
            let _ = Command::new(&self.0)
                .env("HC_SESSION", &self.3)
                .env("HC_SOCKET", &self.2)
                .env("XDG_RUNTIME_DIR", self.1.join("hc-runtime"))
                .env("XDG_STATE_HOME", self.1.join("hc-state"))
                .env("XDG_CONFIG_HOME", self.1.join("hc-config"))
                .args(["server", "stop"])
                .output();
        }
    }
    // Even a timed-out startup may finish in the background. The cleanup
    // command addresses only this test's socket and state directory.
    let _cleanup = Stop(
        binary,
        rt.clone(),
        core.socket_path().to_path_buf(),
        core.session_name().to_owned(),
    );
    tools.ensure_herdr(&core).await?;
    // In the released core the 20th terminal has the first non-decimal
    // address. Create terminals through the real API so allocator changes and
    // launch results are covered, including rediscovery after reconnect.
    let core_id = core.machine_id().await?;
    for n in 1..20 {
        let run = format!("seed-{n}");
        let address = core
            .start_run(&run, &["/bin/true".into()], tmp.path(), &BTreeMap::new())
            .await?;
        assert_eq!(core.find_run(&run).await?, Some(address));
    }
    let id = core
        .start_run(
            "fixture",
            &["/bin/cat".into()],
            tmp.path(),
            &BTreeMap::new(),
        )
        .await?;
    assert_eq!(id, "t1b");
    assert_eq!(core.find_run("fixture").await?, Some(id.clone()));
    assert_eq!(core.status("fixture", &id).await?.state, "live");
    let (mut obs, mut reader_controls) = core.attach("fixture", &id, true, 80, 24).await?;
    assert_eq!(
        reader_controls.input(b"x").await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(reader_controls.resize(90, 45).await.is_err());
    let (_input_reader, mut input) = core.attach("fixture", &id, false, 80, 24).await?;
    input.resize(81, 25).await?;
    input.input(b"tarla-paint-probe\r").await?;
    timeout(Duration::from_secs(5), async {
        let mut seen = vec![];
        loop {
            match obs.next().await? {
                TerminalEvent::Output(bytes) => {
                    reader_controls.credit(bytes.len()).await?;
                    seen.extend(bytes);
                    if seen.windows(17).any(|s| s == b"tarla-paint-probe") {
                        break;
                    }
                }
                TerminalEvent::Resize(_, _) => (),
                TerminalEvent::Closed(_) => return Err(io::Error::other("premature close")),
            }
        }
        io::Result::Ok(())
    })
    .await??;
    assert!(core
        .read("fixture", &id, 5)
        .await?
        .contains("tarla-paint-probe"));
    reader_controls.close().await?;
    input.close().await?;
    // A fresh tarlad client sees the same authoritative terminal after detach.
    let second = Herdr::new(&rt)?;
    assert_eq!(second.machine_id().await?, core_id);
    assert_eq!(
        second.status("seed-1", &id).await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        second.read("seed-1", &id, 5).await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(
        matches!(second.attach("seed-1", &id, false, 80, 24).await, Err(e) if e.kind() == io::ErrorKind::PermissionDenied)
    );
    assert_eq!(
        second
            .send("seed-1", &id, "unrelated input")
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        second.cancel("seed-1", &id).await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(second.status("fixture", &id).await?.state, "live");
    assert_eq!(second.find_run("fixture").await?, Some(id.clone()));
    second.cancel("fixture", &id).await?;
    timeout(Duration::from_secs(5), async {
        while second.status("fixture", &id).await?.state != "finished" {
            sleep(Duration::from_millis(50)).await
        }
        io::Result::Ok(())
    })
    .await??;
    assert!(second
        .attach("fixture", "other/t1", false, 80, 24)
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires upstream EasyTier"]
async fn real_easytier_loopback_socks() -> io::Result<()> {
    let (tmp, tools, _) = fixture("TARLA_TEST_EASYTIER", "easytier")?;
    std::env::set_var("TARLA_E2E_MESH_SECRET", "test-private-identity");
    let rt = tmp.path().join("rt");
    let handle = start_network(
        &tools,
        &rt,
        &NetworkSpec::EasyTier {
            network_name: "test-net".into(),
            secret_env: "TARLA_E2E_MESH_SECRET".into(),
            peers: vec![],
        },
    )
    .await;
    if handle.is_err() {
        eprintln!(
            "{}",
            fs::read_to_string(rt.join("easytier.log")).unwrap_or_default()
        );
    }
    let handle = handle?.unwrap();
    assert!(handle.proxy_url().starts_with("socks5h://127.0.0.1:"));
    assert!(rt.join("easytier.json").exists());
    drop(handle);
    assert!(!rt.join("easytier.json").exists());
    Ok(())
}

#[tokio::test]
#[ignore = "requires upstream tailscaled"]
async fn real_tailscale_loopback_socks() -> io::Result<()> {
    let (tmp, tools, _) = fixture("TARLA_TEST_TAILSCALED", "tailscale")?;
    let rt = tmp.path().join("rt");
    let handle = start_network(&tools, &rt, &NetworkSpec::Tailscale).await;
    if handle.is_err() {
        eprintln!(
            "{}",
            fs::read_to_string(rt.join("tailscale.log")).unwrap_or_default()
        );
    }
    assert!(handle?
        .unwrap()
        .proxy_url()
        .starts_with("socks5h://127.0.0.1:"));
    Ok(())
}
#[test]
fn lock_is_exclusive_and_released_on_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let guard = tarla_sidecar::try_lock_slot(tmp.path(), "a").unwrap();
    assert!(tarla_sidecar::try_lock_slot(tmp.path(), "a").is_err());
    drop(guard);
    assert!(tarla_sidecar::try_lock_slot(tmp.path(), "a").is_ok());
    assert!(tarla_sidecar::try_lock_slot(tmp.path(), "..").is_err());
}

#[tokio::test]
#[ignore = "requires real EasyTier core and CLI"]
async fn real_easytier_two_node_private_ingress() -> io::Result<()> {
    use std::net::{Ipv4Addr, SocketAddr};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn node_info(runtime: &std::path::Path) -> io::Result<serde_json::Value> {
        let info: serde_json::Value =
            serde_json::from_slice(&fs::read(runtime.join("easytier.json"))?)?;
        let rpc = info["rpc"].as_str().unwrap();
        let cli = std::env::var_os("TARLA_TEST_EASYTIER_CLI")
            .ok_or_else(|| io::Error::other("set TARLA_TEST_EASYTIER_CLI"))?;
        let rst = tokio::process::Command::new(cli)
            .args(["-p", rpc, "-o", "json", "node", "info"])
            .output()
            .await?;
        if !rst.status.success() {
            return Err(io::Error::other("node info unavailable"));
        }
        Ok(serde_json::from_slice(&rst.stdout)?)
    }
    async fn peer_summary(runtime: &std::path::Path) -> String {
        let Some(cli) = std::env::var_os("TARLA_TEST_EASYTIER_CLI") else {
            return "missing CLI".into();
        };
        let record = fs::read(runtime.join("easytier.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
        let Some(rpc) = record.as_ref().and_then(|r| r["rpc"].as_str()) else {
            return "missing endpoint".into();
        };
        // This fixture owns synthetic peers only. 'peer' contains routing
        // metadata, unlike 'node info', which can include network credentials.
        let result = timeout(
            Duration::from_secs(3),
            tokio::process::Command::new(cli)
                .args(["-p", rpc, "-o", "json", "peer"])
                .output(),
        )
        .await;
        match result {
            Ok(Ok(r)) => String::from_utf8_lossy(&r.stdout)
                .chars()
                .take(2000)
                .collect(),
            _ => "peer diagnostics unavailable".into(),
        }
    }
    async fn proxy_echo(proxy: SocketAddr, target: Ipv4Addr, port: u16) -> io::Result<()> {
        let mut conn = TcpStream::connect(proxy).await?;
        conn.write_all(&[5, 1, 0]).await?;
        let mut auth = [0u8; 2];
        conn.read_exact(&mut auth).await?;
        let mut req = vec![5, 1, 0, 1];
        req.extend(target.octets());
        req.extend(port.to_be_bytes());
        conn.write_all(&req).await?;
        let mut rep = [0u8; 4];
        conn.read_exact(&mut rep).await?;
        if rep[1] != 0 {
            return Err(io::Error::other("mesh connect rejected"));
        }
        let rest = match rep[3] {
            1 => 6,
            4 => 18,
            3 => conn.read_u8().await? as usize + 2,
            _ => return Err(io::Error::other("invalid address")),
        };
        conn.read_exact(&mut vec![0; rest]).await?;
        conn.write_all(b"mesh").await?;
        let mut bytes = [0u8; 4];
        conn.read_exact(&mut bytes).await?;
        if bytes != *b"mesh" {
            return Err(io::Error::other("bad echo"));
        }
        Ok(())
    }
    async fn echo_server() -> io::Result<SocketAddr> {
        let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = server.local_addr()?;
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = server.accept().await else {
                    return;
                };
                let mut data = [0u8; 4];
                if s.read_exact(&mut data).await.is_ok() {
                    let _ = s.write_all(&data).await;
                }
            }
        });
        Ok(addr)
    }

    let (tmp, tools, _) = fixture("TARLA_TEST_EASYTIER", "easytier")?;
    std::env::set_var("TARLA_TEST_MESH_PAIR_SECRET", "paired-test");
    let net = |ipv4: &str, peers| NetworkSpec::EasyTierStatic {
        ipv4: ipv4.to_owned(),
        network_name: "same-mesh".into(),
        secret_env: "TARLA_TEST_MESH_PAIR_SECRET".into(),
        peers,
    };
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    let api = echo_server().await?;
    let other = echo_server().await?;
    let _a =
        tarla_sidecar::start_network_for(&tools, &a, &net("10.74.64.1/24", vec![]), api).await?;
    let info = timeout(Duration::from_secs(12), async {
        loop {
            if let Ok(v) = node_info(&a).await {
                let listeners = v["listeners"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|s| s.as_str());
                if v["ipv4_addr"] == "10.74.64.1/24"
                    && listeners.into_iter().any(|s| s.starts_with("tcp://"))
                {
                    break v;
                }
            }
            sleep(Duration::from_millis(150)).await
        }
    })
    .await
    .map_err(|_| {
        io::Error::other("primary overlay did not acquire configured address and TCP listener")
    })?;
    let node_ip = Ipv4Addr::new(10, 74, 64, 1);
    let advertised = info["listeners"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .find_map(|v| v.strip_prefix("tcp://"))
        .unwrap()
        .parse::<SocketAddr>()
        .map_err(|_| io::Error::other("unexpected EasyTier listener address"))?;
    // Both native binaries run on this runner. Explicitly connect via loopback
    // even when the runner advertises an external or container-only interface.
    let listener = format!("tcp://127.0.0.1:{}", advertised.port());
    let _b = start_network(&tools, &b, &net("10.74.64.2/24", vec![listener])).await?;
    let endpoint: serde_json::Value = serde_json::from_slice(&fs::read(b.join("easytier.json"))?)?;
    let proxy: SocketAddr = endpoint["socks5"].as_str().unwrap().parse().unwrap();
    let mut last_error = String::new();
    let connected = timeout(Duration::from_secs(35), async {
        loop {
            match timeout(
                Duration::from_secs(3),
                proxy_echo(proxy, node_ip, api.port()),
            )
            .await
            {
                Ok(Ok(())) => break,
                Ok(Err(e)) => last_error = e.to_string(),
                Err(_) => last_error = "timed out waiting for TCP echo".to_owned(),
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    if connected.is_err() {
        let b_info = node_info(&b).await?;
        return Err(io::Error::other(format!(
            "two-node echo failed: {}; A={}, B={}, loopback seed port={}, app port={}, A peers={}, B peers={}",
            last_error,
            info["ipv4_addr"],
            b_info["ipv4_addr"],
            advertised.port(),
            api.port(), peer_summary(&a).await, peer_summary(&b).await
        )));
    }
    assert!(!matches!(
        timeout(
            Duration::from_secs(2),
            proxy_echo(proxy, node_ip, other.port())
        )
        .await,
        Ok(Ok(()))
    ));
    Ok(())
}
