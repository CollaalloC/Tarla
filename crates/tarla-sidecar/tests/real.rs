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
    tools.ensure_herdr(&core).await?;
    struct Stop(PathBuf, PathBuf);
    impl Drop for Stop {
        fn drop(&mut self) {
            let _ = Command::new(&self.0)
                .env("HC_SESSION", "tarla")
                .env("HC_SOCKET", self.1.join("hc.sock"))
                .env("XDG_RUNTIME_DIR", self.1.join("hc-runtime"))
                .env("XDG_STATE_HOME", self.1.join("hc-state"))
                .env("XDG_CONFIG_HOME", self.1.join("hc-config"))
                .args(["server", "stop"])
                .output();
        }
    }
    let _cleanup = Stop(binary, rt.clone());
    let id = core
        .start_run(
            "fixture",
            &["/bin/cat".into()],
            tmp.path(),
            &BTreeMap::new(),
        )
        .await?;
    assert_eq!(core.find_run("fixture").await?, Some(id.clone()));
    assert_eq!(core.status(&id).await?.state, "live");
    let (mut obs, mut reader_controls) = core.attach(&id, true, 80, 24).await?;
    assert_eq!(
        reader_controls.input(b"x").await.unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    assert!(reader_controls.resize(90, 45).await.is_err());
    let (_input_reader, mut input) = core.attach(&id, false, 80, 24).await?;
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
    assert!(core.read(&id, 5).await?.contains("tarla-paint-probe"));
    reader_controls.close().await?;
    input.close().await?;
    // A fresh tarlad client sees the same authoritative terminal after detach.
    let second = Herdr::new(&rt)?;
    assert_eq!(second.find_run("fixture").await?, Some(id.clone()));
    second.cancel(&id).await?;
    timeout(Duration::from_secs(5), async {
        while second.status(&id).await?.state != "finished" {
            sleep(Duration::from_millis(50)).await
        }
        io::Result::Ok(())
    })
    .await??;
    assert!(second.attach("other/t1", false, 80, 24).await.is_err());
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
