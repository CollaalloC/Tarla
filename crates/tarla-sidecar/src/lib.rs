//! Private workspace-owned execution and network sidecars.
mod events;
mod herdr;
mod managed;
mod network;
mod secrets;
mod templates;
pub use events::{parse_event_frame, EventStream, HerdrEvent, HerdrEventKind};
pub use herdr::{
    DocRecord, Herdr, TerminalEvent, TerminalReader, TerminalSpawnSpec, TerminalStatus,
    TerminalWriter, MAX_HERDR_DOC_BYTES, MAX_HERDR_FRAME_BYTES, MAX_TERMINAL_INPUT_BYTES,
};
pub use managed::{credential_environment, try_lock_slot, Toolchain};
pub use network::{start_network, start_network_for, ManagedNetwork, NetworkPorts, NetworkSpec};
pub use templates::{
    render_core_config_toml, render_template_markdown, sync_core_templates, AgentTemplateSpec,
};

use std::{fs, io, path::Path};
pub(crate) fn invalid(s: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, s.into())
}
pub(crate) fn private_dir(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_dir() || meta.file_type().is_symlink() {
        return Err(invalid("expected a private directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if meta.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("private directory must have mode 0700: {}", path.display()),
            ));
        }
    }
    Ok(())
}

/// macOS and BSD kernels cap `sockaddr_un.sun_path` at 104 bytes (103 non-NUL bytes).
/// Keep short runtime sockets inside the workspace directory, and fall back to a
/// UID-verified `0700` `/tmp/tarla-<uid>` directory when deep paths exceed 96 bytes.
#[cfg(unix)]
pub(crate) fn unix_socket_path(
    runtime: &Path,
    filename: &str,
    prefix: &str,
) -> io::Result<std::path::PathBuf> {
    use sha2::{Digest, Sha256};
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let direct = runtime.join(filename);
    if direct.as_os_str().as_encoded_bytes().len() <= 96 {
        return Ok(direct);
    }
    let uid = unsafe { libc::geteuid() };
    let base = std::path::PathBuf::from(format!("/tmp/tarla-{uid}"));
    private_dir(&base)?;
    let meta = fs::symlink_metadata(&base)?;
    if meta.uid() != uid || (meta.permissions().mode() & 0o777) != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "short socket directory is not exclusively owned by uid {uid}: {}",
                base.display()
            ),
        ));
    }
    let digest = format!(
        "{:x}",
        Sha256::digest(direct.as_os_str().as_encoded_bytes())
    );
    Ok(base.join(format!("{prefix}-{}.sock", &digest[..24])))
}
