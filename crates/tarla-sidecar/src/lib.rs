//! Private workspace-owned execution and network sidecars.
mod herdr;
mod managed;
mod network;
mod secrets;
pub use herdr::{Herdr, TerminalEvent, TerminalReader, TerminalStatus, TerminalWriter};
pub use managed::{credential_environment, try_lock_slot, Toolchain};
pub use network::{start_network, start_network_for, ManagedNetwork, NetworkPorts, NetworkSpec};

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
