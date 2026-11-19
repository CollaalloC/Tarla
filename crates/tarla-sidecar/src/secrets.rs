//! Workspace config stores references. Secret bytes travel only to the
//! private sidecar configuration pipe.
use crate::invalid;
use std::io;

pub(crate) async fn resolve(reference: &str) -> io::Result<String> {
    if let Some(item) = reference.strip_prefix("keychain:") {
        if !item.starts_with("tarla/")
            || item.len() > 256
            || item
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == "..")
        {
            return Err(invalid("invalid Tarla keychain item"));
        }
        return keychain(item).await;
    }
    let name = reference.strip_prefix("env:").unwrap_or(reference);
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(invalid("expected env:NAME or keychain:tarla/..."));
    }
    std::env::var(name).map_err(|_| invalid("EasyTier secret environment variable unavailable"))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn keychain(item: &str) -> io::Result<String> {
    #[cfg(target_os = "macos")]
    let (tool, args) = (
        "/usr/bin/security",
        vec!["find-generic-password", "-s", item, "-w"],
    );
    #[cfg(target_os = "linux")]
    let (tool, args) = (
        "/usr/bin/secret-tool",
        vec!["lookup", "service", "tarla", "id", item],
    );
    let mut cmd = tokio::process::Command::new(tool);
    cmd.args(args)
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null());
    let output = tokio::time::timeout(std::time::Duration::from_secs(20), cmd.output()).await??;
    if !output.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "workspace secret unavailable from OS keychain",
        ));
    }
    let mut s = String::from_utf8(output.stdout).map_err(|_| invalid("invalid keychain text"))?;
    if s.ends_with('\n') {
        s.pop();
        if s.ends_with('\r') {
            s.pop();
        }
    }
    Ok(s)
}

#[cfg(windows)]
async fn keychain(item: &str) -> io::Result<String> {
    use windows_sys::Win32::Security::Credentials::{CredFree, CredReadW, CRED_TYPE_GENERIC};
    if item.contains('\0') {
        return Err(invalid("invalid credential name"));
    }
    let key: Vec<u16> = item.encode_utf16().chain(Some(0)).collect();
    let mut entry = std::ptr::null_mut();
    // CredReadW returns one system-owned allocation, including its blob.
    let bytes = unsafe {
        if CredReadW(key.as_ptr(), CRED_TYPE_GENERIC, 0, &mut entry) == 0 {
            return Err(io::Error::last_os_error());
        }
        let n = (*entry).CredentialBlobSize as usize;
        let buf = if n == 0 {
            Vec::new()
        } else {
            std::slice::from_raw_parts((*entry).CredentialBlob, n).to_vec()
        };
        CredFree(entry.cast());
        buf
    };
    if bytes.len() % 2 != 0 {
        return Err(invalid(
            "use a UTF-16LE Windows generic credential password",
        ));
    }
    let words: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|s| u16::from_le_bytes([s[0], s[1]]))
        .collect();
    String::from_utf16(&words).map_err(|_| invalid("invalid Windows credential password"))
}

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
async fn keychain(_item: &str) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "no OS keychain provider for this platform",
    ))
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn malformed_refs_are_rejected_before_lookup() {
        for r in [
            "keychain:another/credential",
            "keychain:tarla/../x",
            "env:",
            "env:no/slash",
        ] {
            assert!(super::resolve(r).await.is_err());
        }
    }
}
