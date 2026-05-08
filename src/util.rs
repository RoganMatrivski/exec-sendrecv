use std::path::{Path, PathBuf};

use sha2::Digest;
use walkdir::WalkDir;

pub fn ensure_dir(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty path",
        ));
    }
    std::fs::create_dir_all(path)?;
    Ok(path.to_path_buf())
}

pub fn get_device_code() -> String {
    use machineid_rs::{Encryption, HWIDComponent, IdBuilder};

    let mut builder = IdBuilder::new(Encryption::SHA256);
    let fingerprint = builder
        .add_component(HWIDComponent::SystemID)
        .add_component(HWIDComponent::Username)
        .build("")
        .expect("Failed getting device fingerprint");

    let bytes = fingerprint.as_bytes();
    let hash = sha2::Sha256::digest(bytes);
    let n = u64::from_le_bytes(hash[..8].try_into().unwrap());
    let id = (n % 9_900_000_000) + 100_000_000;
    format!("{id}")
}

#[cfg(unix)]
pub fn is_executable(path: &Path) -> bool {
    tracing::trace!("Checking if {path:?} is executable in Unix...");
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
pub fn is_executable(path: &Path) -> bool {
    tracing::trace!("Checking if {path:?} is executable in Windows...");
    path.extension()
        .map(|e| e == "exe" || e == "bat" || e == "cmd")
        .unwrap_or(false)
}

pub fn find_executable_or_first(dir: &Path) -> Option<PathBuf> {
    let files: Vec<PathBuf> = WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();

    if let Some(exec) = files.iter().find(|p| {
        is_executable(p)
            && !matches!(
                p.extension().and_then(|e| e.to_str()),
                Some("exe" | "bat" | "cmd" | "dll")
            )
    }) {
        return Some(exec.clone());
    }

    if let Some(exec) = files.iter().find(|p| is_executable(p)) {
        return Some(exec.clone());
    }

    files.into_iter().next()
}
