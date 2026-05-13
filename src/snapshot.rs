use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use blake3::Hasher;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

// ── Types ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: PathBuf,
    pub size: u64,
    pub modified: u64, // seconds since UNIX epoch
    pub hash: String,  // hex-encoded BLAKE3
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub root: PathBuf,
    pub taken_at: u64,
    pub files: HashMap<PathBuf, FileEntry>,
}

#[derive(Debug)]
pub enum Change {
    Added(FileEntry),
    Deleted(PathBuf),
    Modified { before: FileEntry, after: FileEntry },
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

impl Snapshot {
    /// Walk `root` and hash every file.
    pub fn capture(root: impl AsRef<Path>) -> eyre::Result<Self> {
        let root = root.as_ref().canonicalize()?;
        let mut files = HashMap::new();

        for entry in WalkDir::new(&root)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let abs = entry.path().to_path_buf();
            let rel = abs.strip_prefix(&root)?.to_path_buf();

            let meta = fs::metadata(&abs)?;
            let size = meta.len();
            let modified = meta.modified()?.duration_since(UNIX_EPOCH)?.as_secs();

            let hash = hash_file(&abs)?;

            files.insert(
                rel.clone(),
                FileEntry {
                    path: rel,
                    size,
                    modified,
                    hash,
                },
            );
        }

        Ok(Self {
            root: root.clone(),
            taken_at: std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_secs(),
            files,
        })
    }

    /// Persist snapshot to a JSON file.
    pub fn save(&self, path: impl AsRef<Path>) -> eyre::Result<()> {
        let f = File::create(path)?;
        serde_json::to_writer_pretty(f, self)?;
        Ok(())
    }

    /// Load snapshot from a JSON file.
    pub fn load(path: impl AsRef<Path>) -> eyre::Result<Self> {
        let f = File::open(path)?;
        Ok(serde_json::from_reader(BufReader::new(f))?)
    }

    pub fn diff(&self, after: &Snapshot) -> Vec<Change> {
        let mut changes = Vec::new();

        // Files present in `after` — added or modified
        for (path, after_entry) in &after.files {
            match self.files.get(path) {
                None => changes.push(Change::Added(after_entry.clone())),
                Some(before_entry) if before_entry.hash != after_entry.hash => {
                    changes.push(Change::Modified {
                        before: before_entry.clone(),
                        after: after_entry.clone(),
                    });
                }
                _ => {} // identical hash → unchanged
            }
        }

        // Files present in `before` but gone in `after` → deleted
        for path in self.files.keys() {
            if !after.files.contains_key(path) {
                changes.push(Change::Deleted(path.clone()));
            }
        }

        changes
    }
}

impl Change {
    pub fn get_path(&self) -> PathBuf {
        match self {
            Change::Added(f) => &f.path,
            Change::Deleted(p) => p,
            Change::Modified { after, .. } => &after.path,
        }
        .clone()
    }
}

fn hash_file(path: &Path) -> eyre::Result<String> {
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 65536]; // 64 KiB chunks
    let mut f = BufReader::new(File::open(path)?);
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}
