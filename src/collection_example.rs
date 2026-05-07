// src/main.rs

use anyhow::{bail, Context, Result};
use futures::StreamExt;
use iroh::{endpoint::presets, protocol::Router, Endpoint};
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, AddProgressItem, ExportMode, ExportOptions, ImportMode},
        remote::GetProgressItem,
        TempTag,
    },
    format::collection::Collection,
    store::fs::FsStore,
    ticket::BlobTicket,
    BlobFormat, BlobsProtocol,
};
use std::{
    path::{Component, Path, PathBuf},
    str::FromStr,
};
use tempfile::TempDir;
use walkdir::WalkDir;

/// Validate that a single path segment is safe to use on disk.
///
/// This rejects `/` inside a component so we can safely reconstruct paths
/// from collection entries without accidentally creating nested or malicious paths.
fn validate_path_component(component: &str) -> Result<()> {
    anyhow::ensure!(
        !component.contains('/'),
        "path components must not contain /"
    );
    Ok(())
}

/// Convert a filesystem path into a normalized string that can be stored in the collection.
///
/// For a file tree transfer, the collection stores:
/// - a logical name for each entry
/// - the blob hash for the content of that entry
///
/// This helper makes sure the resulting path is relative and only contains
/// normal path components, not `..`, root markers, or weird platform-specific pieces.
fn canonicalized_path_to_string(path: impl AsRef<Path>, must_be_relative: bool) -> Result<String> {
    let mut path_str = String::new();

    let parts = path
        .as_ref()
        .components()
        .filter_map(|c| match c {
            // Normal path components are allowed.
            Component::Normal(x) => {
                let c = match x.to_str() {
                    Some(c) => c,
                    None => return Some(Err(anyhow::anyhow!("invalid character in path"))),
                };

                // Reject any component that contains separators.
                if !c.contains('/') && !c.contains('\\') {
                    Some(Ok(c))
                } else {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                }
            }

            // Root is only allowed if the caller explicitly says the path need not be relative.
            Component::RootDir => {
                if must_be_relative {
                    Some(Err(anyhow::anyhow!("invalid path component {:?}", c)))
                } else {
                    path_str.push('/');
                    None
                }
            }

            // Reject parent dir, current dir, prefix components, etc.
            _ => Some(Err(anyhow::anyhow!("invalid path component {:?}", c))),
        })
        .collect::<Result<Vec<_>>>()?;

    path_str.push_str(&parts.join("/"));
    Ok(path_str)
}

/// Import a file or folder into the blob store and build a collection from it.
///
/// The collection is the manifest that maps:
/// - entry name -> blob hash
///
/// For a folder, we walk the directory tree and add every file.
/// For a single file, the walk yields that file.
/// Each file is added to the blob store, and the resulting hash is inserted into the collection.
async fn import_tree(db: &FsStore, path: PathBuf) -> Result<(Collection, Vec<TempTag>)> {
    // Canonicalize the path so we work with a stable absolute path.
    let path = path.canonicalize()?;
    anyhow::ensure!(path.exists(), "path does not exist: {}", path.display());

    // Use the parent directory as the base so we can store paths relative to it.
    let root = path.parent().context("path has no parent")?;

    // Walk the input path recursively.
    let files = WalkDir::new(path.clone()).into_iter();

    // Collect all file paths as (logical name in collection, actual path on disk).
    let data_sources: Vec<(String, PathBuf)> = files
        .map(|entry| {
            let entry = entry?;
            // Skip directories; we only store files as blobs.
            if !entry.file_type().is_file() {
                return Ok(None);
            }

            let path = entry.into_path();

            // Strip the root prefix so the collection stores relative names.
            let relative = path.strip_prefix(root)?;
            let name = canonicalized_path_to_string(relative, true)?;

            Ok(Some((name, path)))
        })
        // Convert Vec<Option<_>> into Vec<_> while propagating errors.
        .filter_map(Result::transpose)
        .collect::<Result<Vec<_>>>()?;

    // This collection will become the manifest we send to the receiver.
    let mut collection = Collection::default();

    // Keep the temp tags only so the imported blobs remain referenced while we build the collection.
    // In this minimal example they are not otherwise used.
    let mut tags = Vec::new();

    // Add every file blob to the store and record its hash in the collection.
    for (name, path) in data_sources {
        // Import the file into the blob store.
        // TryReference means the store may avoid copying when possible.
        let mut stream = db
            .add_path_with_opts(AddPathOptions {
                path,
                mode: ImportMode::TryReference,
                format: BlobFormat::Raw,
            })
            .stream()
            .await;

        let mut temp_tag = None;

        // Consume the progress stream until the import completes.
        while let Some(item) = stream.next().await {
            match item {
                // Done returns a tag describing the imported blob.
                AddProgressItem::Done(tag) => {
                    temp_tag = Some(tag);
                    break;
                }
                // Surface any import failure immediately.
                AddProgressItem::Error(cause) => {
                    bail!("error importing {name}: {cause}");
                }
                // Ignore intermediate progress events.
                _ => {}
            }
        }

        let tag = temp_tag.context("import ended without a tag")?;

        // Add the entry to the collection: name -> blob hash.
        collection.push(name, tag.hash());
        tags.push(tag);
    }

    Ok((collection, tags))
}

/// Validate and reconstruct a safe output path for exported entries.
///
/// This prevents directory traversal when the receiver writes files back to disk.
fn export_path(root: &Path, name: &str) -> Result<PathBuf> {
    let mut path = root.to_path_buf();

    for part in name.split('/') {
        validate_path_component(part)?;
        path.push(part);
    }

    Ok(path)
}

/// Export every blob from the collection into the local filesystem.
///
/// The collection acts as the manifest, and each entry is restored by its blob hash.
async fn export_tree(db: &FsStore, collection: Collection) -> Result<()> {
    // Write received files into the current working directory.
    let root = std::env::current_dir()?;

    for (name, hash) in collection.iter() {
        let target = export_path(&root, name)?;

        // Do not overwrite existing files in this minimal example.
        if target.exists() {
            bail!("target already exists: {}", target.display());
        }

        // Create any missing parent directories.
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Export the blob to the target path.
        let mut stream = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target,
                mode: ExportMode::Copy,
            })
            .stream()
            .await;

        // Wait for the export operation to finish.
        while let Some(item) = stream.next().await {
            match item {
                iroh_blobs::api::blobs::ExportProgressItem::Done => break,
                iroh_blobs::api::blobs::ExportProgressItem::Error(cause) => {
                    bail!("error exporting {name}: {cause}");
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Run in sender mode.
///
/// This does three things:
/// 1. Load the input file/folder into the blob store.
/// 2. Build a collection that maps names to blob hashes.
/// 3. Print a ticket the receiver can use to fetch the data.
async fn run_send(path: PathBuf) -> Result<()> {
    // Use a temporary directory for the sender's local blob database.
    let temp_dir = TempDir::new()?;
    let store = FsStore::load(temp_dir.path()).await?;

    // Bind a local Iroh endpoint.
    let endpoint = Endpoint::bind(presets::N0).await?;

    // Import the input path and get the collection.
    let (collection, tags) = import_tree(&store, path).await?;

    // Store the collection itself as a blob, so it can be sent as a single root object.
    let temp_tag = collection.clone().store(&store).await?;

    // Keep tags alive long enough for the store to retain the content.
    drop(tags);

    // Start the endpoint so other peers can connect.
    let _ = endpoint.online().await;

    // Build a ticket that includes:
    // - the sender address
    // - the root hash of the stored collection
    // - the blob format
    let ticket = BlobTicket::new(endpoint.addr(), temp_tag.hash(), BlobFormat::HashSeq);

    // Create a protocol router that serves blob requests.
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, BlobsProtocol::new(&store, None))
        .spawn();

    println!("{ticket}");
    println!("recv with: cargo run -- recv {ticket}");

    // Keep the sender alive until interrupted.
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    drop(temp_dir);

    Ok(())
}

/// Download the blob data referenced by the ticket into the local store.
///
/// The receiver connects to the sender, asks for anything missing, and waits
/// until all required blobs have been transferred.
async fn download_collection(
    endpoint: &Endpoint,
    store: &FsStore,
    ticket: &BlobTicket,
) -> Result<()> {
    let hash_and_format = ticket.hash_and_format();

    // Check whether the target blob is already present locally.
    let local = store.remote().local(hash_and_format).await?;
    if local.is_complete() {
        return Ok(());
    }

    // Connect to the sender endpoint from the ticket.
    let connection = endpoint
        .connect(ticket.addr().clone(), iroh_blobs::protocol::ALPN)
        .await?;

    // Ask the remote peer for whatever pieces are missing.
    let mut stream = store
        .remote()
        .execute_get(connection, local.missing())
        .stream();

    // Drive the download to completion.
    while let Some(item) = stream.next().await {
        match item {
            GetProgressItem::Done(_) => break,
            GetProgressItem::Error(cause) => bail!("download failed: {cause}"),
            _ => {}
        }
    }

    Ok(())
}

/// Run in receiver mode.
///
/// This:
/// 1. Uses the ticket to fetch the collection blob.
/// 2. Loads the collection back from the blob store.
/// 3. Recreates the file tree on disk.
async fn run_recv(ticket: BlobTicket) -> Result<()> {
    // Use a temporary directory for the receiver's local blob database.
    let temp_dir = TempDir::new()?;
    let store = FsStore::load(temp_dir.path()).await?;
    let endpoint = Endpoint::bind(presets::N0).await?;

    // Fetch the collection blob and any missing dependencies.
    download_collection(&endpoint, &store, &ticket).await?;

    // Load the collection using the root hash from the ticket.
    let collection = Collection::load(ticket.hash(), store.as_ref()).await?;

    // Recreate the original directory/file structure on disk.
    export_tree(&store, collection).await?;

    endpoint.close().await;
    Ok(())
}

/// Entry point.
///
/// Supported commands:
/// - `send <path>`: import and publish a file or directory
/// - `recv <ticket>`: download and restore the collection
#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);

    match (args.next().as_deref(), args.next(), args.next()) {
        (Some("send"), Some(path), None) => run_send(PathBuf::from(path)).await?,
        (Some("recv"), Some(ticket), None) | (Some("receive"), Some(ticket), None) => {
            let ticket = BlobTicket::from_str(&ticket)?;
            run_recv(ticket).await?
        }
        _ => {
            eprintln!("usage:");
            eprintln!("  cargo run -- send <file-or-folder>");
            eprintln!("  cargo run -- recv <ticket>");
        }
    }

    Ok(())
}
