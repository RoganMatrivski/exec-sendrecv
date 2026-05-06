use std::{
    collections::BTreeSet,
    fmt::{self, Display},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use color_eyre::{
    eyre::{Context, ContextCompat},
    Report,
};
use futures_util::StreamExt;
use iroh::{
    endpoint::presets,
    protocol::{ProtocolHandler, Router},
    Endpoint, EndpointAddr,
};
use iroh_blobs::{
    api::{blobs::AddProgressItem, downloader::DownloadProgressItem},
    store::mem::MemStore,
    ticket::BlobTicket,
    BlobsProtocol,
};
use sha2::Digest;
use indicatif::{ProgressBar, ProgressStyle};

mod broker;
mod init;

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ALPN: &[u8] = b"i/dont/like/this/rock/robert";
const BROKER_ALPN: &[u8] = b"i/dont/like/this/rock/robert/broker";

#[derive(Clone)]
struct TicketReceiver {
    store: MemStore,
    endpoint: Endpoint,
    filedir: Option<PathBuf>,
    on_recv: Option<Arc<dyn Fn(PathBuf) + Send + Sync>>,
}

impl fmt::Debug for TicketReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TicketReceiver")
            .field("store", &self.store)
            .field("endpoint", &self.endpoint)
            .field("filedir", &self.filedir)
            .field("on_recv", &self.on_recv.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Payload<B: Display, F: Display> {
    blob: B,
    filename: F,
}

impl ProtocolHandler for TicketReceiver {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let store = self.store.clone();
        let endpoint = self.endpoint.clone();

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("{msg} [{spinner}] {pos} bytes")
                .expect("invalid progress style"),
        );
        pb.enable_steady_tick(Duration::from_millis(80));
        pb.set_message("receiving ticket");

        let result: Result<(), iroh::protocol::AcceptError> = async {
            let (mut send_ack, mut recv) = conn.accept_bi().await?;

            let mut buf = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;

            let payload = String::from_utf8(buf).expect("Failed to parse payload");
            let payload: Payload<BlobTicket, std::borrow::Cow<'_, str>> =
                serde_json::from_str(&payload).expect("Failed parsing payload");
            let ticket: BlobTicket = payload.blob;

            let dest = if let Some(d) = &self.filedir {
                tempfile::NamedTempFile::new_in(d)
            } else {
                tempfile::NamedTempFile::new()
            }
            .expect("Failed to create temporary file")
            .into_temp_path()
            .keep()
            .expect("Failed to get temporary path");

            pb.set_message("downloading blob");

            let dl = store.downloader(&endpoint);
            let mut progress = dl
                .download(ticket.hash(), Some(ticket.addr().id))
                .stream()
                .await
                .expect("Failed to start downloading");

            while let Some(item) = progress.next().await {
                match item {
                    DownloadProgressItem::TryProvider { .. } => {
                        pb.set_message("trying provider");
                    }
                    DownloadProgressItem::ProviderFailed { .. } => {
                        pb.set_message("provider failed; trying next");
                    }
                    DownloadProgressItem::Progress(n) => {
                        pb.set_position(n);
                    }
                    DownloadProgressItem::PartComplete { .. } => {
                        pb.set_message("download complete");
                    }
                    DownloadProgressItem::DownloadError => {
                        pb.abandon_with_message("download error");
                        return Err(std::io::Error::other("download error").into());
                    }
                    DownloadProgressItem::Error(err) => {
                        pb.abandon_with_message("failed");
                        return Err(err.into());
                    }
                }
            }

            pb.set_message("writing to disk");
            store
                .blobs()
                .export(ticket.hash(), &dest)
                .await
                .expect("Failed copying from memory to local");

            if let Some(f) = self.on_recv.clone() {
                f(dest.to_path_buf());
            }

            tokio::io::AsyncWriteExt::write_all(&mut send_ack, b"done").await?;
            send_ack.finish()?;

            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                pb.finish_with_message("received");
                Ok(())
            }
            Err(err) => {
                pb.abandon_with_message("failed");
                Err(err)
            }
        }
    }
}

pub fn ensure_dir(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
    let path = path.as_ref();

    if path.as_os_str().is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "empty path"));
    }

    // Creates it if missing; succeeds if it already exists as a directory.
    std::fs::create_dir_all(&path)?;

    Ok(path.to_path_buf())
}

pub enum Handler {
    Send(String, String, PathBuf),
    Receive(
        String,
        Option<Arc<dyn Fn(PathBuf) -> () + Send + Sync>>,
        Option<PathBuf>,
    ),
    Broker(String),
}

impl Handler {
    pub async fn run(&self) -> color_eyre::eyre::Result<()> {
        match self {
            Handler::Send(broker_id, recv_code, path) => {
                let endpoint = get_endpoint_builder()?.bind().await?;
                let store = MemStore::new();

                // Derive broker's PublicKey from the shared broker_id
                let broker_key = broker::broker_public_key(broker_id);

                let recv_code = recv_code.split_whitespace().collect::<Vec<_>>().join("");

                // Ask broker for the receiver's PublicKey
                tracing::info!("Looking up receiver via broker...");
                let receiver_key = broker::broker_lookup(&endpoint, broker_key, &recv_code).await?;
                tracing::info!(?receiver_key, "Found receiver");

                let blobs = BlobsProtocol::new(&store, None);

                tracing::debug!(?path, "Hashing file");

                let add = store.blobs().add_path(&path);
                let mut stream = add.stream().await;

                let pb = ProgressBar::new(0);
                pb.set_style(
                    ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {pos}/{len}")
                        .expect("invalid progress style"),
                );
                pb.set_message("adding file");

                let mut tag = None;

                while let Some(item) = stream.next().await {
                    match item {
                        AddProgressItem::Size(size) => {
                            pb.set_length(size);
                        }
                        AddProgressItem::CopyProgress(size)
                        | AddProgressItem::OutboardProgress(size) => {
                            pb.set_position(size);
                        }
                        AddProgressItem::CopyDone => {
                            pb.set_message("hashing");
                        }
                        AddProgressItem::Done(tt) => {
                            tag = Some(tt);
                            break;
                        }
                        AddProgressItem::Error(err) => {
                            pb.abandon_with_message("failed");
                            return Err(err.into());
                        }
                    }
                }

                let tt = tag.expect("add_path ended without Done");
                let tag = tt.hash_and_format();
                pb.finish_with_message("done");

                let ticket = BlobTicket::new(endpoint.addr(), tag.hash, tag.format);
                let filename = path
                    .file_name()
                    .wrap_err("Failed getting filename from path")?
                    .to_string_lossy();

                let payload = Payload {
                    blob: ticket,
                    filename,
                };

                let addr = EndpointAddr {
                    id: receiver_key,       // your PublicKey
                    addrs: BTreeSet::new(), // empty set -> discovery will be used
                };

                let conn = endpoint
                    .connect(addr, ALPN)
                    .await
                    .wrap_err("Failed to connect to iroh endpoint")?;

                let (mut send, mut recv_ack) = conn.open_bi().await?;

                tokio::io::AsyncWriteExt::write_all(
                    &mut send,
                    serde_json::to_string(&payload)?.as_bytes(),
                )
                .await?;
                send.finish()?;

                let router = Router::builder(endpoint)
                    .accept(iroh_blobs::ALPN, blobs)
                    .spawn();

                // wait for receiver to signal done
                let mut ack = Vec::new();
                tokio::io::AsyncReadExt::read_to_end(&mut recv_ack, &mut ack).await?;

                conn.close(0u32.into(), b"bye");
                drop(conn);

                tracing::info!("Receiver done, shutting down.");
                tokio::time::timeout(std::time::Duration::from_secs(3), router.shutdown())
                    .await
                    .ok();
            }
            Handler::Receive(broker_id, on_recv, filedir) => {
                let endpoint = get_endpoint_builder()?.bind().await?;
                let store = MemStore::new();

                let id = endpoint.id();
                let fingerprint = get_device_code();
                tracing::info!(?id, "App ID: {fingerprint}");
                let key = endpoint.id();

                // Derive broker's PublicKey and register ourselves
                let broker_key = broker::broker_public_key(&broker_id);
                broker::broker_register(&endpoint, broker_key, &fingerprint, key).await?;

                // Split digits to three like rustdesk or anydesk
                let fingerprint = {
                    use digit_group::FormatGroup;

                    fingerprint
                        .parse::<usize>()?
                        .format_custom('.', ' ', 3, 3, false)
                };

                println!("Your code (give this to sender): {fingerprint}");
                tracing::info!("Registered with broker. Waiting for sender...");

                let handler = TicketReceiver {
                    store,
                    filedir: filedir.clone(),
                    endpoint: endpoint.clone(),
                    on_recv: on_recv.clone(),
                };

                let router = Router::builder(endpoint).accept(ALPN, handler).spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
            Handler::Broker(client_id) => {
                let secret_key = broker::derive_secret_key(&client_id);

                let endpoint = get_endpoint_builder()?
                    .secret_key(secret_key)
                    .bind()
                    .await?;

                tracing::info!("Broker pubkey: {}", endpoint.id());

                let handler = broker::BrokerHandler::default();

                let router = Router::builder(endpoint)
                    .accept(BROKER_ALPN, handler)
                    .spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
        }

        Ok(())
    }
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

fn get_endpoint_builder() -> color_eyre::eyre::Result<iroh::endpoint::Builder> {
    let endpoint_builder = Endpoint::builder(presets::N0)
        .addr_filter(iroh::endpoint_info::AddrFilter::unfiltered())
        .address_lookup(iroh::address_lookup::PkarrPublisher::n0_dns())
        .address_lookup(iroh::address_lookup::DnsAddressLookup::n0_dns())
        .address_lookup(iroh::address_lookup::mdns::MdnsAddressLookup::builder());

    Ok(endpoint_builder)
}

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;
    let broker_id = args.broker_id;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(1);

    // Spawn a dedicated thread for process management (blocking ops)
    let exec_runner = tokio::task::spawn_blocking(move || {
        use std::process::{Child, Command};
        let mut child_handle: Option<Child> = None;

        while let Some(path) = rx.blocking_recv() {
            // Kill old process + its entire tree
            if let Some(mut child) = child_handle.take() {
                kill_tree(&mut child);
            }

            match Command::new(&path).spawn() {
                Ok(child) => {
                    println!("Spawned: {:?} (pid {})", path, child.id());
                    child_handle = Some(child);
                }
                Err(e) => eprintln!("Failed to spawn {:?}: {e}", path),
            }
        }

        if let Some(mut child) = child_handle.take() {
            kill_tree(&mut child);
        }
    });

    match args.command {
        init::AppSubcommand::Send { key, file } => {
            Handler::Send(broker_id, key, file)
        }
        init::AppSubcommand::Receive { filedir } => Handler::Receive(
            broker_id,
            Some(std::sync::Arc::new(move |p| {
                if let Err(e) = tx.try_send(p) {
                    tracing::warn!(?e, "Failed to send to exec_runner");
                }
            })),
            filedir,
        ),
        init::AppSubcommand::Broker => Handler::Broker(broker_id),
    }
    .run()
    .await?;

    exec_runner.await?;

    Ok(())
}

fn kill_tree(child: &mut std::process::Child) {
    let pid = child.id();

    #[cfg(windows)]
    {
        // /F = force, /T = include all child processes
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .output();
    }

    #[cfg(not(windows))]
    {
        // On Unix, kill the process group instead
        unsafe {
            libc::killpg(pid as i32, libc::SIGKILL);
        }
        // fallback
        let _ = child.kill();
    }

    let _ = child.wait();
}
