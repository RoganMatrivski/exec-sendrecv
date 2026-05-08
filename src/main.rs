use std::{
    collections::BTreeSet,
    fmt::{self},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::Duration,
};

use color_eyre::{eyre::Context, Report};
use iroh::{
    endpoint::presets,
    protocol::{ProtocolHandler, Router},
    Endpoint, EndpointAddr,
};
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, ExportMode, ExportOptions, ImportMode},
        Store, TempTag,
    },
    format::collection::Collection,
    store::fs::FsStore,
    ticket::BlobTicket,
    BlobFormat, BlobsProtocol, Hash, HashAndFormat,
};
use sha2::Digest;
use std::io::Stderr;
use tracing::Instrument;
use walkdir::WalkDir;

use crate::init::ProgressBarLogWriter;

mod broker;
mod init;

pub static MPB: LazyLock<ProgressBarLogWriter<Stderr>> =
    LazyLock::new(|| ProgressBarLogWriter::default());

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const ALPN: &[u8] = b"i/dont/like/this/rock/robert";
const BROKER_ALPN: &[u8] = b"i/dont/like/this/rock/robert/broker";

#[derive(Clone)]
struct TicketReceiver {
    node: Node,
    filedir: Option<PathBuf>,
    on_recv: Option<Arc<dyn Fn(PathBuf) + Send + Sync>>,
}

impl fmt::Debug for TicketReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TicketReceiver")
            .field("store", &self.node.store)
            .field("endpoint", &self.node.endpoint())
            .field("filedir", &self.filedir)
            .field("on_recv", &self.on_recv.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    tracing::trace!("Checking if {path:?} is executable in Unix...");
    use std::os::unix::fs::PermissionsExt;
    path.metadata()
        .map(|m| m.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    tracing::trace!("Checking if {path:?} is executable in Windows...");
    path.extension()
        .map(|e| e == "exe" || e == "bat" || e == "cmd")
        .unwrap_or(false)
}

fn find_executable_or_first(dir: &Path) -> Option<PathBuf> {
    let files: Vec<PathBuf> = WalkDir::new(dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
        .collect();

    // Try to find first executable
    if let Some(exec) = files.iter().find(|p| {
        is_executable(p)
            && !matches!(
                p.extension().and_then(|e| e.to_str()),
                Some("exe" | "bat" | "cmd" | "dll")
            )
    }) {
        return Some(exec.clone());
    }
    // Fallback: any executable (including .exe)
    if let Some(exec) = files.iter().find(|p| is_executable(p)) {
        return Some(exec.clone());
    }

    // Fallback: return first file regardless
    files.into_iter().next()
}

impl ProtocolHandler for TicketReceiver {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let conn_id = format!("{:?}", conn.remote_id());

        let span = tracing::info_span!(
            "ticket_receiver.accept",
            %conn_id,
        );

        async move {
            tracing::info!("accepting incoming ticket transfer");

            let store = self.node.store.clone();

            let result: Result<(), iroh::protocol::AcceptError> = async {
                tracing::debug!("waiting for bidi stream");

                let (mut send_ack, mut recv) = conn.accept_bi().await?;

                tracing::debug!("bidi stream accepted");

                let mut buf = Vec::new();

                tracing::trace!("reading incoming payload");

                tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;

                tracing::debug!(payload_size = buf.len(), "received payload bytes");

                let payload = String::from_utf8(buf).expect("Failed to parse payload");

                tracing::trace!(
                    payload_len = payload.len(),
                    payload = %payload,
                    "decoded payload string"
                );

                let ticket: BlobTicket = payload.parse().expect("Failed parsing payload to Ticket");

                tracing::info!(
                    hash = %ticket.hash(),
                    addr = ?ticket.addr(),
                    format = ?ticket.format(),
                    "parsed blob ticket"
                );

                tracing::info!(
                    source = ?ticket.addr(),
                    "starting collection download"
                );

                self.node
                    .get_collection(ticket.hash(), ticket.addr().clone())
                    .await
                    .expect("Failed to download collection");

                tracing::info!("collection download completed");

                use iroh_blobs::format::collection::Collection;

                tracing::debug!(
                    hash = %ticket.hash(),
                    "loading collection metadata"
                );

                let collection = Collection::load(ticket.hash(), &store).await?;

                tracing::info!(files = collection.len(), "loaded collection");

                // Choose an output directory.
                let dest_root = if let Some(d) = &self.filedir {
                    tracing::debug!(
                        dir = %d.display(),
                        "using configured file output directory"
                    );

                    ensure_dir(d).expect("Failed to create destination directory");

                    tempfile::tempdir_in(d)
                        .expect("Failed to create temp output dir")
                        .keep()
                } else {
                    tracing::debug!("using system temporary directory");

                    tempfile::tempdir()
                        .expect("Failed to create temp output dir")
                        .keep()
                };

                tracing::info!(
                    path = %dest_root.display(),
                    "created destination root"
                );

                for (name, hash) in collection.iter() {
                    let export_span = tracing::debug_span!(
                        "export_blob",
                        file = %name,
                        hash = %hash,
                    );

                    async {
                        let target = dest_root.join(name);

                        tracing::debug!(
                            target = %target.display(),
                            "exporting blob"
                        );

                        store
                            .export_with_opts(ExportOptions {
                                hash: hash.clone(),
                                target: target.clone(),
                                mode: ExportMode::TryReference,
                            })
                            .await
                            .expect("Failed to export file from Store");

                        tracing::info!(
                            target = %target.display(),
                            "export completed"
                        );
                    }
                    .instrument(export_span)
                    .await;
                }

                // For single-file transfers, keep the old behavior and hand back the file path.
                // For folders, hand back the output directory.
                let base_path = if collection.len() == 1 {
                    let (name, _) = collection.iter().next().unwrap();
                    dest_root.join(name)
                } else {
                    dest_root.clone()
                };

                let recv_path = if base_path.is_dir() {
                    find_executable_or_first(&base_path).unwrap_or(base_path)
                } else {
                    base_path
                };

                tracing::info!(
                    recv_path = %recv_path.display(),
                    "resolved receive path"
                );

                if let Some(f) = self.on_recv.clone() {
                    tracing::debug!("invoking receive callback");

                    f(recv_path);

                    tracing::debug!("receive callback completed");
                } else {
                    tracing::trace!("no receive callback registered");
                }
                tracing::info!("receiver finished restore; sending ack");

                tokio::io::AsyncWriteExt::write_all(&mut send_ack, b"done").await?;

                tracing::debug!("receiver finishing ack stream");
                send_ack.finish()?;

                tracing::debug!("waiting for ack delivery");
                send_ack
                    .stopped()
                    .await
                    .expect("Failed to wait for ACK request");

                tracing::info!("receiver ack delivered");

                tracing::info!("transfer completed successfully");

                Ok(())
            }
            .await;

            match result {
                Ok(()) => {
                    tracing::info!("ticket receiver completed");
                    Ok(())
                }
                Err(err) => {
                    tracing::error!(?err, "ticket receiver failed");

                    Err(err)
                }
            }
        }
        .instrument(span)
        .await
    }
}

pub fn ensure_dir(path: impl AsRef<Path>) -> std::io::Result<PathBuf> {
    let path = path.as_ref();

    if path.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty path",
        ));
    }

    // Creates it if missing; succeeds if it already exists as a directory.
    std::fs::create_dir_all(&path)?;

    Ok(path.to_path_buf())
}

#[derive(Debug, Clone)]
struct Node {
    store: Store,
    router: Router,
}

impl Node {
    pub async fn new() -> eyre::Result<Self> {
        let endpoint = get_endpoint_builder()?.bind().await?;
        let tempdir = tempfile::tempdir()?.keep();
        let store = FsStore::load(tempdir).await?;

        let blobs_protocol = BlobsProtocol::new(&store, None);
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs_protocol)
            .spawn();

        Ok(Self {
            store: store.into(),
            router,
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        self.router.endpoint()
    }

    // get address of this node. Has the side effect of waiting for the node
    // to be online & ready to accept connections
    async fn addr(&self) -> eyre::Result<EndpointAddr> {
        self.router.endpoint().online().await;
        let addr = self.router.endpoint().addr();
        Ok(addr)
    }

    #[allow(dead_code)]
    async fn list_hashes(&self) -> eyre::Result<Vec<Hash>> {
        self.store
            .blobs()
            .list()
            .hashes()
            .await
            .context("Failed to list hashes")
    }

    async fn create_collection<I, P>(&self, root: P, paths: I) -> eyre::Result<TempTag>
    where
        I: Iterator<Item = P>,
        P: Into<PathBuf>,
    {
        let root = root.into();

        let path_and_hash_tasks = paths.map(|x| async {
            let path = x.into();

            tracing::trace!(?path, "Tagging path");
            let tag = self
                .store
                .add_path_with_opts(AddPathOptions {
                    path: path.canonicalize()?,
                    mode: ImportMode::TryReference,
                    format: BlobFormat::Raw,
                })
                .await?;

            let path = dunce::canonicalize(path)?;
            let path = path.strip_prefix(&root)?;
            let pathstr = path.to_string_lossy().to_string();

            eyre::Ok((pathstr, tag.hash))
        });

        let path_and_hash = futures::future::try_join_all(path_and_hash_tasks).await?;

        let collection = Collection::from_iter(path_and_hash);

        let temptag = collection.store(&self.store).await?;
        self.store.tags().create(temptag.hash_and_format()).await?;

        Ok(temptag)
    }

    #[allow(dead_code)]
    pub async fn get_collection(&self, hash: Hash, source_addr: EndpointAddr) -> eyre::Result<()> {
        let req = HashAndFormat::hash_seq(hash);
        self.store
            .downloader(self.router.endpoint())
            .download(req, Some(source_addr.id))
            .await?;

        Ok(())
    }
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
                let span = tracing::info_span!(
                    "handler.send",
                    broker_id = %broker_id,
                    path = ?path,
                );

                async move {
                    tracing::info!("starting send handler");

                    let node = Node::new().await?;
                    tracing::debug!("node created");

                    let broker_key = broker::broker_public_key(broker_id);
                    let recv_code = recv_code.split_whitespace().collect::<Vec<_>>().join("");

                    tracing::info!("looking up receiver via broker");
                    let receiver_key =
                        broker::broker_lookup(node.endpoint(), broker_key, &recv_code).await?;
                    tracing::info!(?receiver_key, "found receiver");

                    tracing::debug!(?path, "building collection");

                    let root = dunce::canonicalize(path)?;

                    let files = walkdir::WalkDir::new(path)
                        .into_iter()
                        .filter_map(Result::ok)
                        .filter(|x| !x.file_type().is_dir())
                        .map(walkdir::DirEntry::into_path);

                    let root_tag = node.create_collection(root, files).await?;

                    tracing::info!(
                        hash = %root_tag.hash(),
                        format = ?root_tag.format(),
                        "collection built"
                    );

                    // Send collection root hash to receiver.
                    let ticket =
                        BlobTicket::new(node.addr().await?, root_tag.hash(), root_tag.format());

                    tracing::debug!(
                        ticket_addr = ?ticket.addr(),
                        ticket_hash = %ticket.hash(),
                        ticket_format = ?ticket.format(),
                        "built blob ticket"
                    );

                    let addr = EndpointAddr {
                        id: receiver_key,
                        addrs: BTreeSet::new(),
                    };

                    tracing::info!(?addr, "connecting to receiver");

                    let conn = node
                        .endpoint()
                        .connect(addr, ALPN)
                        .await
                        .wrap_err("Failed to connect to iroh endpoint")?;

                    tracing::debug!("connection established");

                    let (mut send, mut recv_ack) = conn.open_bi().await?;
                    tracing::debug!("opened bidi stream to receiver");

                    tracing::info!("sending ticket payload");
                    tokio::io::AsyncWriteExt::write_all(&mut send, ticket.to_string().as_bytes())
                        .await?;
                    send.finish()?;
                    tracing::debug!("ticket sent and stream finished");

                    tracing::info!("sending ticket sent; waiting for receiver ack");
                    let mut ack = [0u8; 4];

                    // TODO: Either increase, or do something else
                    // TODO: Add progress bar?
                    // TODO: Make FsStore fixed

                    let ack_result = tokio::time::timeout(
                        Duration::from_mins(5),
                        tokio::io::AsyncReadExt::read_exact(&mut recv_ack, &mut ack),
                    )
                    .await;

                    match ack_result {
                        Ok(Ok(n)) => {
                            tracing::info!(
                                ack_len = n,
                                ack = ?String::from_utf8_lossy(&ack),
                                "received receiver ack"
                            );
                        }
                        Ok(Err(err)) => {
                            tracing::error!(?err, "failed while reading receiver ack");
                            return Err(err.into());
                        }
                        Err(_) => {
                            tracing::error!("timed out waiting for receiver ack");
                            return Err(eyre::eyre!("receiver did not finish in time"));
                        }
                    }

                    tracing::info!("shutting down router");
                    node.router.shutdown().await?;
                    tracing::debug!("router shutdown complete");

                    tracing::debug!("closing connection");
                    conn.close(0u32.into(), b"bye");

                    tracing::info!("send handler done");

                    Ok::<_, eyre::Error>(())
                }
                .instrument(span)
                .await?;
            }
            Handler::Receive(broker_id, on_recv, filedir) => {
                let node = Node::new().await?;
                let endpoint = node.endpoint().clone();

                let id = endpoint.id();
                let fingerprint = get_device_code();
                tracing::info!(?id, "App ID: {fingerprint}");
                let key = endpoint.id();

                let broker_key = broker::broker_public_key(&broker_id);
                broker::broker_register(&endpoint, broker_key, &fingerprint, key).await?;

                let fingerprint = {
                    use digit_group::FormatGroup;
                    fingerprint
                        .parse::<usize>()?
                        .format_custom('.', ' ', 3, 3, false)
                };

                println!("Your code (give this to sender): {fingerprint}");
                tracing::info!("Registered with broker. Waiting for sender...");

                let handler = TicketReceiver {
                    node,
                    filedir: filedir.clone(),
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
    let endpoint_builder = Endpoint::builder(presets::Minimal)
        .relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::try_from_iter(
            vec!["https://aps1-1.relay.n0.iroh-canary.iroh.link./"],
        )?))
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
        init::AppSubcommand::Send { key, file } => Handler::Send(broker_id, key, file),
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
