use std::{
    collections::BTreeSet,
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};

use color_eyre::eyre::{Context, ContextCompat};
use iroh::{
    endpoint::presets,
    protocol::{ProtocolHandler, Router},
    Endpoint, EndpointAddr, PublicKey,
};
use iroh_blobs::{store::mem::MemStore, ticket::BlobTicket, BlobsProtocol};
use redis::TypedCommands;
use sha2::Digest;

const ALPN: &[u8] = b"i/dont/like/this/rock/robert";

#[derive(Debug, Clone)]
struct TicketReceiver {
    store: MemStore,
    endpoint: Endpoint,
    filedir: PathBuf,
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

        let mut recv = conn.accept_uni().await?;

        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;

        let payload = String::from_utf8(buf).expect("Failed to parse payload");

        tracing::debug!(payload, "RECV Payload");

        let payload: Payload<BlobTicket, std::borrow::Cow<'_, str>> =
            serde_json::from_str(&payload).expect("Failed parsing payload");
        let ticket: BlobTicket = payload.blob;

        let dest = PathBuf::from_str(&payload.filename).expect("Failed parsing filename as path");

        let dest_dir = ensure_dir(&self.filedir).expect("Failed using path as dir");
        let dest = dest_dir.join(&dest);
        let dest = std::path::absolute(dest)?;

        let dl = store.downloader(&endpoint);

        dl.download(ticket.hash(), Some(ticket.addr().id)).await?;

        tracing::info!(?dest, "Done downloading. Copying...");

        store
            .blobs()
            .export(ticket.hash(), dest)
            .await
            .expect("Failed copying from memory to local");

        Ok(())
    }
}

use std::{fs, io};

pub fn ensure_dir(path: impl AsRef<Path>) -> io::Result<PathBuf> {
    let path = path.as_ref();

    if path.as_os_str().is_empty() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "empty path"));
    }

    // Creates it if missing; succeeds if it already exists as a directory.
    fs::create_dir_all(&path)?;

    Ok(path.to_path_buf())
}

pub enum Handler {
    Send(PublicKey, PathBuf),
    Receive(PathBuf),
}

impl Handler {
    pub async fn run(&self, redis_connstr: impl AsRef<str>) -> color_eyre::eyre::Result<()> {
        tracing::trace!(conn_str = redis_connstr.as_ref(), "Connecting to redis...");
        let mut code_mgr = RedisGetterSetter::new(redis_connstr.as_ref())?;
        tracing::trace!("Connected to redis");

        match self {
            Handler::Send(id, path) => {
                let dht = iroh::address_lookup::dht::DhtAddressLookup::builder();
                let mdns = iroh::address_lookup::mdns::MdnsAddressLookup::builder();
                let endpoint = Endpoint::builder(presets::N0)
                    .address_lookup(mdns)
                    .address_lookup(dht)
                    .bind()
                    .await?;
                let store = MemStore::new();

                let blobs = BlobsProtocol::new(&store, None);

                tracing::debug!(?path, "Hashing file");
                let tag = store.blobs().add_path(&path).await?;

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
                    id: id.clone(),         // your PublicKey
                    addrs: BTreeSet::new(), // empty set -> discovery will be used
                };

                let conn = endpoint
                    .connect(addr, ALPN)
                    .await
                    .wrap_err("Failed to connect to iroh endpoint")?;

                let mut send = conn.open_uni().await?;

                tokio::io::AsyncWriteExt::write_all(
                    &mut send,
                    serde_json::to_string(&payload)?.as_bytes(),
                )
                .await?;
                send.finish()?;

                let router = Router::builder(endpoint)
                    .accept(iroh_blobs::ALPN, blobs)
                    .spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
            Handler::Receive(filedir) => {
                let dht = iroh::address_lookup::dht::DhtAddressLookup::builder();
                let mdns = iroh::address_lookup::mdns::MdnsAddressLookup::builder();
                let endpoint = Endpoint::builder(presets::N0)
                    .address_lookup(mdns)
                    .address_lookup(dht)
                    .bind()
                    .await?;
                let store = MemStore::new();

                let id = endpoint.id();
                let fingerprint = get_device_code();
                code_mgr.set(&fingerprint, &id.to_string());
                tracing::info!(?id, "App ID: {fingerprint}");

                let handler = TicketReceiver {
                    store,
                    filedir: filedir.clone(),
                    endpoint: endpoint.clone(),
                };

                let router = Router::builder(endpoint).accept(ALPN, handler).spawn();

                tokio::signal::ctrl_c().await?;
                router.shutdown().await?;
            }
        }

        Ok(())
    }
}

trait KeyGetterSetter<C: AsRef<str>, S: Display> {
    fn get(&mut self, code: C) -> String;
    fn set(&mut self, code: C, id: S);
}

trait KeyGetterSetterAsync<C: AsRef<str>, S: Display> {
    async fn get(&mut self, code: C) -> String;
    async fn set(&mut self, code: C, id: S);
}

struct RedisGetterSetter {
    conn: redis::Connection,
}

impl RedisGetterSetter {
    pub fn new(connstr: impl redis::IntoConnectionInfo) -> color_eyre::eyre::Result<Self> {
        let c = redis::Client::open(connstr)?;
        tracing::trace!("REDIS: Opened connection");
        let conn = c.get_connection()?;
        tracing::trace!("REDIS: Connection GET");

        Ok(Self { conn })
    }
}

impl<C: AsRef<str>, S: Display + Send + Sync + redis::ToSingleRedisArg> KeyGetterSetter<C, S>
    for RedisGetterSetter
{
    fn get(&mut self, code: C) -> String {
        self.conn
            .get(code.as_ref())
            .expect("Failed getting code")
            .expect("Code doesn't exist")
    }

    fn set(&mut self, code: C, id: S) {
        self.conn
            .set(code.as_ref(), id)
            .expect("Failed setting code")
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
