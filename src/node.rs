use std::path::PathBuf;

use color_eyre::eyre::{self, Context};
use iroh::protocol::Router;
use iroh::{endpoint::presets, Endpoint, EndpointAddr};
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, ImportMode},
        Store, TempTag,
    },
    format::collection::Collection,
    store::fs::FsStore,
    BlobFormat, BlobsProtocol, Hash, HashAndFormat,
};

#[derive(Debug, Clone)]
pub struct Node {
    pub store: Store,
    pub router: Router,
}

impl Node {
    pub async fn new() -> eyre::Result<Self> {
        let endpoint = get_endpoint_builder()?.bind().await?;
        // let tempdir = tempfile::tempdir()?.keep();
        let tempdir =
            directories::ProjectDirs::from("com.github", "roganmatrivski", "exec-sendrecv")
                .map(|p| p.cache_dir().to_path_buf())
                .or_else(|| directories::BaseDirs::new().map(|b| b.cache_dir().to_path_buf()))
                .unwrap_or_else(std::env::temp_dir);

        let tempdir = tempdir.join("fs-store");

        std::fs::create_dir_all(&tempdir)
            .wrap_err_with(|| format!("Failed to create directory: {}", tempdir.display()))?;

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

    /// Returns the node's address, waiting until online first.
    pub async fn addr(&self) -> eyre::Result<EndpointAddr> {
        self.router.endpoint().online().await;
        Ok(self.router.endpoint().addr())
    }

    #[allow(dead_code)]
    pub async fn list_hashes(&self) -> eyre::Result<Vec<Hash>> {
        self.store
            .blobs()
            .list()
            .hashes()
            .await
            .context("Failed to list hashes")
    }

    pub async fn create_collection<I, P>(&self, root: P, paths: I) -> eyre::Result<TempTag>
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

    pub async fn get_collection(
        &self,
        hash: Hash,
        source_addr: EndpointAddr,
        mut on_progress: impl FnMut(u64),
    ) -> eyre::Result<()> {
        use futures_util::StreamExt;
        use iroh_blobs::api::downloader::DownloadProgressItem;

        let req = HashAndFormat::hash_seq(hash);

        let downloader = self.store.downloader(self.router.endpoint());
        let mut progress = downloader
            .download(req, Some(source_addr.id))
            .stream()
            .await?;

        while let Some(item) = progress.next().await {
            tracing::trace!(?item);

            match item {
                DownloadProgressItem::Progress(n) => on_progress(n),
                DownloadProgressItem::ProviderFailed { .. } => {
                    tracing::warn!("provider failed, trying next");
                }
                DownloadProgressItem::TryProvider { .. } => {}
                DownloadProgressItem::PartComplete { .. } => {}
                DownloadProgressItem::DownloadError => {
                    eyre::bail!("download error");
                }
                DownloadProgressItem::Error(err) => {
                    return Err(err.into());
                }
            }
        }

        Ok(())
    }
}

pub fn get_endpoint_builder() -> color_eyre::eyre::Result<iroh::endpoint::Builder> {
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
