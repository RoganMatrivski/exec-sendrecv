use std::path::PathBuf;

use color_eyre::eyre::{self, Context};
use iroh::protocol::Router;
use iroh::{endpoint::presets, Endpoint, EndpointAddr};
use iroh_blobs::api::remote::GetProgressItem;
use iroh_blobs::get::request::get_hash_seq_and_sizes;
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

    pub async fn create_collection<I, P>(&self, root: P, paths: I) -> eyre::Result<(TempTag, u64)>
    where
        I: Iterator<Item = P>,
        P: Into<PathBuf>,
    {
        let root = root.into();

        let path_and_hash_tasks = paths.map(|x| {
            let root = root.clone();
            let store = self.store.clone();
            async move {
                let rel_path = x.into();
                let full_path = root.join(&rel_path);
                tracing::trace!(?full_path, "Tagging path");

                let size = full_path.metadata()?.len();

                let tag = store
                    .add_path_with_opts(AddPathOptions {
                        path: full_path.canonicalize()?,
                        mode: ImportMode::Copy,
                        format: BlobFormat::Raw,
                    })
                    .await?;

                eyre::Ok(((rel_path.to_string_lossy().to_string(), tag.hash), size))
            }
        });

        let (path_and_hash, file_sizes): (Vec<(String, Hash)>, Vec<u64>) =
            futures::future::try_join_all(path_and_hash_tasks)
                .await?
                .into_iter()
                .unzip();

        let collection = Collection::from_iter(path_and_hash);
        let temptag = collection.store(&self.store).await?;
        self.store.tags().create(temptag.hash_and_format()).await?;

        let total_size: u64 = file_sizes.into_iter().sum();

        Ok((temptag, total_size))
    }

    pub async fn get_collection(
        &self,
        hash: Hash,
        source_addr: EndpointAddr,
        mut on_progress: impl FnMut(u64),
    ) -> eyre::Result<()> {
        use futures_util::StreamExt;

        let hashseq = HashAndFormat::hash_seq(hash);

        let local = self.store.remote().local(hashseq).await?;

        if local.is_complete() {
            return Ok(());
        }

        let conn = self
            .router
            .endpoint()
            .connect(source_addr, iroh_blobs::ALPN)
            .await?;

        let (_hash_seq, sizes) =
            get_hash_seq_and_sizes(&conn, &hash, 1024 * 1024 * 32, None).await?;

        let total_size = sizes.iter().copied().sum::<u64>();
        let payload_size = sizes.iter().skip(2).copied().sum::<u64>();
        let total_files = (sizes.len().saturating_sub(1)) as u64;

        tracing::info!(total_size, total_files, payload_size, "getting collection");

        let get = self.store.remote().execute_get(conn, local.missing());
        let mut progress = get.stream();

        while let Some(item) = progress.next().await {
            tracing::trace!(?item);

            match item {
                GetProgressItem::Progress(n) => on_progress(n),
                GetProgressItem::Error(err) => {
                    return Err(err.into());
                }
                GetProgressItem::Done(stats) => {
                    tracing::debug!(?stats, "Done downloading file")
                }
            }
        }

        Ok(())
    }
}

pub fn get_endpoint_builder() -> color_eyre::eyre::Result<iroh::endpoint::Builder> {
    let transport_config = iroh::endpoint::QuicTransportConfig::builder()
        .initial_mtu(1200)
        .min_mtu(1200)
        .build();

    let endpoint_builder = Endpoint::builder(presets::Minimal)
        .transport_config(transport_config)
        .relay_mode(iroh::RelayMode::Custom(iroh::RelayMap::try_from_iter(
            vec![
                "https://relay.rgmtrv.my.id/",
                "https://aps1-1.relay.n0.iroh-canary.iroh.link/",
            ],
        )?))
        .addr_filter(iroh::endpoint_info::AddrFilter::unfiltered())
        .address_lookup(iroh::address_lookup::PkarrPublisher::n0_dns())
        .address_lookup(iroh::address_lookup::DnsAddressLookup::n0_dns())
        .address_lookup(iroh::address_lookup::mdns::MdnsAddressLookup::builder());

    Ok(endpoint_builder)
}
