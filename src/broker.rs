use std::sync::Arc;

use color_eyre::eyre::Context;
use dashmap::DashMap;
use iroh::{
    endpoint::Connection, protocol::ProtocolHandler, Endpoint, EndpointAddr, PublicKey, SecretKey,
};
use iroh_tickets::endpoint::EndpointTicket;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum BrokerRequest {
    // Receiver sends this: "I am reachable at this ticket, my short code is X"
    Register { code: String, ticket: String },
    // Sender sends this: "Give me the ticket for short code X"
    Lookup { code: String },
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum BrokerResponse {
    Found { ticket: String },
    NotFound,
    Ok,
}

#[derive(Debug, Default)]
pub struct BrokerHandler {
    // Shared across all connections: short_code -> ticket string
    registry: Arc<DashMap<String, String>>,
}

impl ProtocolHandler for BrokerHandler {
    #[tracing::instrument(skip(self, conn), err)]
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let registry = self.registry.clone();

        // Bidi stream: peer writes request, broker writes response
        let (mut send, mut recv) = conn.accept_bi().await?;
        tracing::debug!("Accepted bidi stream from peer");

        // Read until peer closes its send side
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
        tracing::debug!(len = buf.len(), "Read request from peer");

        let request: BrokerRequest = serde_json::from_slice(&buf).map_err(|e| {
            tracing::error!(error = %e, "Failed to parse request");
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let response = match &request {
            BrokerRequest::Register { code, ticket } => {
                tracing::info!(code, ticket, "Registering peer");
                registry.insert(code.clone(), ticket.clone());
                BrokerResponse::Ok
            }
            BrokerRequest::Lookup { code } => {
                tracing::info!(code, "Looking up peer");
                match registry.get(code) {
                    Some(ticket) => {
                        tracing::debug!(code, ticket = %ticket.value(), "Found peer");
                        BrokerResponse::Found {
                            ticket: ticket.clone(),
                        }
                    }
                    None => {
                        tracing::debug!(code, "Peer not found");
                        BrokerResponse::NotFound
                    }
                }
            }
        };

        let resp_bytes = serde_json::to_vec(&response).map_err(|e| {
            tracing::error!(error = %e, "Failed to serialize broker response");
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })?;
        tracing::debug!(len = resp_bytes.len(), "Sending response to peer");

        tokio::io::AsyncWriteExt::write_all(&mut send, &resp_bytes).await?;

        // Close our send side so the peer's read_to_end returns
        send.finish()?;
        tracing::debug!("Closed send stream to peer");

        conn.closed().await;
        tracing::debug!("Connection closed");

        Ok(())
    }
}

// --- Key derivation ---
// Same token always produces the same SecretKey -> same PublicKey.
// Both broker and peers call this with the same client_id to agree on
// the broker's identity without hardcoding anything.
pub fn derive_secret_key(token: &str) -> SecretKey {
    use sha2::Digest;
    let hash = sha2::Sha256::digest(token.as_bytes());
    let bytes: [u8; 32] = hash.into();
    SecretKey::from_bytes(&bytes)
}

// Both sender and receiver call this to get the broker's PublicKey.
// Same client_id always produces the same key — no coordination neededpub .
pub fn broker_public_key(client_id: &str) -> PublicKey {
    derive_secret_key(client_id).public()
}

pub fn resolve_broker_addr(id: &str) -> EndpointAddr {
    use std::str::FromStr;
    if let Ok(ticket) = EndpointTicket::from_str(id) {
        tracing::info!("Broker ticket get!");
        ticket.into()
    } else {
        tracing::info!("Can't parse broker ticket. Assuming it's a public key...");
        EndpointAddr::from(broker_public_key(id))
    }
}

/// Client-side handle for talking to a broker node.
///
/// Construct with [`BrokerClient::new`], then call [`register`](BrokerClient::register)
/// or [`lookup`](BrokerClient::lookup) as needed.
#[derive(Debug, Clone)]
pub struct BrokerClient {
    endpoint: Endpoint,
    broker_addr: EndpointAddr,
    cachedb: Arc<Option<redb::Database>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CacheValue<T> {
    exp: std::time::SystemTime,
    dat: T,
}

impl<T> CacheValue<T> {
    pub fn new(dat: T) -> Self {
        Self {
            dat,
            exp: std::time::SystemTime::now(),
        }
    }

    pub fn with_exp(self, exp_dur: std::time::Duration) -> Self {
        Self {
            exp: self.exp + exp_dur,
            ..self
        }
    }

    pub fn get_expiring_value(self) -> Option<T> {
        if std::time::SystemTime::now() < self.exp {
            Some(self.dat)
        } else {
            None
        }
    }
}

impl redb::Value for CacheValue<String> {
    type SelfType<'a> = Self;
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).expect("failed to deserialize CacheValue")
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Vec<u8> {
        postcard::to_allocvec(value).expect("failed to serialize CacheValue")
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("CacheValue<String>")
    }
}

const TABLE: redb::TableDefinition<&str, CacheValue<String>> =
    redb::TableDefinition::new("receiver_lookup");

impl BrokerClient {
    pub fn new(endpoint: Endpoint, broker_addr: EndpointAddr) -> Self {
        let tempdir =
            directories::ProjectDirs::from("com.github", "roganmatrivski", "exec-sendrecv")
                .map(|p| p.cache_dir().to_path_buf())
                .or_else(|| directories::BaseDirs::new().map(|b| b.cache_dir().to_path_buf()))
                .unwrap_or_else(std::env::temp_dir);

        let cachedb_res = || -> eyre::Result<_> {
            let db = redb::Database::create(tempdir.join("cache.db"))?;

            let w = db.begin_write()?;
            let _ = w.open_table(TABLE)?;
            w.commit()?;

            Ok(Some(db))
        }();

        let cachedb = match cachedb_res {
            Ok(db) => db,
            Err(e) => {
                tracing::warn!(%e, "Failed to init cache db");
                None
            }
        };

        let cachedb = Arc::new(cachedb);

        Self {
            endpoint,
            broker_addr,
            cachedb,
        }
    }

    /// Receiver calls this to advertise "I am reachable at `own_ticket`, my short code is `code`".
    #[tracing::instrument(skip(self), err)]
    pub async fn register(
        &self,
        code: &str,
        own_ticket: EndpointTicket,
    ) -> color_eyre::eyre::Result<()> {
        let mut last_error = None;
        for i in 0..5 {
            if i > 0 {
                let delay = std::time::Duration::from_secs(2u64.pow(i as u32));
                tracing::info!(?delay, attempt = i + 1, "Retrying broker registration");
                tokio::time::sleep(delay).await;
            }

            let res: color_eyre::eyre::Result<()> = async {
                tracing::debug!("Connecting to broker");
                let conn = self
                    .endpoint
                    .connect(self.broker_addr.clone(), crate::BROKER_ALPN)
                    .await
                    .wrap_err("Failed to connect to broker")?;
                tracing::debug!("Connected to broker");

                tracing::debug!("Opening bidi stream");
                let (mut send, mut recv) = conn.open_bi().await?;

                let request = BrokerRequest::Register {
                    code: code.to_string(),
                    ticket: own_ticket.to_string(),
                };

                let bytes = serde_json::to_vec(&request)?;
                tracing::debug!(len = bytes.len(), "Sending register request");
                tokio::io::AsyncWriteExt::write_all(&mut send, &bytes).await?;

                // Close our send side so the broker's read_to_end returns
                send.finish()?;
                tracing::debug!("Closed send stream");

                // Wait for broker's acknowledgement
                let mut buf = Vec::new();
                tracing::debug!("Waiting for response");
                tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
                let response: BrokerResponse = serde_json::from_slice(&buf)?;
                tracing::debug!("Received response");

                match response {
                    BrokerResponse::Ok => {
                        tracing::info!(code, "Registered with broker");
                        Ok(())
                    }
                    _ => color_eyre::eyre::bail!("Unexpected broker response during register"),
                }
            }
            .await;

            match res {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(error = ?e, attempt = i + 1, "Broker registration failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            color_eyre::eyre::eyre!("Failed to register with broker after retries")
        }))
    }

    /// Sender calls this to ask the broker "who has code `code`?"
    #[tracing::instrument(skip(self), err)]
    pub async fn lookup(&self, code: &str) -> color_eyre::eyre::Result<EndpointTicket> {
        if let Some(db) = self.cachedb.as_ref() {
            let r = db.begin_read()?;
            let t = r.open_table(TABLE)?;

            if let Some(v) = t.get(code)?.and_then(|x| x.value().get_expiring_value()) {
                tracing::debug!(code, "Cache hit");
                use std::str::FromStr;
                let ticket = EndpointTicket::from_str(&v)?;
                return Ok(ticket);
            }
            tracing::debug!(code, "Cache miss");
        }

        let mut last_error = None;
        for i in 0..5 {
            if i > 0 {
                let delay = std::time::Duration::from_secs(2u64.pow(i as u32));
                tracing::info!(?delay, attempt = i + 1, "Retrying broker lookup");
                tokio::time::sleep(delay).await;
            }

            let res: color_eyre::eyre::Result<EndpointTicket> = async {
                tracing::debug!("Connecting to broker");
                let conn = self
                    .endpoint
                    .connect(self.broker_addr.clone(), crate::BROKER_ALPN)
                    .await
                    .context("Failed to connect to broker")?;
                tracing::debug!("Connected to broker");

                tracing::debug!("Opening bidi stream");
                let (mut send, mut recv) = conn.open_bi().await?;

                let request = BrokerRequest::Lookup {
                    code: code.to_string(),
                };

                let bytes = serde_json::to_vec(&request)?;
                tracing::debug!(len = bytes.len(), "Sending lookup request");
                tokio::io::AsyncWriteExt::write_all(&mut send, &bytes).await?;

                send.finish()?;
                tracing::debug!("Closed send stream");

                let mut buf = Vec::new();
                tracing::debug!("Waiting for response");
                tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
                let response: BrokerResponse = serde_json::from_slice(&buf)?;
                tracing::debug!("Received response");

                match response {
                    BrokerResponse::Found { ticket } => {
                        use std::str::FromStr;
                        let ticket = EndpointTicket::from_str(&ticket)
                            .context("Broker returned invalid Ticket")?;
                        tracing::info!(code, "Found peer");
                        Ok(ticket)
                    }
                    BrokerResponse::NotFound => {
                        tracing::info!(code, "Peer not found");
                        color_eyre::eyre::bail!("No peer registered with that code")
                    }
                    _ => color_eyre::eyre::bail!("Unexpected broker response during lookup"),
                }
            }
            .await;

            match res {
                Ok(ticket) => {
                    if let Some(db) = self.cachedb.as_ref() {
                        tracing::debug!(code, "Updating cache");
                        let w = db.begin_write()?;
                        {
                            let mut t = w.open_table(TABLE)?;
                            t.insert(
                                code,
                                CacheValue::new(ticket.to_string())
                                    .with_exp(std::time::Duration::from_hours(24)),
                            )?;
                        }
                        w.commit()?;
                    }

                    return Ok(ticket);
                }
                Err(e) => {
                    // If the error is "No peer registered with that code", don't retry.
                    if e.to_string().contains("No peer registered with that code") {
                        return Err(e);
                    }
                    tracing::warn!(error = ?e, attempt = i + 1, "Broker lookup failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            color_eyre::eyre::eyre!("Failed to lookup with broker after retries")
        }))
    }
}
