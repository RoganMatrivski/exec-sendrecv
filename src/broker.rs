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

#[tracing::instrument(skip(endpoint), err)]
pub async fn broker_register(
    endpoint: &Endpoint,
    broker_addr: EndpointAddr,
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
            let conn = endpoint
                .connect(broker_addr.clone(), crate::BROKER_ALPN)
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

    Err(last_error
        .unwrap_or_else(|| color_eyre::eyre::eyre!("Failed to register with broker after retries")))
}

// Sender calls this to ask the broker "who has code X?"
#[tracing::instrument(skip(endpoint), err)]
pub async fn broker_lookup(
    endpoint: &Endpoint,
    broker_addr: EndpointAddr,
    code: &str,
) -> color_eyre::eyre::Result<EndpointTicket> {
    let mut last_error = None;
    for i in 0..5 {
        if i > 0 {
            let delay = std::time::Duration::from_secs(2u64.pow(i as u32));
            tracing::info!(?delay, attempt = i + 1, "Retrying broker lookup");
            tokio::time::sleep(delay).await;
        }

        let res: color_eyre::eyre::Result<EndpointTicket> = async {
            tracing::debug!("Connecting to broker");
            let conn = endpoint
                .connect(broker_addr.clone(), crate::BROKER_ALPN)
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
            Ok(ticket) => return Ok(ticket),
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

    Err(last_error
        .unwrap_or_else(|| color_eyre::eyre::eyre!("Failed to lookup with broker after retries")))
}
