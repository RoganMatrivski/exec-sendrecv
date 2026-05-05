use std::sync::Arc;

use color_eyre::eyre::Context;
use dashmap::DashMap;
use iroh::{protocol::ProtocolHandler, Endpoint, PublicKey, SecretKey};

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum BrokerRequest {
    // Receiver sends this: "I am reachable at this PublicKey, my short code is X"
    Register { code: String, key: String },
    // Sender sends this: "Give me the PublicKey for short code X"
    Lookup { code: String },
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum BrokerResponse {
    Found { key: String },
    NotFound,
    Ok,
}

#[derive(Debug, Default)]
pub struct BrokerHandler {
    // Shared across all connections: short_code -> PublicKey string
    registry: Arc<DashMap<String, String>>,
}

impl ProtocolHandler for BrokerHandler {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let registry = self.registry.clone();

        // Bidi stream: peer writes request, broker writes response
        let (mut send, mut recv) = conn.accept_bi().await?;

        // Read until peer closes its send side
        let mut buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;

        let request: BrokerRequest = serde_json::from_slice(&buf).expect("Failed to parse request");

        let response = match request {
            BrokerRequest::Register { code, key } => {
                tracing::info!(code, key, "Registering peer");
                registry.insert(code, key);
                BrokerResponse::Ok
            }
            BrokerRequest::Lookup { code } => {
                tracing::info!(code, "Looking up peer");
                match registry.get(&code) {
                    Some(key) => BrokerResponse::Found { key: key.clone() },
                    None => BrokerResponse::NotFound,
                }
            }
        };

        tokio::io::AsyncWriteExt::write_all(
            &mut send,
            serde_json::to_string(&response)
                .expect("Failed to serialize broker response")
                .as_bytes(),
        )
        .await?;

        // Close our send side so the peer's read_to_end returns
        send.finish()?;

        conn.closed().await;

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

// Receiver calls this to tell the broker "I'm here, my code is X"
pub async fn broker_register(
    endpoint: &Endpoint,
    broker_key: PublicKey,
    code: &str,
    own_key: PublicKey,
) -> color_eyre::eyre::Result<()> {
    let conn = endpoint
        .connect(broker_key, crate::BROKER_ALPN)
        .await
        .wrap_err("Failed to connect to broker")?;

    let (mut send, mut recv) = conn.open_bi().await?;

    let request = BrokerRequest::Register {
        code: code.to_string(),
        key: own_key.to_string(),
    };

    tokio::io::AsyncWriteExt::write_all(&mut send, serde_json::to_string(&request)?.as_bytes())
        .await?;

    // Close our send side so the broker's read_to_end returns
    send.finish()?;

    // Wait for broker's acknowledgement
    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
    let response: BrokerResponse = serde_json::from_slice(&buf)?;

    match response {
        BrokerResponse::Ok => {
            tracing::info!(code, "Registered with broker");
            Ok(())
        }
        _ => color_eyre::eyre::bail!("Unexpected broker response during register"),
    }
}

// Sender calls this to ask the broker "who has code X?"
pub async fn broker_lookup(
    endpoint: &Endpoint,
    broker_key: PublicKey,
    code: &str,
) -> color_eyre::eyre::Result<PublicKey> {
    let conn = endpoint
        .connect(broker_key, crate::BROKER_ALPN)
        .await
        .context("Failed to connect to broker")?;

    let (mut send, mut recv) = conn.open_bi().await?;

    tracing::trace!("Finding receiver with code {code}");

    let request = BrokerRequest::Lookup {
        code: code.to_string(),
    };

    tokio::io::AsyncWriteExt::write_all(&mut send, serde_json::to_string(&request)?.as_bytes())
        .await?;

    send.finish()?;

    let mut buf = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf).await?;
    let response: BrokerResponse = serde_json::from_slice(&buf)?;

    match response {
        BrokerResponse::Found { key } => {
            let pk: PublicKey = key.parse().context("Broker returned invalid PublicKey")?;
            Ok(pk)
        }
        BrokerResponse::NotFound => color_eyre::eyre::bail!("No peer registered with that code"),
        _ => color_eyre::eyre::bail!("Unexpected broker response during lookup"),
    }
}
