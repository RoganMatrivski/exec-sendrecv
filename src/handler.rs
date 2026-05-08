use std::{path::PathBuf, sync::Arc};

use color_eyre::eyre;
use iroh::protocol::Router;

use crate::{
    broker,
    node::{get_endpoint_builder, Node},
    receive::TicketReceiver,
    util::get_device_code,
    ALPN, BROKER_ALPN,
};

pub enum Handler {
    Send(String, String, PathBuf),
    Receive(
        String,
        Option<Arc<dyn Fn(PathBuf) + Send + Sync>>,
        Option<PathBuf>,
    ),
    Broker(String),
}

impl Handler {
    pub async fn run(&self) -> eyre::Result<()> {
        match self {
            Handler::Send(broker_id, recv_code, path) => {
                crate::send::run(broker_id, recv_code, path).await?;
            }

            Handler::Receive(broker_id, on_recv, filedir) => {
                let node = Node::new().await?;
                let endpoint = node.endpoint().clone();

                let fingerprint = get_device_code();
                tracing::info!(id = ?endpoint.id(), "App ID: {fingerprint}");

                let broker_key = broker::broker_public_key(broker_id);
                broker::broker_register(&endpoint, broker_key, &fingerprint, endpoint.id())
                    .await?;

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
                let secret_key = broker::derive_secret_key(client_id);
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
