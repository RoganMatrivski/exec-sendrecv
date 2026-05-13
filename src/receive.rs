use std::{fmt, path::PathBuf, sync::Arc};

use futures::{SinkExt, StreamExt};
use iroh::protocol::ProtocolHandler;
use iroh_blobs::{
    api::blobs::{ExportMode, ExportOptions},
    format::collection::Collection,
};
use tracing::Instrument;

use crate::{
    node::Node,
    util::{ensure_dir, find_executable_or_first},
};

#[derive(Default)]
pub struct TicketReceiverOptions {
    pub sync: bool,
}

pub struct TicketReceiver {
    pub node: Node,
    pub filedir: Option<PathBuf>,
    pub on_export: Option<Arc<dyn Fn() + Send + Sync>>,
    pub on_recv: Option<Arc<dyn Fn(PathBuf) + Send + Sync>>,

    pub opt: TicketReceiverOptions,
}

impl fmt::Debug for TicketReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TicketReceiver")
            .field("store", &self.node.store)
            .field("endpoint", &self.node.endpoint())
            .field("filedir", &self.filedir)
            .field("on_recv", &self.on_export.as_ref().map(|_| "<fn>"))
            .field("on_recv", &self.on_recv.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl ProtocolHandler for TicketReceiver {
    async fn accept(
        &self,
        conn: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        let conn_id = format!("{:?}", conn.remote_id());
        let span = tracing::info_span!("ticket_receiver.accept", %conn_id);

        async move {
            tracing::info!("accepting incoming ticket transfer");

            let store = self.node.store.clone();

            let result: Result<(), iroh::protocol::AcceptError> = async {
                tracing::info!(remote_id = ?conn.remote_id(), "Connection received from sender");
                tracing::debug!("waiting for bidi stream");
                let (send, recv) = conn.accept_bi().await?;
                tracing::info!("Bidi-stream accepted from sender");

                let dest_root = if let Some(d) = &self.filedir {
                    ensure_dir(d).map_err(|e| {
                        tracing::error!(error = ?e, path = %d.display(), "failed to ensure destination directory");
                        iroh::protocol::AcceptError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                    })?;
                    d.canonicalize()?
                } else {
                    tempfile::tempdir()
                        .map_err(|e| {
                            tracing::error!(error = %e, "failed to create temp output dir");
                            iroh::protocol::AcceptError::from(std::io::Error::new(std::io::ErrorKind::Other, e))
                        })?
                        .keep()
                };

                tracing::info!(path = %dest_root.display(), "created destination root");

                let (mut sink, mut stream) = crate::codec::peer_channel(send, recv);

                // Wait for the sender to be ready (this triggers the accept_bi)
                if let Some(msg) = stream.next().await {
                    match msg? {
                        crate::codec::PeerMessages::Ack => {
                             tracing::info!("Sender is ready; capturing snapshot");
                        }
                        _ => {
                            tracing::warn!("Received unexpected initial message from sender");
                        }
                    }
                }

                let snapshot = crate::snapshot::Snapshot::capture(&dest_root).expect("Failed to scan output dir for changes");
                sink.send(crate::codec::PeerMessages::DirSnapshot(snapshot)).await?;
                sink.flush().await?;

                while let Some(msg) = stream.next().await {
                    match msg? {
                        crate::codec::PeerMessages::PayloadInfo { total_size: total_bytes, ticket, delete_targets } => {
                            let pb = crate::MPB.add(indicatif::ProgressBar::new(total_bytes));
                            pb.set_style(
                                indicatif::ProgressStyle::with_template(
                                    "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec})",
                                )
                                .map_err(|e| {
                                    tracing::error!(error = %e, "failed to create progress bar style");
                                    iroh::protocol::AcceptError::from(std::io::Error::new(std::io::ErrorKind::Other, e))
                                })?,
                            );
                            pb.set_message("downloading");

                            self.node
                                .get_collection(ticket.hash(), ticket.addr().clone(), |bytes| {
                                    pb.set_position(bytes);
                                })
                                .await
                                .map_err(|e| {
                                    tracing::error!(error = ?e, "failed to download collection");
                                    iroh::protocol::AcceptError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                })?;

                            pb.finish_with_message("download complete");

                            let collection = Collection::load(ticket.hash(), &store).await.map_err(|e| {
                                tracing::error!(error = ?e, "failed to load collection from store");
                                iroh::protocol::AcceptError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                            })?;
                            tracing::info!(files = collection.len(), "loaded collection");

                            if let Some(f) = self.on_export.clone() {
                                tracing::debug!("invoking export callback");
                                let c_start = tokio::time::Instant::now();
                                f();
                                tracing::debug!(c_time = c_start.elapsed().as_millis(), "export callback completed");
                            }

                            for (name, hash) in collection.iter() {
                                let export_span =
                                    tracing::debug_span!("export_blob", file = %name, hash = %hash);

                                let res = async {
                                    let target = dest_root.join(name);
                                    tracing::debug!(target = %target.display(), "exporting blob");


                                    let export_fn = || async {
                                        store
                                            .export_with_opts(ExportOptions {
                                                hash: hash.clone(),
                                                target: target.clone(),
                                                mode: ExportMode::Copy,
                                            })
                                            .await
                                            .map_err(|e| {
                                                tracing::error!(error = ?e, "failed to export file from Store");
                                                iroh::protocol::AcceptError::from(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                                            })
                                    };


                                    use backon::ExponentialBuilder;
                                    use backon::Retryable;

                                    export_fn
                                        .retry(ExponentialBuilder::default())
                                        .sleep(backon::FuturesTimerSleeper::default())
                                        .await
                                        .expect("Failed to export blobs");


                                    tracing::info!(target = %target.display(), "export completed");
                                    Ok::<(), iroh::protocol::AcceptError>(())
                                }
                                .instrument(export_span)
                                .await;

                                if let Err(e) = res {
                                    sink.send(crate::codec::PeerMessages::ErrorMsg(e.to_string())).await?;
                                    sink.flush().await?;

                                    return Err(e);
                                }
                            }

                            if self.opt.sync {
                                for p in delete_targets {
                                    std::fs::remove_file(&p)?;
                                }
                            }

                            let base_path = dest_root.clone();

                            let recv_path = if base_path.is_dir() {
                                find_executable_or_first(&base_path).unwrap_or(base_path)
                            } else {
                                base_path
                            };

                            tracing::info!(recv_path = %recv_path.display(), "resolved receive path");

                            if let Some(f) = self.on_recv.clone() {
                                tracing::debug!("invoking receive callback");
                                f(recv_path);
                                tracing::debug!("receive callback completed");
                            }

                            tracing::info!("receiver finished; sending ack");
                            sink.send(crate::codec::PeerMessages::Ack).await?;
                            sink.flush().await?;
                            sink.close().await?;

                            tracing::info!("transfer completed successfully");
                            break;
                        },
                        _ => (),
                    }
                };

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
