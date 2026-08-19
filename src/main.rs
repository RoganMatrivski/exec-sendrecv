use std::{io::Stderr, sync::LazyLock};

use color_eyre::Report;
use eyre::Context;
use init::ProgressBarLogWriter;

mod broker;
mod dir_getter;
mod init;

pub static MPB: LazyLock<ProgressBarLogWriter<Stderr>> =
    LazyLock::new(|| ProgressBarLogWriter::default());

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;

    if MPB.is_hidden() {
        tracing::warn!("Warning! Progress bar is hidden.");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    match args.command {
        init::AppSubcommand::Send { recv_code, root } => {
            handle_send(recv_code, root).await?;
        }
        init::AppSubcommand::Receive { root, install } => {
            handle_recv(root, install).await?;
        }
    }

    Ok(())
}

use iroh::endpoint::{Endpoint, presets};

pub async fn handle_send(
    recv_code: impl std::fmt::Display,
    root: impl AsRef<std::path::Path>,
) -> eyre::Result<()> {
    let ticket = broker::get(recv_code).await?;
    let root = root.as_ref().to_path_buf();

    let endpoint = Endpoint::bind(presets::N0).await?;
    let conn = endpoint.connect(ticket, patchsync::ALPN).await?;

    let (send, recv) = conn.open_bi().await?;

    let (ev_tx, ev_rx) = flume::unbounded();

    let handler = patchsync::SendHandler::new(send, recv, root).await?;

    let pb = MPB.add(indicatif::ProgressBar::new_spinner());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    let _send_evloop = tokio::spawn(async move {
        for x in ev_rx {
            match x {
                patchsync::sync::SendEvent::DiffComputed { total_bytes, .. } => {
                    pb.disable_steady_tick();
                    pb.set_style(indicatif::ProgressStyle::with_template(
                            "{spinner:.green} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {eta}"
                        ).expect("Failed to set PB style")
                        .progress_chars("=>-"));
                    pb.set_length(total_bytes);
                    pb.tick();
                }
                patchsync::sync::SendEvent::Progress { bytes } => {
                    pb.inc(bytes as u64);
                }
                patchsync::sync::SendEvent::Finished => {
                    pb.finish_and_clear();
                }
                ev => pb.println(format!("SEND: {ev:?}")),
            }
        }
    });

    let res = handler.send_loop(ev_tx).await;
    let _ = endpoint.close().await;
    res?;

    Ok(())
}

pub async fn handle_recv(root: impl AsRef<std::path::Path>, install: bool) -> eyre::Result<()> {
    let cfg_path = dir_getter::get_data_dir(install)?;

    let keyfile = cfg_path.map(|x| x.join("app.key"));

    let key = match keyfile {
        Some(path) if !path.exists() => {
            tracing::debug!("Key not exist. Generating");
            let keybytes = iroh::SecretKey::generate();
            if let Err(e) = std::fs::write(path, keybytes.to_bytes()) {
                tracing::warn!("Failed to persist key to disk: {e}");
            }

            keybytes
        }
        Some(path) => {
            tracing::debug!("Key exist. Loading");
            let keybytes = std::fs::read(path).wrap_err("Failed to read key from disk")?;
            iroh::SecretKey::from_bytes(
                &keybytes
                    .try_into()
                    .map_err(|_| eyre::eyre!("Failed to convert key on disk to secret key"))?,
            )
        }
        None => {
            tracing::warn!("Failed to find config dir. Generating key to memory");
            iroh::SecretKey::generate()
        }
    };

    let endpoint = Endpoint::builder(presets::N0)
        .secret_key(key)
        .bind()
        .await?;
    let ticket = iroh_tickets::endpoint::EndpointTicket::new(endpoint.addr());

    let code = broker::set(ticket).await?;
    println!("Receiver code: {code}");

    let (ev_tx, ev_rx) = flume::unbounded();
    let protocol_handler = patchsync::RecvProtocol::new(root.as_ref().to_path_buf(), ev_tx);
    let router = iroh::protocol::Router::builder(endpoint)
        .accept(patchsync::ALPN, protocol_handler)
        .spawn();

    let _recv_evloop = tokio::spawn(async move {
        for _e in ev_rx {
            // no-op. idk what to display on recv.
        }
    });

    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;

    Ok(())
}
