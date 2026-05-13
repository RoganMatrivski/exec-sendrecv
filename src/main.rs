use std::sync::LazyLock;

use color_eyre::Report;
use init::ProgressBarLogWriter;
use std::io::Stderr;

mod broker;
mod codec;
mod handler;
mod init;
mod node;
mod receive;
mod send;
mod snapshot;
mod util;

pub static MPB: LazyLock<ProgressBarLogWriter<Stderr>> =
    LazyLock::new(|| ProgressBarLogWriter::default());

pub const ALPN: &[u8] = b"i/dont/like/this/rock/robert";
pub const BROKER_ALPN: &[u8] = b"i/dont/like/this/rock/robert/broker";

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

enum ExecState {
    Receive(std::path::PathBuf),
    Export,
}

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;
    let broker_id = args.broker_id;

    if MPB.is_hidden() {
        tracing::warn!("Warning! Progress bar is hidden.");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<ExecState>(1);

    let exec_runner = tokio::task::spawn_blocking(move || {
        use std::process::{Child, Command};
        let mut child_handle: Option<Child> = None;

        while let Some(state) = rx.blocking_recv() {
            match state {
                ExecState::Receive(path) => {
                    let workdir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
                    tracing::trace!(?workdir, "Spawning process");

                    match Command::new(&path).current_dir(workdir).spawn() {
                        Ok(child) => {
                            println!("Spawned: {:?} (pid {})", path, child.id());
                            child_handle = Some(child);
                        }
                        Err(e) => eprintln!("Failed to spawn {:?}: {e}", path),
                    };
                }
                ExecState::Export => {
                    if let Some(mut child) = child_handle.take() {
                        kill_tree(&mut child);
                    }
                }
            }
        }

        if let Some(mut child) = child_handle.take() {
            kill_tree(&mut child);
        }
    });

    let tx = std::sync::Arc::new(tx);
    let tx_for_export = tx.clone(); // clone BEFORE the move
    let tx_for_receive = tx.clone(); // clone BEFORE the move

    match args.command {
        init::AppSubcommand::Send { key, file } => handler::Handler::Send(broker_id, key, file),
        init::AppSubcommand::Receive { filedir } => handler::Handler::Receive(
            broker_id,
            Some(std::sync::Arc::new(move || {
                if let Err(e) = tx_for_export.try_send(ExecState::Export) {
                    tracing::warn!(?e, "Failed to send export event to exec_runner");
                }
            })),
            Some(std::sync::Arc::new(move |p| {
                if let Err(e) = tx_for_receive.try_send(ExecState::Receive(p)) {
                    tracing::warn!(?e, "Failed to send receive event to exec_runner");
                }
            })),
            filedir,
            false, // TODO: Add clap opts for this
        ),
        init::AppSubcommand::Broker => handler::Handler::Broker(broker_id),
    }
    .run()
    .await?;

    drop(tx);

    exec_runner.await?;

    Ok(())
}

fn kill_tree(child: &mut std::process::Child) {
    use sysinfo::{Pid, System};
    use tracing::{error, warn};

    let sys = System::new_all();
    let pid = Pid::from_u32(child.id());

    let Some(process) = sys.process(pid) else {
        warn!("process {} not found", pid);
        return;
    };

    match process.kill_and_wait() {
        Ok(Some(status)) => {
            tracing::debug!(
                "process {} killed successfully with status {:?}",
                pid,
                status
            );
        }
        Ok(None) => {
            warn!("process {} killed but no exit status available", pid);
        }
        Err(err) => {
            error!("failed to kill process {}: {:?}", pid, err);
        }
    }
}
