use std::sync::LazyLock;

use color_eyre::Report;
use init::ProgressBarLogWriter;
use std::io::Stderr;

mod broker;
mod handler;
mod init;
mod node;
mod receive;
mod send;
mod util;

pub static MPB: LazyLock<ProgressBarLogWriter<Stderr>> =
    LazyLock::new(|| ProgressBarLogWriter::default());

pub const ALPN: &[u8] = b"i/dont/like/this/rock/robert";
pub const BROKER_ALPN: &[u8] = b"i/dont/like/this/rock/robert/broker";

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;
    let broker_id = args.broker_id;

    if MPB.is_hidden() {
        tracing::warn!("Warning! Progress bar is hidden.");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(1);

    let exec_runner = tokio::task::spawn_blocking(move || {
        use std::process::{Child, Command};
        let mut child_handle: Option<Child> = None;

        while let Some(path) = rx.blocking_recv() {
            if let Some(mut child) = child_handle.take() {
                kill_tree(&mut child);
            }
            match Command::new(&path).spawn() {
                Ok(child) => {
                    println!("Spawned: {:?} (pid {})", path, child.id());
                    child_handle = Some(child);
                }
                Err(e) => eprintln!("Failed to spawn {:?}: {e}", path),
            }
        }

        if let Some(mut child) = child_handle.take() {
            kill_tree(&mut child);
        }
    });

    match args.command {
        init::AppSubcommand::Send { key, file } => handler::Handler::Send(broker_id, key, file),
        init::AppSubcommand::Receive { filedir } => handler::Handler::Receive(
            broker_id,
            Some(std::sync::Arc::new(move |p| {
                if let Err(e) = tx.try_send(p) {
                    tracing::warn!(?e, "Failed to send to exec_runner");
                }
            })),
            filedir,
        ),
        init::AppSubcommand::Broker => handler::Handler::Broker(broker_id),
    }
    .run()
    .await?;

    exec_runner.await?;

    Ok(())
}

fn kill_tree(child: &mut std::process::Child) {
    let pid = child.id();

    #[cfg(windows)]
    {
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .output();
    }

    #[cfg(not(windows))]
    {
        unsafe { libc::killpg(pid as i32, libc::SIGKILL) };
        let _ = child.kill();
    }

    let _ = child.wait();
}
