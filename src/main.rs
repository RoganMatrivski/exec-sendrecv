use std::str::FromStr;

use color_eyre::Report;

mod init;

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tracing::instrument]
#[tokio::main]
async fn main() -> Result<(), Report> {
    let args = init::initialize()?;
    let broker_id = args.broker_id;

    let (tx, mut rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(1);

    // Spawn a dedicated thread for process management (blocking ops)
    let exec_runner = tokio::task::spawn_blocking(move || {
        use std::process::{Child, Command};
        let mut child_handle: Option<Child> = None;

        while let Some(path) = rx.blocking_recv() {
            // Kill old process + its entire tree
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
        init::AppSubcommand::Send { key, file } => {
            exec_sendrecv::Handler::Send(broker_id, key, file)
        }
        init::AppSubcommand::Receive { filedir } => exec_sendrecv::Handler::Receive(
            broker_id,
            Some(std::sync::Arc::new(move |p| {
                if let Err(e) = tx.try_send(p) {
                    tracing::warn!(?e, "Failed to send to exec_runner");
                }
            })),
            filedir,
        ),
        init::AppSubcommand::Broker => exec_sendrecv::Handler::Broker(broker_id),
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
        // /F = force, /T = include all child processes
        let _ = std::process::Command::new("taskkill")
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .output();
    }

    #[cfg(not(windows))]
    {
        // On Unix, kill the process group instead
        unsafe {
            libc::killpg(pid as i32, libc::SIGKILL);
        }
        // fallback
        let _ = child.kill();
    }

    let _ = child.wait();
}
