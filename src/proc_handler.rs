// This file is written by Claude

use command_group::CommandGroup;
use std::path::{Path, PathBuf};
use tokio::task::JoinHandle;

enum ExecCmd {
    Spawn(PathBuf),
    Kill,
}

#[derive(Debug)]
pub struct ExecRunner {
    tx: flume::Sender<ExecCmd>,
    handle: JoinHandle<()>,
}

impl ExecRunner {
    pub fn spawn_task() -> Self {
        let (tx, rx) = flume::unbounded::<ExecCmd>();

        let handle = tokio::task::spawn_blocking(move || {
            let mut child_handle: Option<command_group::GroupChild> = None;

            while let Ok(cmd) = rx.recv() {
                match cmd {
                    ExecCmd::Spawn(path) => {
                        if child_handle.is_some() {
                            eprintln!("Spawn requested but a process is already running; ignoring");
                            continue;
                        }

                        let workdir = path.parent().unwrap_or_else(|| Path::new("."));
                        tracing::trace!(?workdir, "Spawning process");

                        match std::process::Command::new(&path)
                            .current_dir(workdir)
                            .group_spawn()
                        {
                            Ok(child) => {
                                println!("Spawned: {:?} (pid {})", path, child.id());
                                child_handle = Some(child);
                            }
                            Err(e) => eprintln!("Failed to spawn {:?}: {e}", path),
                        }
                    }
                    ExecCmd::Kill => {
                        if let Some(mut child) = child_handle.take() {
                            if let Err(e) = child.kill() {
                                eprintln!("Failed to kill process: {e}");
                            }
                        }
                    }
                }
            }

            if let Some(mut child) = child_handle.take() {
                let _ = child.kill();
            }
        });

        Self { tx, handle }
    }

    pub fn spawn(&self, path: PathBuf) -> Result<(), flume::SendError<()>> {
        self.tx
            .send(ExecCmd::Spawn(path))
            .map_err(|_| flume::SendError(()))
    }

    pub fn kill(&self) -> Result<(), flume::SendError<()>> {
        self.tx
            .send(ExecCmd::Kill)
            .map_err(|_| flume::SendError(()))
    }

    pub async fn shutdown(self) {
        drop(self.tx);
        if let Err(e) = self.handle.await {
            // Idk what to write here.
            tracing::warn!("Failed on runner_handle: {e}");
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_exec_runner_kill_when_idle() {
        let runner = ExecRunner::spawn_task();
        assert!(runner.kill().is_ok());
        runner.shutdown().await;
    }

    #[tokio::test]
    async fn test_exec_runner_spawn_and_kill() {
        let runner = ExecRunner::spawn_task();
        let exe = std::env::current_exe().expect("Should get current exe");

        assert!(runner.spawn(exe.clone()).is_ok());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(runner.kill().is_ok());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        runner.shutdown().await;
    }

    #[tokio::test]
    async fn test_exec_runner_multiple_spawns() {
        let runner = ExecRunner::spawn_task();
        let exe = std::env::current_exe().expect("Should get current exe");

        assert!(runner.spawn(exe.clone()).is_ok());
        // Second spawn while first is running should be safely ignored
        assert!(runner.spawn(exe).is_ok());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(runner.kill().is_ok());

        runner.shutdown().await;
    }

    #[tokio::test]
    async fn test_exec_runner_nonexistent_binary() {
        let runner = ExecRunner::spawn_task();
        let nonexistent = PathBuf::from("non_existent_binary_12345");

        assert!(runner.spawn(nonexistent).is_ok());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        runner.shutdown().await;
    }
}

