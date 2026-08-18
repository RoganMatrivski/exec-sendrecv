use std::sync::LazyLock;

use color_eyre::Report;
use init::ProgressBarLogWriter;
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
    let broker_id = args.broker_id;

    if MPB.is_hidden() {
        tracing::warn!("Warning! Progress bar is hidden.");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
    Ok(())
}
