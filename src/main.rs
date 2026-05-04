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

    match args.command {
        init::AppSubcommand::Send { key, file } => {
            exec_sendrecv::Handler::Send(iroh::PublicKey::from_str(&key)?, file)
        }
        init::AppSubcommand::Receive { filedir } => exec_sendrecv::Handler::Receive(filedir),
    }
    .run()
    .await?;

    Ok(())
}
