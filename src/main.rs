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

    match args.command {
        init::AppSubcommand::Send { key, file } => {
            exec_sendrecv::Handler::Send(broker_id, key, file)
        }
        init::AppSubcommand::Receive { filedir } => {
            exec_sendrecv::Handler::Receive(broker_id, filedir)
        }
        init::AppSubcommand::Broker => exec_sendrecv::Handler::Broker(broker_id),
    }
    .run()
    .await?;

    Ok(())
}
