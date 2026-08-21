use clap::Parser;
use color_eyre::Report;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Verbosity log
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: AppSubcommand,
}

#[derive(clap::Subcommand)]
pub enum AppSubcommand {
    Send {
        recv_code: String,
        root: std::path::PathBuf,
    },
    Receive {
        #[arg(long)]
        install: bool,
        root: std::path::PathBuf,
    },
}

const VERBOSE_LEVELS: &[&str] = &["info", "debug", "trace"];

macro_rules! pkg_name {
    () => {
        env!("CARGO_PKG_NAME").replace('-', "_")
    };
}
pub fn initialize() -> Result<Args, Report> {
    dotenvy::dotenv().ok();

    use tracing_error::ErrorLayer;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{EnvFilter, fmt};

    let supports_color = supports_color::on(supports_color::Stream::Stderr).is_some();

    if supports_color {
        color_eyre::install()?;
    } else {
        color_eyre::config::HookBuilder::new()
            .theme(color_eyre::config::Theme::new())
            .install()?;
    }

    let args = Args::parse();

    let crate_level = args
        .verbose
        .min(VERBOSE_LEVELS.len() as u8)
        .checked_sub(1)
        .map(|i| VERBOSE_LEVELS[i as usize])
        .unwrap_or("warn");

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("warn"))
        .add_directive(format!("{}={}", pkg_name!(), crate_level).parse().unwrap())
        .add_directive(format!("patchsync={}", crate_level).parse().unwrap());

    let fmt_layer = fmt::layer()
        .with_writer(|| crate::MPB.mpb_writer())
        .with_level(true)
        .with_thread_ids(args.verbose > 1)
        .with_thread_names(args.verbose > 2)
        .with_ansi(supports_color);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(env_filter)
        .with(ErrorLayer::default())
        .init();

    Ok(args)
}

use indicatif::MultiProgress;
use std::{io::Write, ops::Deref};
pub struct ProgressBarLogWriter<W: Write> {
    writer: W,
    mpb: MultiProgress,
}

impl<W: Write> ProgressBarLogWriter<W> {
    pub fn new(writer: W, mpb: MultiProgress) -> Self {
        Self { writer, mpb }
    }

    fn mpb_writer(&self) -> Box<dyn std::io::Write> {
        Box::new(ProgressBarLogWriter::new(
            std::io::stderr(),
            self.mpb.clone(),
        ))
    }
}

impl<W: Write> Write for ProgressBarLogWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.mpb.suspend(|| self.writer.write(buf))
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.mpb.suspend(|| self.writer.flush())
    }
}

impl<W: Write> Deref for ProgressBarLogWriter<W> {
    type Target = MultiProgress;

    fn deref(&self) -> &Self::Target {
        &self.mpb
    }
}

impl Default for ProgressBarLogWriter<std::io::Stderr> {
    fn default() -> Self {
        Self {
            writer: std::io::stderr(),
            mpb: indicatif::MultiProgress::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_parse_send_subcommand() {
        let args = Args::try_parse_from(["exec-sendrecv", "send", "CODE123", "/path/to/source"])
            .expect("Should parse send command");
        assert_eq!(args.verbose, 0);
        match args.command {
            AppSubcommand::Send { recv_code, root } => {
                assert_eq!(recv_code, "CODE123");
                assert_eq!(root, std::path::PathBuf::from("/path/to/source"));
            }
            _ => panic!("Expected Send subcommand"),
        }
    }

    #[test]
    fn test_parse_receive_subcommand() {
        let args = Args::try_parse_from(["exec-sendrecv", "receive", "/path/to/dest"])
            .expect("Should parse receive command");
        assert_eq!(args.verbose, 0);
        match args.command {
            AppSubcommand::Receive { root, install } => {
                assert_eq!(root, std::path::PathBuf::from("/path/to/dest"));
                assert!(!install);
            }
            _ => panic!("Expected Receive subcommand"),
        }
    }

    #[test]
    fn test_parse_receive_install_flag() {
        let args = Args::try_parse_from([
            "exec-sendrecv",
            "-vv",
            "receive",
            "--install",
            "/path/to/dest",
        ])
        .expect("Should parse receive with install and verbose flags");
        assert_eq!(args.verbose, 2);
        match args.command {
            AppSubcommand::Receive { root, install } => {
                assert_eq!(root, std::path::PathBuf::from("/path/to/dest"));
                assert!(install);
            }
            _ => panic!("Expected Receive subcommand"),
        }
    }

    #[test]
    fn test_parse_invalid_subcommand() {
        let res = Args::try_parse_from(["exec-sendrecv", "unknown"]);
        assert!(res.is_err());
    }

    #[test]
    fn test_progress_bar_log_writer() {
        use std::io::Write;
        let mut buffer = Vec::new();
        let mpb = indicatif::MultiProgress::new();
        let mut writer = ProgressBarLogWriter::new(&mut buffer, mpb);

        writer.write_all(b"hello world\n").expect("write should succeed");
        writer.flush().expect("flush should succeed");

        assert_eq!(buffer, b"hello world\n");
    }
}

