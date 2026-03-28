//! imgpatchtools-rs — CLI entry point.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use imgpatchtools_rs::core::applypatch::cli::ApplypatchArgs;
use imgpatchtools_rs::core::blockimg::cli::BlockimgArgs;
use imgpatchtools_rs::core::edify::cli::EdifyArgs;

/// Bit-exact, high-performance Rust reimplementation of AOSP imgpatchtools.
#[derive(Parser, Debug)]
#[command(
    name = "imgpatchtools-rs",
    version,
    about = "Bit-exact Rust reimplementation of AOSP imgpatchtools"
)]
struct Cli {
    /// Enable verbose logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

/// Top-level subcommands.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Block-image operations (update / verify / range-sha1).
    Blockimg(BlockimgArgs),

    /// Apply a bsdiff / imgdiff patch to a single file.
    Applypatch(ApplypatchArgs),

    /// Create an imgdiff-format patch between two images.
    Imgdiff {
        /// Path to the source (old) image.
        #[arg(short = 's', long)]
        source: String,
        /// Path to the target (new) image.
        #[arg(short = 't', long)]
        target: String,
        /// Path to the output patch file.
        #[arg(short = 'o', long)]
        output: String,
        /// Optional chunk split size in bytes.
        #[arg(long)]
        chunk_size: Option<usize>,
    },

    /// Execute an Edify updater-script.
    Edify(EdifyArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    init_logger(cli.verbose);

    match dispatch(cli.command, cli.verbose) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            log::error!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

fn dispatch(cmd: Commands, verbose: bool) -> anyhow::Result<()> {
    match cmd {
        Commands::Blockimg(args) => imgpatchtools_rs::core::blockimg::cli::run(&args, verbose),
        Commands::Applypatch(args) => imgpatchtools_rs::core::applypatch::cli::run(&args, verbose),
        Commands::Imgdiff { source, target, output, chunk_size } => {
            imgpatchtools_rs::core::applypatch::imgdiff(&source, &target, &output, chunk_size)
        }
        Commands::Edify(args) => imgpatchtools_rs::core::edify::cli::run(&args, verbose),
    }
}

fn init_logger(verbose: bool) {
    let level = if verbose {
        log::LevelFilter::Info
    } else {
        log::LevelFilter::Warn
    };
    env_logger::Builder::new()
        .filter_level(level)
        .format_timestamp_millis()
        .init();
}