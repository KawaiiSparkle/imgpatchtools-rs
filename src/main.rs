//! imgpatchtools-rs — CLI entry point.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use imgpatchtools_rs::core::applypatch::cli::ApplypatchArgs;
use imgpatchtools_rs::core::blockimg::cli::BlockimgArgs;
use imgpatchtools_rs::core::edify::cli::EdifyArgs;
use imgpatchtools_rs::core::super_img::cli::{LpmakeArgs, LpdumpArgs, LpunpackArgs, SuperArgs};

/// Bit-exact, high-performance Rust reimplementation of AOSP imgpatchtools.
#[derive(Parser, Debug)]
#[command(
    name = "imgpatchtools-rs",
    version,
    about = "Bit-exact Rust reimplementation of AOSP imgpatchtools",
    arg_required_else_help(true)
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

    /// Smart super.img builder — auto-detect partitions from workdir.
    Super(SuperArgs),

    /// Build a super.img (expert mode, like AOSP lpmake).
    Lpmake(LpmakeArgs),

    /// Dump LP metadata from a super.img (like AOSP lpdump).
    Lpdump(LpdumpArgs),

    /// Extract partition images from a super.img (like AOSP lpunpack).
    Lpunpack(LpunpackArgs),

    /// [WIP] Batch process sequential OTA packages.
    Batch {
        /// Path to the Full OTA zip.
        full_ota: String,
        /// Paths to incremental OTA zips (in order).
        inc_otas: Vec<String>,
        /// Working directory.
        #[arg(short, long, default_value = ".")]
        workdir: String,
    },
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
        Commands::Super(args) => imgpatchtools_rs::core::super_img::cli::run_super(&args),
        Commands::Lpmake(args) => imgpatchtools_rs::core::super_img::cli::run_lpmake(&args),
        Commands::Lpdump(args) => imgpatchtools_rs::core::super_img::cli::run_lpdump(&args),
        Commands::Lpunpack(args) => imgpatchtools_rs::core::super_img::cli::run_lpunpack(&args),
        Commands::Batch { .. } => {
            anyhow::bail!("batch command is under construction!");
        }
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