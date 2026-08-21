//! imgpatchtools-rs — CLI entry point.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use imgpatchtools_rs::core::applypatch::cli::ApplypatchArgs;
use imgpatchtools_rs::core::batch::cli::BatchArgs;
use imgpatchtools_rs::core::blockimg::cli::BlockimgArgs;

const PROJECT_URL: &str = "https://github.com/KawaiiSparkle/imgpatchtools-rs";

const COMPREHENSIVE_HELP: &str =
    "Run without arguments to see all commands and detailed usage examples.";

/// Bit-exact, high-performance Rust reimplementation of the AOSP OTA patching
/// toolchain, focused on unpacking and applying traditional block-based OTAs.
#[derive(Parser, Debug)]
#[command(
    name = "imgpatchtools-rs",
    version,
    about = "Bit-exact Rust tool for unpacking traditional Android OTA packages",
    after_help = COMPREHENSIVE_HELP,
)]
struct Cli {
    /// Enable verbose logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

/// Top-level subcommands.
#[derive(Subcommand, Debug)]
enum Commands {
    /// Block-image operations (update / verify / range-sha1).
    Blockimg(BlockimgArgs),

    /// Apply a bsdiff / imgdiff patch to a single file.
    Applypatch(ApplypatchArgs),

    /// Batch process sequential OTA packages (full + incremental chain).
    Batch(BatchArgs),
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Some(cmd) => {
            init_logger(cli.verbose);
            match dispatch(cmd, cli.verbose) {
                Ok(()) => ExitCode::SUCCESS,
                Err(err) => {
                    log::error!("{err:#}");
                    ExitCode::FAILURE
                }
            }
        }
        None => {
            // No subcommand: print comprehensive usage guide.
            print_full_usage();
            ExitCode::SUCCESS
        }
    }
}

fn dispatch(cmd: Commands, verbose: bool) -> anyhow::Result<()> {
    match cmd {
        Commands::Blockimg(args) => imgpatchtools_rs::core::blockimg::cli::run(&args, verbose),
        Commands::Applypatch(args) => imgpatchtools_rs::core::applypatch::cli::run(&args, verbose),
        Commands::Batch(args) => imgpatchtools_rs::core::batch::cli::run(&args),
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

/// Comprehensive usage guide printed when no subcommand is given.
fn print_full_usage() {
    println!();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║              imgpatchtools-rs                                   ║");
    println!("║     Bit-exact Rust tool for unpacking traditional Android OTAs    ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Project:  {PROJECT_URL}");
    println!();
    println!("OPTIONS:");
    println!("  -v, --verbose    Enable verbose logging");
    println!();
    println!("COMMANDS:");
    println!();
    println!("  blockimg         Block-image operations");
    println!("  applypatch       Apply bsdiff/imgdiff patch");
    println!("  batch            Batch process sequential OTA packages");
    println!();
    println!("──────────────────── COMMAND DETAILS ────────────────────");
    println!();
    println!("blockimg update <target> <transfer_list> <new_data> <patch_data>");
    println!("  Apply a block-based OTA update to a target image.");
    println!("  OPTIONS:");
    println!("    --source <PATH>        Source image for incremental updates");
    println!(
        "    --stash-dir <DIR>      Stash directory (default: <system_temp>/imgpatchtools-stash)"
    );
    println!("    --resume-file <PATH>   Resume from checkpoint");
    println!();
    println!("blockimg verify <target> <transfer_list>");
    println!("  Verify a target image against transfer list hashes.");
    println!();
    println!("blockimg range-sha1 <file> <ranges>");
    println!("  Compute SHA-1 of block ranges in a file.");
    println!("  OPTIONS:");
    println!("    --block-size <N>      Block size in bytes (default: 4096)");
    println!();
    println!("─────────────────────────────────────────────────────");
    println!();
    println!("applypatch <source> <target> <target_sha1> <target_size> <patch> [-c]");
    println!("  Apply a bsdiff/imgdiff patch. Use -c for check-only mode.");
    println!();
    println!("applypatch <partition> - --from-script");
    println!("  Read apply_patch parameters from update-script.");
    println!("  OPTIONS:");
    println!("    --from-script          Read parameters from ./update-script or META-INF/...");
    println!("    --ota <OTA_ZIP>        Read parameters and the patch from an OTA zip");
    println!();
    println!("─────────────────────────────────────────────────────");
    println!();
    println!("batch <full_ota> [inc_otas]... [OPTIONS]");
    println!("  Batch process sequential OTA packages (full + incremental chain).");
    println!("  OPTIONS:");
    println!("    -w, --workdir <DIR>         Working directory (default: ./batch_work)");
    println!("    -o, --output <DIR>          Output directory (default: <workdir>/output)");
    println!("    --cap <PART@INDEX>          Cap partition at OTA index (repeatable)");
    println!("    --exclude <PARTITIONS>      Exclude partitions (repeatable)");
    println!("    -t, --threads <N>           Thread count (default: 32)");
    println!("    --list                      List partitions in each OTA");
    println!("    --dry-run                   Show plan without executing");
    println!();
    println!("  EXAMPLES:");
    println!("    # Full OTA only:");
    println!("    imgpatchtools-rs batch full.zip");
    println!();
    println!("    # Full + two incrementals:");
    println!("    imgpatchtools-rs batch full.zip inc1.zip inc2.zip");
    println!();
    println!("    # With partition capping and exclusions:");
    println!("    imgpatchtools-rs batch full.zip inc1.zip inc2.zip \\");
    println!("      --cap boot@0 --exclude odm,dtbo");
    println!();
    println!("─────────────────────────────────────────────────────");
    println!();
    println!("NOTE: Logical-partition (super.img) tooling is intentionally not part of");
    println!("this tool. Use a dedicated `lptools` build that tracks AOSP");
    println!("partition_tools (lpmake / lpdump / lpunpack) instead.");
    println!();
}
