//! imgpatchtools-rs — CLI entry point.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use imgpatchtools_rs::core::applypatch::cli::ApplypatchArgs;
use imgpatchtools_rs::core::batch::cli::BatchArgs;
use imgpatchtools_rs::core::blockimg::cli::BlockimgArgs;
use imgpatchtools_rs::core::edify::cli::EdifyArgs;
use imgpatchtools_rs::core::super_img::cli::{LpdumpArgs, LpmakeArgs, LpunpackArgs, SuperArgs};

const PROJECT_URL: &str = "https://github.com/KawaiiSparkle/imgpatchtools-rs";
const WIKI_URL: &str = "https://github.com/KawaiiSparkle/imgpatchtools-rs/wiki";

const COMPREHENSIVE_HELP: &str =
    "Run without arguments to see all commands and detailed usage examples.";

/// Bit-exact, high-performance Rust reimplementation of AOSP imgpatchtools.
#[derive(Parser, Debug)]
#[command(
    name = "imgpatchtools-rs",
    version,
    about = "Bit-exact Rust reimplementation of AOSP imgpatchtools",
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

    /// Create an imgdiff-format patch between two images (not yet implemented).
    Imgdiff {
        #[arg(short = 's', long)]
        source: String,
        #[arg(short = 't', long)]
        target: String,
        #[arg(short = 'o', long)]
        output: String,
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
        Commands::Imgdiff {
            source,
            target,
            output,
            chunk_size,
        } => imgpatchtools_rs::core::applypatch::imgdiff(&source, &target, &output, chunk_size),
        Commands::Edify(args) => imgpatchtools_rs::core::edify::cli::run(&args, verbose),
        Commands::Super(args) => imgpatchtools_rs::core::super_img::cli::run(&args),
        Commands::Lpmake(args) => imgpatchtools_rs::core::super_img::cli::run_lpmake(&args),
        Commands::Lpdump(args) => imgpatchtools_rs::core::super_img::cli::run_lpdump(&args),
        Commands::Lpunpack(args) => imgpatchtools_rs::core::super_img::cli::run_lpunpack(&args),
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
    println!("║     Bit-exact Rust reimplementation of AOSP imgpatchtools         ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Project:  {}", PROJECT_URL);
    println!("Wiki:     {}", WIKI_URL);
    println!();
    println!("OPTIONS:");
    println!("  -v, --verbose    Enable verbose logging");
    println!();
    println!("COMMANDS:");
    println!();
    println!("  blockimg         Block-image operations");
    println!("  applypatch       Apply bsdiff/imgdiff patch");
    println!("  edify            Execute Edify updater-script");
    println!("  super            Smart super.img builder (auto-detect)");
    println!("  lpmake           Build super.img (expert mode)");
    println!("  lpdump           Dump LP metadata from super.img");
    println!("  lpunpack         Extract partitions from super.img");
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
    println!("applypatch --source <SRC> --target <TGT> --target-hash <SHA1>");
    println!("           --target-size <SIZE> --patch <PATCH> [-c]");
    println!("  Apply a bsdiff/imgdiff patch. Use -c for check-only mode.");
    println!();
    println!("─────────────────────────────────────────────────────");
    println!();
    println!("edify <script> -w <workdir>");
    println!("  Execute an Edify updater-script.");
    println!("  Automatically patches recovery.img if applicable.");
    println!("  Prompts for super.img build if dynamic partitions detected.");
    println!();
    println!("─────────────────────────────────────────────────────");
    println!();
    println!("super -w <workdir> [-o <output>] [--op-list <FILE>]");
    println!("      [--partitions <list>] [--groups <spec>]...");
    println!("  Smart super.img builder. Auto-detects *.img from workdir.");
    println!("  OPTIONS:");
    println!("    --op-list <FILE>        dynamic_partitions_op_list file");
    println!("    --partitions <LIST>     Comma-separated partition names");
    println!("    --groups <SPEC>         Group definitions (repeatable): \"name:max_size\"");
    println!("    --android-version <V>   LP metadata version (10/11/12)");
    println!("    --slots <N>             Metadata slots (1=non-A/B, 2=A/B)");
    println!("    --device-size <BYTES>   Device size (0=auto)");
    println!("    --metadata-size <BYTES> Metadata max size (default: 65536)");
    println!("    --format <FMT>          Output: sparse (default) or raw");
    println!();
    println!("lpmake --output <FILE> --device-size <BYTES>");
    println!("       --partition <spec>... --group <spec>...");
    println!("  Expert mode: build super.img from explicit specs.");
    println!("  Partition spec: \"name:group:size[:attr]\"");
    println!("  Group spec: \"name:max_size\"");
    println!("  OPTIONS:");
    println!("    --alignment <BYTES>     Partition alignment (default: 1048576)");
    println!("    --metadata-size <BYTES> Metadata max size (default: 65536)");
    println!("    --metadata-slots <N>   Metadata slots (default: 2)");
    println!("    --android-version <V>  LP metadata version (default: 12)");
    println!("    --sparse                Write sparse format (default: true)");
    println!();
    println!("lpdump <image> [--slot <N>]");
    println!("  Dump LP metadata from a super.img.");
    println!();
    println!("lpunpack <image> [-o <DIR>] [--slot <N>] [--partitions <list>]");
    println!("  Extract partition images from a super.img.");
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
    println!("    --no-super                  Skip automatic super.img build");
    println!("    --list                      List partitions in each OTA");
    println!("    --dry-run                   Show plan without executing");
    println!("    --android-version <V>       LP metadata version (default: 12)");
    println!("    --format <FMT>              Super output: sparse or raw");
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
    println!("    # List partitions in each OTA:");
    println!("    imgpatchtools-rs batch full.zip inc1.zip --list");
    println!();
    println!("    # Dry run to preview execution plan:");
    println!("    imgpatchtools-rs batch full.zip inc1.zip --dry-run");
    println!();
}
