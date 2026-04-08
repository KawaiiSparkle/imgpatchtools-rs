//! CLI front-end for the `batch` subcommand.

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use super::run_batch_internal;

/// Arguments for the `batch` subcommand.
#[derive(Args, Debug, Clone)]
pub struct BatchArgs {
    /// Path to the full OTA zip package.
    pub full_ota: PathBuf,

    /// Paths to incremental OTA zip packages (in order).
    pub inc_otas: Vec<PathBuf>,

    /// Working directory for temporary extraction and processing.
    #[arg(short, long, default_value = "./batch_work")]
    pub workdir: String,

    /// Output directory for final partition images and super.img.
    #[arg(short, long)]
    pub output: Option<String>,

    /// Cap a partition's updates at a specific OTA index (0-based).
    /// Format: "partition@index". The partition is included in OTAs 0..=index,
    /// then excluded from all subsequent OTAs. Repeatable.
    ///
    /// Examples:
    ///   --cap boot@1    (boot updates only through the 1st incremental)
    ///   --cap system@0  (system only in the full OTA, never in incrementals)
    #[arg(long, value_name = "PART@INDEX")]
    pub cap: Vec<String>,

    /// Exclude specific partitions from ALL OTA updates (comma-separated).
    /// Repeatable: --exclude system,vendor --exclude odm
    #[arg(long, value_name = "PARTITIONS")]
    pub exclude: Vec<String>,

    /// Number of parallel threads (default: min(32, CPU cores)).
    #[arg(short, long, default_value = "32")]
    pub threads: usize,

    /// Skip automatic super.img building at the end.
    #[arg(long)]
    pub no_super: bool,

    /// List partitions referenced in each OTA package without executing.
    #[arg(long)]
    pub list: bool,

    /// Show execution plan without actually running anything.
    #[arg(long)]
    pub dry_run: bool,

    /// Android version for LP metadata when building super.img (10, 11, 12+).
    #[arg(long, default_value = "12")]
    pub android_version: String,

    /// Super.img output format: "sparse" or "raw".
    #[arg(long, default_value = "sparse")]
    pub format: String,

    /// Verify mode: execute all commands including block_image_verify and assertions.
    /// Without this flag, only apply_patch, block_image_update and abort are executed.
    #[arg(long)]
    pub verify: bool,
}

/// Execute the `batch` subcommand.
pub fn run(args: &BatchArgs) -> Result<()> {
    // Parse --cap arguments into a HashMap<partition, max_ota_index>.
    let mut caps: HashMap<String, usize> = HashMap::new();
    for spec in &args.cap {
        let parts: Vec<&str> = spec.splitn(2, '@').collect();
        if parts.len() != 2 {
            anyhow::bail!(
                "invalid --cap format '{}', expected PARTITION@INDEX (e.g. boot@1)",
                spec
            );
        }
        let partition = parts[0].trim().to_string();
        let index: usize = parts[1].trim().parse().with_context(|| {
            format!(
                "invalid OTA index in --cap '{}', expected non-negative integer",
                spec
            )
        })?;
        caps.insert(partition, index);
    }

    // Parse --exclude arguments (comma-separated per flag).
    let mut excludes: Vec<String> = Vec::new();
    for spec in &args.exclude {
        for name in spec.split(',') {
            let trimmed = name.trim();
            if !trimmed.is_empty() {
                excludes.push(trimmed.to_string());
            }
        }
    }

    // Determine effective thread count.
    let max_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let threads = args.threads.min(max_threads).max(1);

    // Determine output directory (defaults to <workdir>/output).
    let output_dir = args
        .output
        .clone()
        .unwrap_or_else(|| {
            let workdir_path = std::path::Path::new(&args.workdir);
            workdir_path.join("output").to_string_lossy().into_owned()
        });

    let config = super::BatchConfig {
        workdir: args.workdir.clone(),
        output_dir,
        threads,
        no_super: args.no_super,
        dry_run: args.dry_run,
        list_only: args.list,
        caps,
        excludes,
        android_version: args.android_version.clone(),
        format: args.format.clone(),
        verify: args.verify,
    };

    run_batch_internal(&args.full_ota, &args.inc_otas, &config)
}
