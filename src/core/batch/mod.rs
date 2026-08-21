//! Batch OTA processing engine — sequential full + incremental chain execution.
//!
//! This is a thin orchestration layer: it extracts the files each OTA package
//! needs and then delegates the actual work to [`crate::core::blockimg`]
//! (block partitions) and [`crate::core::applypatch`] (boot/recovery-style
//! partitions patched via `apply_patch`).
//!
//! OTA zip reading goes through the `zip` crate ([`crate::core::ota`]); no
//! external binary (such as `7z`) is invoked. Logical-partition (`super.img`)
//! assembly is intentionally out of scope here — use a dedicated `lptools`
//! build tracking AOSP `partition_tools` for that.

pub mod cli;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::core::applypatch::apply;
use crate::core::blockimg::update;
use crate::core::ota;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Configuration for the batch run, parsed from CLI arguments by `cli.rs`.
pub struct BatchConfig {
    pub workdir: String,
    pub output_dir: String,
    pub threads: usize,
    pub dry_run: bool,
    pub list_only: bool,
    pub caps: HashMap<String, usize>,
    pub excludes: Vec<String>,
}

// ---------------------------------------------------------------------------
// Step timing tracker
// ---------------------------------------------------------------------------

/// Tracks timing for each processing step.
#[derive(Debug, Default)]
pub struct StepTimer {
    steps: Vec<(String, Duration)>,
    current: Option<(String, Instant)>,
}

impl StepTimer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn start(&mut self, name: impl Into<String>) {
        self.current = Some((name.into(), Instant::now()));
    }

    pub fn end(&mut self) {
        if let Some((name, start)) = self.current.take() {
            let duration = start.elapsed();
            self.steps.push((name, duration));
        }
    }

    pub fn print_summary(&self) {
        if self.steps.is_empty() {
            return;
        }
        let total: Duration = self.steps.iter().map(|(_, d)| *d).sum();

        println!();
        println!("╔══════════════════════════════════════════════════════╗");
        println!("║          Step Timing Summary                         ║");
        println!("╚══════════════════════════════════════════════════════╝");
        println!();
        for (name, duration) in &self.steps {
            let pct = if total.as_secs() > 0 {
                (duration.as_secs_f64() / total.as_secs_f64()) * 100.0
            } else {
                0.0
            };
            println!("  {name:40} {:>10} ({pct:5.1}%)", format_duration(*duration));
        }
        println!();
        println!("  {:40} {:>10}", "TOTAL", format_duration(total));
        println!();
    }
}

fn format_duration(d: Duration) -> String {
    if d.as_secs() >= 3600 {
        format!(
            "{}h {:02}m {:02}s",
            d.as_secs() / 3600,
            (d.as_secs() % 3600) / 60,
            d.as_secs() % 60
        )
    } else if d.as_secs() >= 60 {
        format!("{}m {:02}s", d.as_secs() / 60, d.as_secs() % 60)
    } else if d.as_secs() > 0 {
        format!("{}.{:03}s", d.as_secs(), d.subsec_millis())
    } else {
        format!("{}ms", d.as_millis())
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct OtaPackage {
    path: PathBuf,
    index: usize,
    name: String,
    is_full: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PartitionKind {
    /// Reconstructed from a transfer list + new data (`block_image_update`).
    Block,
    /// Patched via `apply_patch` (e.g. boot, recovery).
    Patch,
}

// ---------------------------------------------------------------------------
// CLI entry point (called from batch::cli::run)
// ---------------------------------------------------------------------------

pub fn run_batch_internal(
    full_ota: &Path,
    inc_otas: &[PathBuf],
    config: &BatchConfig,
) -> Result<()> {
    // Collect all OTA packages: full first, then incrementals.
    let mut packages: Vec<OtaPackage> = Vec::new();
    packages.push(OtaPackage {
        path: full_ota.to_path_buf(),
        index: 0,
        name: file_stem(full_ota),
        is_full: true,
    });
    for (i, path) in inc_otas.iter().enumerate() {
        packages.push(OtaPackage {
            path: path.clone(),
            index: i + 1,
            name: file_stem(path),
            is_full: false,
        });
    }

    // Validate that all packages exist.
    for pkg in &packages {
        if !pkg.path.exists() {
            bail!("OTA package not found: {}", pkg.path.display());
        }
    }

    // List-only mode.
    if config.list_only {
        return list_partitions(&packages);
    }

    // Dry-run mode: print the execution plan.
    if config.dry_run {
        print_dry_run(&packages, config);
        return Ok(());
    }

    // Actual execution.
    execute_batch(&packages, config)
}

// ---------------------------------------------------------------------------
// Partition listing (--list mode)
// ---------------------------------------------------------------------------

fn list_partitions(packages: &[OtaPackage]) -> Result<()> {
    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          OTA Package — Partition Overview            ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();

    for pkg in packages {
        let label = if pkg.is_full {
            format!("OTA #{} (full)", pkg.index)
        } else {
            format!("OTA #{} (inc)", pkg.index)
        };

        println!("{}: {}", label, pkg.path.display());

        match scan_ota_partitions(&pkg.path) {
            Ok(parts) => {
                if parts.is_empty() {
                    println!("  (no partitions detected)");
                } else {
                    println!("  Partitions ({}): {}", parts.len(), parts.join(", "));
                }
            }
            Err(e) => {
                println!("  [ERROR] failed to scan: {e}");
            }
        }
        println!();
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Dry-run mode
// ---------------------------------------------------------------------------

fn print_dry_run(packages: &[OtaPackage], config: &BatchConfig) {
    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Batch OTA — Dry Run Plan                   ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  Workdir:       {}", config.workdir);
    println!("  Output dir:    {}", config.output_dir);
    println!("  Threads:       {}", config.threads);
    println!();

    if !config.excludes.is_empty() {
        println!("  Excluded partitions: {}", config.excludes.join(", "));
    }
    if !config.caps.is_empty() {
        println!("  Capped partitions:");
        for (part, idx) in &config.caps {
            println!("    {part} → up to OTA #{idx}");
        }
    }
    println!();

    println!("  Execution plan:");
    for pkg in packages {
        let label = if pkg.is_full { "FULL" } else { "INC " };
        println!("    [{label}] OTA #{}: {}", pkg.index, pkg.path.display());

        let excluded_here = partitions_excluded_at(pkg.index, config);
        if !excluded_here.is_empty() {
            println!("         skip: {}", excluded_here.join(", "));
        }
    }

    println!();
    println!("  (dry run — no changes were made)");
}

// ---------------------------------------------------------------------------
// Main batch execution
// ---------------------------------------------------------------------------

fn execute_batch(packages: &[OtaPackage], config: &BatchConfig) -> Result<()> {
    let workdir = Path::new(&config.workdir);
    let mut timer = StepTimer::new();

    // Clean up any residual files from previous runs.
    if workdir.exists() {
        log::info!("cleaning up residual workdir: {}", workdir.display());
        fs::remove_dir_all(workdir)
            .with_context(|| format!("failed to remove residual workdir {}", workdir.display()))?;
    }
    fs::create_dir_all(workdir)
        .with_context(|| format!("create workdir {}", workdir.display()))?;

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Batch OTA Processing Started                ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  Packages:    {}", packages.len());
    println!("  Workdir:     {}", config.workdir);
    println!("  Output:      {}", config.output_dir);
    println!("  Threads:     {}", config.threads);
    println!();

    for pkg in packages {
        let label = if pkg.is_full { "full OTA" } else { "incremental OTA" };

        println!("──────────────────────────────────────────────────");
        println!("[OTA #{}] Processing {} ({})", pkg.index, label, pkg.name);
        println!("  Source: {}", pkg.path.display());

        // Prepare source images for incremental (link previous version -> current name).
        if !pkg.is_full {
            timer.start(format!("OTA #{} - Prepare sources", pkg.index));
            prepare_source_images(workdir, pkg.index - 1)?;
            timer.end();
        }

        // Process partitions.
        timer.start(format!("OTA #{} - Process partitions", pkg.index));
        let partitions = scan_ota_partitions(&pkg.path)?;
        let (ok, failed) = process_partitions(workdir, pkg, &partitions, config)?;
        timer.end();

        if !failed.is_empty() {
            bail!(
                "OTA #{} processing failed — failed partitions: {}",
                pkg.index,
                failed.join(", ")
            );
        }

        // Promote freshly-built images to versioned names.
        timer.start(format!("OTA #{} - Promote images", pkg.index));
        promote_files_to_version(workdir, pkg.index)?;
        timer.end();

        println!("  OTA #{} completed ({} partitions).", pkg.index, ok.len());
        println!();
    }

    // Copy the final (highest-version) images to the output directory.
    timer.start("Finalize output");
    let final_output = Path::new(&config.output_dir);
    fs::create_dir_all(final_output)
        .with_context(|| format!("create output dir {}", final_output.display()))?;
    move_final_images(workdir, final_output, packages.len() - 1)?;
    timer.end();

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Batch OTA Processing Complete               ║");
    println!("╚══════════════════════════════════════════════════════╝");
    timer.print_summary();

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition processing
// ---------------------------------------------------------------------------

/// Process all partitions of one OTA package.
/// Returns (succeeded, failed) partition name lists.
fn process_partitions(
    workdir: &Path,
    pkg: &OtaPackage,
    partitions: &[String],
    config: &BatchConfig,
) -> Result<(Vec<String>, Vec<String>)> {
    let mut ok = Vec::new();
    let mut failed = Vec::new();

    for part in partitions {
        if partitions_excluded_at(pkg.index, config).contains(part) {
            log::info!("skipping excluded/capped partition: {part}");
            continue;
        }

        let kind = detect_partition_kind(&pkg.path, part);
        let result = match kind {
            PartitionKind::Block => process_block_partition(workdir, &pkg.path, part, pkg.is_full),
            PartitionKind::Patch => process_patch_partition(workdir, &pkg.path, part, pkg.is_full),
        };

        match result {
            Ok(()) => {
                println!("    ✓ {part}");
                ok.push(part.clone());
            }
            Err(e) => {
                log::error!("partition {part} failed: {e:#}");
                failed.push(part.clone());
            }
        }
    }

    Ok((ok, failed))
}

/// Process a block partition (transfer list + new data) into `{part}.img`.
fn process_block_partition(
    workdir: &Path,
    ota_path: &Path,
    part: &str,
    is_full: bool,
) -> Result<()> {
    let transfer_list = workdir.join(format!("{part}.transfer.list"));
    let new_data = extract_new_data(ota_path, workdir, part)?;
    let patch_data = workdir.join(format!("{part}.patch.dat"));

    // Extract transfer list and patch data (patch data is optional in full OTAs).
    ota::extract_from_zip(ota_path, &format!("{part}.transfer.list"), &transfer_list)
        .with_context(|| format!("extract transfer list for {part}"))?;
    if ota::extract_from_zip(ota_path, &format!("{part}.patch.dat"), &patch_data).is_err() {
        // Some full OTAs ship no patch.dat; block_image_update ignores it for `new` ops.
        log::info!("{part}: no patch.dat in package, assuming full-OTA `new` only");
    }

    let target = workdir.join(format!("{part}.img"));
    let source = if is_full {
        None
    } else {
        // Incremental: source is the current (unversioned) image, prepared by
        // prepare_source_images from the previous version.
        let src = workdir.join(format!("{part}.img"));
        if src.exists() {
            Some(src)
        } else {
            None
        }
    };

    let stash_dir = std::env::temp_dir().join("imgpatchtools-stash");
    fs::create_dir_all(&stash_dir).with_context(|| "create stash dir")?;

    update::block_image_update(
        &target,
        &transfer_list,
        &new_data,
        &patch_data,
        source.as_deref(),
        &stash_dir,
        true,
        None,
    )?;

    Ok(())
}

/// Extract the new-data file (`{part}.new.dat` / `.br` / `.lzma`) and return
/// its filesystem path.
fn extract_new_data(ota_path: &Path, workdir: &Path, part: &str) -> Result<PathBuf> {
    for suffix in [".new.dat.br", ".new.dat.lzma", ".new.dat"] {
        let entry = format!("{part}{suffix}");
        let dest = workdir.join(&entry);
        if ota::extract_from_zip(ota_path, &entry, &dest).is_ok() {
            return Ok(dest);
        }
    }
    bail!("no new-data file found for partition {part}")
}

/// Process a patch partition (boot/recovery style) via `apply_patch`.
fn process_patch_partition(
    workdir: &Path,
    ota_path: &Path,
    part: &str,
    is_full: bool,
) -> Result<()> {
    let script = ota::read_update_script_from_zip(ota_path)?;
    let info = ota::parse_apply_patch(&script, part)?;

    let source = workdir.join(format!("{part}.img"));
    let target = workdir.join(format!("{part}.img"));

    // In a full OTA, if a full image ships directly, just extract it.
    if is_full {
        for entry in [format!("{part}.img"), format!("{part}.img.br")] {
            if ota::extract_from_zip(ota_path, &entry, &target).is_ok() {
                log::info!("{part}: extracted full image from package");
                return Ok(());
            }
        }
        // Otherwise fall through: apply the patch (typically against an empty source).
        if !source.exists() {
            let size = info.target_size;
            let zeros = vec![0u8; size as usize];
            fs::write(&source, &zeros).with_context(|| format!("create zero source {part}"))?;
        }
    } else if !source.exists() {
        bail!("source image for {part} not found in workdir (missing previous version?)");
    }

    // Extract the patch from the zip.
    let patch_name = info
        .patch_file
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| format!("{part}.img.p"));
    let patch_dest = workdir.join(&patch_name);
    for candidate in [format!("patch/{patch_name}"), patch_name.clone()] {
        if ota::extract_from_zip(ota_path, &candidate, &patch_dest).is_ok() {
            break;
        }
    }
    if !patch_dest.exists() {
        bail!("patch for {part} not found in {}", ota_path.display());
    }

    apply::apply_patch(&source, &target, &info.target_sha1, info.target_size, &patch_dest)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Partition scanning & detection
// ---------------------------------------------------------------------------

/// Detect the kind of a partition by inspecting the OTA zip entries.
fn detect_partition_kind(ota_path: &Path, part: &str) -> PartitionKind {
    let entries = ota::list_zip_entries(ota_path).unwrap_or_default();
    let transfer = format!("{part}.transfer.list");
    if entries.iter().any(|e| e.ends_with(&transfer)) {
        PartitionKind::Block
    } else {
        PartitionKind::Patch
    }
}

/// Scan an OTA package and return the list of partition names it touches.
fn scan_ota_partitions(ota_path: &Path) -> Result<Vec<String>> {
    let entries = ota::list_zip_entries(ota_path)?;
    let mut parts: HashSet<String> = HashSet::new();

    for name in &entries {
        let basename = name.rsplit('/').next().unwrap_or(name);

        // Block partitions: <part>.transfer.list
        if let Some(rest) = basename.strip_suffix(".transfer.list")
            && !rest.is_empty()
        {
            parts.insert(rest.to_string());
        }

        // Patch partitions: patch/<part>.img.p or <part>.img.p
        if let Some(rest) = basename.strip_suffix(".img.p")
            && !rest.is_empty()
        {
            parts.insert(rest.to_string());
        }
    }

    let mut result: Vec<String> = parts.into_iter().collect();
    result.sort();
    Ok(result)
}

// ---------------------------------------------------------------------------
// Versioned image management
// ---------------------------------------------------------------------------

/// Prepare source images for an incremental OTA by copying the previous
/// version's image into place as the current (unversioned) name.
fn prepare_source_images(workdir: &Path, prev_version: usize) -> Result<()> {
    let suffix = format!(".img.{prev_version}");
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().into_owned();
        let Some(partition) = filename.strip_suffix(&suffix) else {
            continue;
        };
        let target = workdir.join(format!("{partition}.img"));
        fs::copy(&path, &target)
            .with_context(|| format!("copy {} -> {}", path.display(), target.display()))?;
    }
    Ok(())
}

/// Rename freshly-built unversioned `*.img` files to `*.img.{version}`.
fn promote_files_to_version(workdir: &Path, version: usize) -> Result<()> {
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().into_owned();
        let Some(partition) = filename.strip_suffix(".img") else {
            continue;
        };
        // Skip already-versioned names.
        if partition.contains(".img.") {
            continue;
        }
        let versioned = workdir.join(format!("{partition}.img.{version}"));
        fs::rename(&path, &versioned)
            .with_context(|| format!("rename {} -> {}", path.display(), versioned.display()))?;
    }
    Ok(())
}

/// Copy the final-version images into the output directory.
fn move_final_images(workdir: &Path, output_dir: &Path, version: usize) -> Result<()> {
    let suffix = format!(".img.{version}");
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().into_owned();
        let Some(partition) = filename.strip_suffix(&suffix) else {
            continue;
        };
        let dest = output_dir.join(format!("{partition}.img"));
        fs::copy(&path, &dest)
            .with_context(|| format!("copy {} -> {}", path.display(), dest.display()))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Partition exclusion
// ---------------------------------------------------------------------------

fn partitions_excluded_at(ota_index: usize, config: &BatchConfig) -> Vec<String> {
    let mut excluded = Vec::new();
    let exclude_set: HashSet<&String> = config.excludes.iter().collect();

    let mut all_parts: HashSet<String> = exclude_set.iter().map(|s| (*s).clone()).collect();
    for part in config.caps.keys() {
        all_parts.insert(part.clone());
    }

    for part in &all_parts {
        if exclude_set.contains(part) {
            excluded.push(part.clone());
        } else if let Some(&cap_idx) = config.caps.get(part)
            && ota_index > cap_idx
        {
            excluded.push(part.clone());
        }
    }

    excluded.sort();
    excluded
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "ota".to_string())
}
