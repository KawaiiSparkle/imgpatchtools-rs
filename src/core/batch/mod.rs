//! Batch OTA processing engine — sequential full + incremental chain execution.
//!
//! Handles:
//!   - Extracting OTA zips to isolated work directories.
//!   - Running edify scripts for each OTA in sequence.
//!   - Partition version capping (`--cap PART@N`) and exclusion (`--exclude`).
//!   - Automatic super.img assembly when dynamic partitions are detected.
//!   - Partition listing (`--list`) and dry-run (`--dry-run`) modes.

pub mod cli;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

use crate::core::edify::functions::{builtin_registry, run_script_with_mode};
use crate::core::super_img::cli::SuperArgs;
use crate::core::super_img::op_list::DynamicPartitionState;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Configuration for the batch run, parsed from CLI arguments by `cli.rs`.
pub struct BatchConfig {
    pub workdir: String,
    pub output_dir: String,
    pub threads: usize,
    pub no_super: bool,
    pub dry_run: bool,
    pub list_only: bool,
    pub caps: HashMap<String, usize>,
    pub excludes: Vec<String>,
    pub android_version: String,
    pub format: String,
    pub verify: bool,
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
    /// Create a new step timer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Start timing a new step.
    pub fn start(&mut self, name: impl Into<String>) {
        self.current = Some((name.into(), Instant::now()));
    }

    /// End the current step and record its duration.
    pub fn end(&mut self) {
        if let Some((name, start)) = self.current.take() {
            let duration = start.elapsed();
            self.steps.push((name, duration));
        }
    }

    /// Record a completed step with its duration.
    pub fn record(&mut self, name: impl Into<String>, duration: Duration) {
        self.steps.push((name.into(), duration));
    }

    /// Print all recorded timings.
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
            println!(
                "  {:40} {:>10} ({:5.1}%)",
                name,
                format_duration(*duration),
                pct
            );
        }
        println!("  {:40} {:>10}", "─".repeat(40), "─".repeat(10));
        println!("  {:40} {:>10}", "TOTAL", format_duration(total));
        println!();
    }
}

/// Format a duration in human-readable form.
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
// Internal types
// ---------------------------------------------------------------------------

struct OtaPackage {
    path: PathBuf,
    index: usize,
    name: String,
    is_full: bool,
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
    println!(
        "  Build super:   {}",
        if config.no_super { "no" } else { "yes" }
    );
    println!();

    if !config.excludes.is_empty() {
        println!("  Excluded partitions: {}", config.excludes.join(", "));
    }
    if !config.caps.is_empty() {
        println!("  Capped partitions:");
        for (part, idx) in &config.caps {
            println!("    {} → up to OTA #{}", part, idx);
        }
    }
    println!();

    println!("  Execution plan:");
    for pkg in packages {
        let label = if pkg.is_full { "FULL" } else { "INC " };
        println!("    [{}] OTA #{}: {}", label, pkg.index, pkg.path.display());

        // Show which partitions would be excluded at this step.
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
    let batch_start = Instant::now();

    // Clean up any residual files from previous runs (Windows: ensure no leftovers)
    if workdir.exists() {
        log::info!("cleaning up residual workdir: {}", workdir.display());
        fs::remove_dir_all(workdir)
            .with_context(|| format!("failed to remove residual workdir {}", workdir.display()))?;
    }

    // Create fresh workdir.
    fs::create_dir_all(workdir).with_context(|| format!("create workdir {}", workdir.display()))?;

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Batch OTA Processing Started                ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  Packages:    {}", packages.len());
    println!("  Workdir:     {}", config.workdir);
    println!("  Output:      ./output");
    println!("  Threads:     {}", config.threads);
    println!();

    let mut last_dp: Option<DynamicPartitionState> = None;
    let mut current_version: usize = 0; // 0 = full OTA, 1+ = incremental

    for pkg in packages {
        let label = if pkg.is_full { "full OTA" } else { "incremental OTA" };
        
        println!("──────────────────────────────────────────────────");
        println!("[OTA #{}] Processing {} ({})", pkg.index, label, pkg.name);
        println!("  Source: {}", pkg.path.display());
        println!("  Target version: .{}", current_version);

        // Step 1: Extract OTA zip (only needed files, directly to workdir).
        println!("  [1/3] Extracting required files...");
        timer.start(format!("OTA #{} - Extract", pkg.index));
        extract_ota_zip(&pkg.path, workdir)
            .with_context(|| format!("extract OTA #{} from {}", pkg.index, pkg.path.display()))?;
        timer.end();

        // Step 2: Prepare source images for incremental (create symlinks/copies with .img suffix).
        if !pkg.is_full {
            println!("  [2/3] Preparing source images from version .{}...", current_version - 1);
            timer.start(format!("OTA #{} - Prepare sources", pkg.index));
            prepare_source_images(workdir, current_version - 1)?;
            timer.end();
        } else {
            println!("  [2/3] Full OTA - no source images needed.");
        }

        // Step 3: Process partitions with version tracking and error handling.
        println!("  [3/3] Processing partitions...");
        timer.start(format!("OTA #{} - Process partitions", pkg.index));
        
        let (success_count, fail_count, failed_partitions) = process_partitions_with_version(
            workdir,
            current_version,
            config,
        ).with_context(|| format!("process partitions for OTA #{}", pkg.index))?;
        
        timer.end();

        // Check results - if more than one partition failed, stop here
        if fail_count > 0 {
            log::error!(
                "OTA #{}: {} partitions succeeded, {} failed (failed: {:?})",
                pkg.index, success_count, fail_count, failed_partitions
            );
            
            if fail_count > 1 || pkg.index < packages.len() - 1 {
                println!();
                println!("  ERROR: Multiple partitions failed or not the last OTA.");
                println!("  Failed partitions: {}", failed_partitions.join(", "));
                println!("  Please fix errors before proceeding to next OTA.");
                
                // Write error log
                let error_log = workdir.join("error.log");
                let error_content = format!(
                    "OTA #{} failed partitions: {:?}\n\
                     Success: {}, Failed: {}\n\
                     Fix errors before proceeding to next OTA.\n",
                    pkg.index, failed_partitions, success_count, fail_count
                );
                fs::write(&error_log, error_content)?;
                println!("  Error log written to: {}", error_log.display());
                
                bail!("OTA #{} processing failed - fix errors before continuing", pkg.index);
            } else {
                println!("  WARNING: One partition failed, but continuing...");
            }
        }

        // Update version tracking
        println!("  {} partitions processed successfully.", success_count);

        // Track dynamic partitions BEFORE cleanup (must be before cleanup_after_processing!)
        if let Ok(script_path) = find_updater_script(workdir) {
            let script_content = fs::read_to_string(&script_path).unwrap_or_default();
            if script_content.contains("update_dynamic_partitions") {
                let op_list_path = workdir.join("dynamic_partitions_op_list");
                if op_list_path.exists() {
                    if let Ok(content) = fs::read_to_string(&op_list_path) {
                        if let Ok(dp) = crate::core::super_img::op_list::parse_op_list(&content) {
                            last_dp = Some(merge_dynamic_partitions(last_dp, dp));
                            println!("  Dynamic partitions detected — layout cached for super.img build.");
                        }
                    }
                } else {
                    println!("  WARNING: updater-script references dynamic partitions but op_list not found.");
                }
            }
        }
        
        // Clean up temporary files (transfer lists, patch files, etc.)
        // This is done AFTER dynamic partition detection to ensure op_list is available
        timer.start(format!("OTA #{} - Cleanup temp files", pkg.index));
        cleanup_after_processing(workdir)?;
        timer.end();
        
        // Clean up old versioned files
        timer.start(format!("OTA #{} - Cleanup old versions", pkg.index));
        cleanup_versioned_workdir(workdir, current_version)?;
        timer.end();

        current_version += 1;
        println!("  OTA #{} completed. Current version: .{}", pkg.index, current_version - 1);
        println!();
    }

    // Final output directory: current directory + "output"
    let final_output = Path::new("output");
    fs::create_dir_all(final_output)
        .with_context(|| format!("create output dir {}", final_output.display()))?;

    // Build super.img if dynamic partitions were detected
    if !config.no_super {
        if let Some(ref dp) = last_dp {
            println!();
            println!("  Dynamic partitions detected — building super.img...");
            timer.start("Build super.img");

            // Create symlinks for partition images so super builder can find them
            let linked = link_partition_images_for_super(workdir, current_version - 1)?;
            println!("  Linked {} partition images for super build", linked.len());

            build_super_from_batch(dp, workdir, final_output, config)?;
            timer.end();
        } else {
            println!();
            println!("  No dynamic partitions detected — skipping super.img build.");
        }
    }

    println!("══════════════════════════════════════════════════════");
    println!("  Moving final images to: {}", final_output.display());

    timer.start("Move final images");
    move_final_images(workdir, final_output)?;
    timer.end();

    // Record total batch time.
    let total_time = batch_start.elapsed();
    timer.record("Total batch time", total_time);

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Batch OTA Processing Complete               ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  Output directory: {}", final_output.display());

    // List output files.
    list_output_files(final_output);

    // Print timing summary.
    timer.print_summary();

    // Clean up workdir after successful completion.
    if workdir.exists() {
        log::info!(
            "cleaning up workdir after successful batch: {}",
            workdir.display()
        );
        if let Err(e) = fs::remove_dir_all(workdir) {
            log::warn!("failed to clean up workdir: {}", e);
        } else {
            println!("  Cleaned up workdir: {}", workdir.display());
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// OTA zip extraction - only extract needed files
// ---------------------------------------------------------------------------

/// Extract only necessary files from OTA zip to workdir.
/// Extracts: boot*, *.dat, *.dat.br, *.dat.lzma, *.transfer.list, updater-script, dynamic_partitions_op_list
fn extract_ota_zip(zip_path: &Path, workdir: &Path) -> Result<()> {
    // Ensure workdir exists
    fs::create_dir_all(workdir)
        .with_context(|| format!("create workdir {}", workdir.display()))?;

    // Build 7z command with specific file patterns
    let status = std::process::Command::new("7z")
        .arg("x")
        .arg(zip_path)
        .arg("-r")
        .arg("boot*")
        .arg("*.dat")
        .arg("*.dat.br")
        .arg("*.dat.lzma")
        .arg("*.transfer.list")
        .arg("META-INF/com/google/android/updater-script")
        .arg("dynamic_partitions_op_list")
        .arg(format!("-o{}", workdir.display()))
        .arg("-y")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .status();

    match status {
        Ok(s) if s.success() => {
            log::info!("extracted {} to {}", zip_path.display(), workdir.display());
            Ok(())
        }
        Ok(s) => {
            // 7z returns 2 if some files don't exist, which is OK for optional files
            if s.code() == Some(2) {
                log::info!("extracted {} to {} (some optional files not found)", zip_path.display(), workdir.display());
                Ok(())
            } else {
                bail!(
                    "7z exited with code {:?} while extracting {}",
                    s.code(),
                    zip_path.display()
                )
            }
        }
        Err(e) => {
            bail!(
                "failed to run 7z (is it installed and Configure in Environment Variables?): {}\n\
                 Please install 7-Zip: https://github.com/ip7z/7zip/releases/latest",
                e
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Versioned file management
// ---------------------------------------------------------------------------

/// Rollback failed version - delete new version files, keep old version
fn rollback_versioned_files(workdir: &Path, version: usize) -> Result<()> {
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        
        let filename = entry.file_name().to_string_lossy().into_owned();
        // Delete files with this version suffix, but keep dynamic_partitions_op_list
        let suffix = format!(".img.{}", version);
        if filename.ends_with(&suffix) && filename != "dynamic_partitions_op_list" {
            fs::remove_file(&path)
                .with_context(|| format!("remove failed version file {}", path.display()))?;
            log::info!("rolled back: deleted {}", path.display());
        }
    }
    
    Ok(())
}

// ---------------------------------------------------------------------------
// Workdir cleanup - keep only essential files
// ---------------------------------------------------------------------------

/// Clean up workdir after edify execution.
/// Keeps only: *.*.dat.*, *.transfer.list, *.img, boot.img.p, boot.img, updater-script
/// Note: *.img files are kept for incremental OTA chaining (will be moved to next OTA)
#[allow(dead_code)]
fn cleanup_workdir(ota_dir: &Path) -> Result<()> {
    // Allowed file patterns:
    // - *.*.dat.* (system.new.dat.br, etc.)
    // - *.transfer.list
    // - dynamic_partitions_op_list
    // - boot.img.p
    // - boot.img
    // - updater-script

    fn is_allowed_file(filename: &str) -> bool {
        // Check for *.*.dat.* pattern (e.g., system.new.dat.br)
        if filename.contains(".dat.") {
            return true;
        }
        // Check for *.transfer.list
        if filename.ends_with(".transfer.list") {
            return true;
        }
        // Check for dynamic_partitions_op_list (needed for super.img building)
        if filename == "dynamic_partitions_op_list" {
            return true;
        }
        // Check for *.img files (needed for incremental OTA chaining)
        if filename.ends_with(".img") {
            return true;
        }
        // Check for specific files
        if filename == "boot.img.p" || filename == "boot.img" || filename == "updater-script" {
            return true;
        }
        false
    }

    // Collect files to delete
    let mut files_to_delete: Vec<PathBuf> = Vec::new();
    let mut dirs_to_delete: Vec<PathBuf> = Vec::new();

    for entry in fs::read_dir(ota_dir).context("read ota_dir for cleanup")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();

        if path.is_file() {
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if !is_allowed_file(&filename_str) {
                files_to_delete.push(path);
            }
        } else if path.is_dir() {
            // Mark directories for deletion (except hidden backup dir)
            let dirname = entry.file_name();
            let dirname_str = dirname.to_string_lossy();

            if dirname_str != ".batch_backup" {
                dirs_to_delete.push(path);
            }
        }
    }

    // Delete files
    for path in files_to_delete {
        if let Err(e) = fs::remove_file(&path) {
            log::warn!("cleanup: failed to delete file {}: {}", path.display(), e);
        } else {
            log::debug!("cleanup: deleted file {}", path.display());
        }
    }

    // Delete directories recursively
    for path in dirs_to_delete {
        if let Err(e) = fs::remove_dir_all(&path) {
            log::warn!(
                "cleanup: failed to delete directory {}: {}",
                path.display(),
                e
            );
        } else {
            log::debug!("cleanup: deleted directory {}", path.display());
        }
    }

    log::info!("cleanup: workdir cleaned, only essential files retained");
    Ok(())
}

// ---------------------------------------------------------------------------
// Partition exclusion (save + restore)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn partitions_excluded_at(ota_index: usize, config: &BatchConfig) -> Vec<String> {
    let mut excluded = Vec::new();
    let exclude_set: HashSet<&String> = config.excludes.iter().collect();

    // Collect all partitions that are known from caps and excludes.
    let mut all_parts: HashSet<String> = exclude_set.iter().map(|s| (*s).clone()).collect();
    for part in config.caps.keys() {
        all_parts.insert(part.clone());
    }

    for part in &all_parts {
        if exclude_set.contains(part) {
            excluded.push(part.clone());
        } else if let Some(&cap_idx) = config.caps.get(part)
            && ota_index > cap_idx {
                excluded.push(part.clone());
            }
    }

    excluded.sort();
    excluded
}

// ---------------------------------------------------------------------------
// Edify script discovery
// ---------------------------------------------------------------------------

fn find_updater_script(workdir: &Path) -> Result<PathBuf> {
    let candidates = [
        "META-INF/com/google/android/updater-script",
        "META-INF/com/google/android/update-script",
        "META-INF/com/android/updater-script",
    ];

    for rel in &candidates {
        let full = workdir.join(rel);
        if full.exists() {
            return Ok(full);
        }
    }

    // Last resort: search for any updater-script in META-INF.
    let meta_inf = workdir.join("META-INF");
    if meta_inf.is_dir() {
        for entry in walkdir_nested(&meta_inf)? {
            let name = entry.file_name();
            if name.is_some_and(|n| n == "updater-script" || n == "update-script") {
                return Ok(entry);
            }
        }
    }

    bail!(
        "updater-script not found in {} (searched META-INF/com/google/android/)",
        workdir.display()
    )
}

/// Simple recursive directory walk to find a file.
fn walkdir_nested(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(dir).context("read dir")? {
        let entry = entry.context("dir entry")?;
        let path = entry.path();
        if path.is_dir() {
            result.extend(walkdir_nested(&path)?);
        } else {
            result.push(path);
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Partition scanning (for --list mode)
// ---------------------------------------------------------------------------

fn scan_ota_partitions(ota_path: &Path) -> Result<Vec<String>> {
    // Extract just the updater-script to stdout.
    let output = std::process::Command::new("7z")
        .arg("e")
        .arg("-so")
        .arg(ota_path)
        .arg("META-INF/com/google/android/updater-script")
        .output();

    let script_text = match output {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).to_string(),
        Ok(_) => {
            // Try alternative script paths.
            let alt_output = std::process::Command::new("7z")
                .arg("e")
                .arg("-so")
                .arg(ota_path)
                .arg("META-INF/com/android/updater-script")
                .output();
            match alt_output {
                Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).to_string(),
                _ => {
                    // Try listing and finding any script.
                    return scan_partitions_from_listing(ota_path);
                }
            }
        }
        Err(e) => {
            bail!("failed to run 7z for scanning: {}", e);
        }
    };

    Ok(extract_partition_names(&script_text))
}

/// Fallback: list zip contents and extract partition names from filenames.
fn scan_partitions_from_listing(ota_path: &Path) -> Result<Vec<String>> {
    let output = std::process::Command::new("7z")
        .arg("l")
        .arg(ota_path)
        .output()
        .context("run 7z l")?;

    let listing = String::from_utf8_lossy(&output.stdout);
    let mut partitions = HashSet::new();

    for line in listing.lines() {
        // Look for patterns like "system.transfer.list", "system.new.dat.br", etc.
        for suffix in &[
            ".transfer.list",
            ".new.dat.br",
            ".new.dat.lzma",
            ".new.dat.xz",
            ".new.dat",
            ".patch.dat",
        ] {
            if let Some(pos) = line.find(suffix) {
                // Walk backwards from pos to find the partition name.
                let before = &line[..pos];
                if let Some(name_end) = before.rfind('/') {
                    let name = &before[name_end + 1..];
                    if !name.is_empty() && name.len() < 64 {
                        partitions.insert(name.to_string());
                    }
                } else if let Some(name_start) = before.rfind(|c: char| c.is_whitespace()) {
                    let name = before[name_start..].trim();
                    if !name.is_empty() && name.len() < 64 {
                        partitions.insert(name.to_string());
                    }
                }
            }
        }
    }

    let mut result: Vec<String> = partitions.into_iter().collect();
    result.sort();
    Ok(result)
}

/// Extract partition names from an edify script's text content.
fn extract_partition_names(script: &str) -> Vec<String> {
    let mut partitions = HashSet::new();

    // Pattern 1: /by-name/PARTITION — the standard AOSP device path pattern.
    for (i, _) in script.match_indices("/by-name/") {
        let rest = &script[i + "/by-name/".len()..];
        // Partition name ends at a delimiter: quote, close-paren, comma, space, or line end.
        let end = rest.find(['"', ')', ',', ' ', '\n']).unwrap_or(rest.len());
        let name = rest[..end].trim();
        if !name.is_empty()
            && name.len() < 64
            && name.chars().all(|c| c.is_alphanumeric() || c == '_')
        {
            partitions.insert(name.to_string());
        }
    }

    // Pattern 2: bare partition names before .transfer.list / .new.dat.br / .patch.dat
    for suffix in &[
        ".transfer.list",
        ".new.dat.br",
        ".new.dat.lzma",
        ".new.dat.xz",
        ".new.dat",
        ".patch.dat",
    ] {
        for part in script.split(suffix) {
            let trimmed = part.trim_end_matches(|c: char| c.is_whitespace());
            // Find the last "word" before the suffix.
            if let Some(pos) = trimmed.rfind(['"', '(', '/', ' ']) {
                let name = trimmed[pos + 1..].trim();
                if !name.is_empty()
                    && name.len() < 64
                    && name
                        .chars()
                        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
                {
                    partitions.insert(name.to_string());
                }
            }
        }
    }

    let mut result: Vec<String> = partitions.into_iter().collect();
    result.sort();
    result
}

// ---------------------------------------------------------------------------
// Final image output
// ---------------------------------------------------------------------------

fn move_final_images(workdir: &Path, output_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(workdir).context("read final workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(ext) = path.extension()
            && ext == "img" {
                let dest = output_dir.join(entry.file_name());
                // Remove destination if exists, then move (overwrite).
                if dest.exists() {
                    fs::remove_file(&dest)
                        .with_context(|| format!("remove existing {}", dest.display()))?;
                    log::info!("overwriting: {}", dest.display());
                }
                fs::rename(&path, &dest)
                    .with_context(|| format!("move {} → {}", path.display(), dest.display()))?;
                log::info!("output: {}", dest.display());
            }
    }
    Ok(())
}

fn list_output_files(output_dir: &Path) {
    println!();
    println!("  Output files:");
    let mut files: Vec<(String, u64)> = Vec::new();
    if let Ok(entries) = fs::read_dir(output_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                files.push((entry.file_name().to_string_lossy().into_owned(), size));
            }
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));

    if files.is_empty() {
        println!("    (none)");
    } else {
        for (name, size) in &files {
            if *size > 1048576 {
                println!("    {:<32} {:.1} MB", name, *size as f64 / 1048576.0);
            } else {
                println!("    {:<32} {} bytes", name, size);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Super.img building from batch
// ---------------------------------------------------------------------------

fn build_super_from_batch(
    dp: &DynamicPartitionState,
    workdir: &Path,
    output_dir: &Path,
    _config: &BatchConfig,
) -> Result<()> {
    let super_args = SuperArgs {
        workdir: workdir.to_string_lossy().into(),
        output: output_dir.join("super.img").to_string_lossy().into(),
        dynamic_list: None,
        op_list: None,
        partitions: None,
        groups: Vec::new(),
        lp_version: "10.0".to_string(), // Default to safest version
        slots: 2,
        device_size: 0,
        metadata_size: 65536,
    };

    // We call the super module's run function directly.
    // Since it needs a DynamicPartitionState, we use the op_list path.
    // Build a temporary op_list file from the DP state and pass it.
    let op_list_content = serialize_dp_to_op_list(dp);
    let tmp_op_list = workdir.join(".batch_dynamic_op_list.txt");
    fs::write(&tmp_op_list, &op_list_content).context("write temporary op_list")?;

    let args_with_op_list = SuperArgs {
        op_list: Some(tmp_op_list.clone()),
        ..super_args
    };

    let result = crate::core::super_img::cli::run(&args_with_op_list);

    // Clean up temp file.
    let _ = fs::remove_file(&tmp_op_list);

    result
}

/// Serialize a DynamicPartitionState back to op_list text format
/// so it can be consumed by the super builder.
fn serialize_dp_to_op_list(dp: &DynamicPartitionState) -> String {
    let mut lines = Vec::new();
    lines.push("remove_all_groups".to_string());
    for g in &dp.groups {
        lines.push(format!("add_group {} {}", g.name, g.max_size));
    }
    for p in &dp.partitions {
        lines.push(format!("add {} {}", p.name, p.group_name));
        if p.size > 0 {
            lines.push(format!("resize {} {}", p.name, p.size));
        }
    }
    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn file_stem(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string())
}

/// Merge two DynamicPartitionState, keeping the latest version of each partition/group.
/// Used when processing multiple OTAs to accumulate the final partition layout.
fn merge_dynamic_partitions(
    existing: Option<DynamicPartitionState>,
    new: DynamicPartitionState,
) -> DynamicPartitionState {
    let mut result = existing.unwrap_or_else(DynamicPartitionState::new);

    // Merge groups (newer takes precedence)
    for new_group in new.groups {
        if let Some(existing) = result.groups.iter_mut().find(|g| g.name == new_group.name) {
            existing.max_size = new_group.max_size;
        } else {
            result.groups.push(new_group);
        }
    }

    // Merge partitions (newer takes precedence)
    for new_part in new.partitions {
        if let Some(existing) = result.partitions.iter_mut().find(|p| p.name == new_part.name) {
            existing.group_name = new_part.group_name;
            existing.size = new_part.size;
        } else {
            result.partitions.push(new_part);
        }
    }

    result
}

/// Create symlinks/hardlinks for partition images to make them available for super build.
/// Links {partition}.img.{version} -> {partition}.img for each partition.
fn link_partition_images_for_super(workdir: &Path, version: usize) -> Result<Vec<String>> {
    let mut linked = Vec::new();

    for entry in fs::read_dir(workdir).context("read workdir for super linking")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let filename = entry.file_name().to_string_lossy().into_owned();
        let expected_suffix = format!(".img.{version}");

        if filename.ends_with(&expected_suffix) {
            let partition = &filename[..filename.len() - expected_suffix.len()];
            let target = workdir.join(format!("{partition}.img"));

            // Remove existing target if any
            if target.exists() {
                fs::remove_file(&target)?;
            }

            // Create hard link (Windows) or symlink (Unix)
            #[cfg(windows)]
            {
                fs::copy(&path, &target).with_context(|| {
                    format!("copy {} -> {}", path.display(), target.display())
                })?;
            }
            #[cfg(not(windows))]
            {
                std::os::unix::fs::symlink(&path, &target)
                    .or_else(|_| fs::copy(&path, &target).map(|_| ()))
                    .with_context(|| {
                        format!("link/copy {} -> {}", path.display(), target.display())
                    })?;
            }

            linked.push(partition.to_string());
        }
    }

    Ok(linked)
}

// ---------------------------------------------------------------------------
// New versioned file processing functions
// ---------------------------------------------------------------------------

/// Prepare source images for incremental OTA by creating symlinks/copies
/// from versioned files (e.g., system.0.img -> system.img)
fn prepare_source_images(workdir: &Path, prev_version: usize) -> Result<()> {
    // Find all versioned .img files with the previous version suffix
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        
        let filename = entry.file_name().to_string_lossy().into_owned();
        let expected_suffix = format!(".img.{}", prev_version);
        
        if filename.ends_with(&expected_suffix) {
            // Extract partition name (e.g., "system.img.0" -> "system")
            let partition = &filename[..filename.len() - expected_suffix.len()];
            let target = workdir.join(format!("{}.img", partition));
            
            // Remove existing target if any
            if target.exists() {
                fs::remove_file(&target)?;
            }
            
            // Create hard link or copy (Windows may not support symlinks without admin)
            #[cfg(windows)]
            {
                fs::copy(&path, &target)
                    .with_context(|| format!("copy {} -> {}", path.display(), target.display()))?;
            }
            #[cfg(not(windows))]
            {
                std::os::unix::fs::symlink(&path, &target)
                    .or_else(|_| {
                        fs::copy(&path, &target).map(|_| ())
                    })
                    .with_context(|| format!("link/copy {} -> {}", path.display(), target.display()))?;
            }
            
            log::info!("prepared source: {} -> {}", path.display(), target.display());
        }
    }
    
    Ok(())
}

/// Process all partitions with version tracking and per-partition error handling
/// Returns: (success_count, fail_count, failed_partitions)
fn process_partitions_with_version(
    workdir: &Path,
    target_version: usize,
    config: &BatchConfig,
) -> Result<(usize, usize, Vec<String>)> {
    let script_path = find_updater_script(workdir)?;
    let script_content = fs::read_to_string(&script_path)
        .with_context(|| format!("read {}", script_path.display()))?;
    
    let registry = builtin_registry();
    
    // Run the edify script - this will process all partitions
    let result = run_script_with_mode(
        &script_content,
        &registry,
        &workdir.to_string_lossy(),
        config.verify,
        true, // Always offline mode
    );
    
    match result {
        Ok(_) => {
            // Script executed successfully - now rename output files to versioned names
            let (promoted, failed) = promote_files_to_version(workdir, target_version)?;
            
            // Note: cleanup_after_processing is now called AFTER dynamic partition detection
            // in the main execute_batch loop to ensure op_list is available for parsing.
            
            Ok((promoted.len(), failed.len(), failed))
        }
        Err(e) => {
            // Script failed - rollback any partial changes
            log::error!("edify script failed: {}", e);
            rollback_versioned_files(workdir, target_version)?;
            
            // Try to identify which partitions might have failed
            let failed = vec!["unknown".to_string()]; // Generic failure
            Ok((0, 1, failed))
        }
    }
}

/// Promote processed output files to versioned names
/// Returns: (promoted_partitions, failed_partitions)
fn promote_files_to_version(workdir: &Path, version: usize) -> Result<(Vec<String>, Vec<String>)> {
    let mut promoted = Vec::new();
    let mut failed = Vec::new();
    
    // Find all non-versioned .img files and rename them to versioned names
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        
        let filename = entry.file_name().to_string_lossy().into_owned();
        
        // Skip already versioned files (*.img.N) and non-.img files
        let is_versioned = filename.contains(".img.") &&
            filename.rsplitn(2, '.').next().and_then(|s| s.parse::<usize>().ok()).is_some();
        if !filename.ends_with(".img") || is_versioned {
            continue;
        }

        // Extract partition name (e.g., "system.img" -> "system")
        let partition = &filename[..filename.len() - 4];
        let versioned_name = format!("{}.img.{}", partition, version);
        let versioned_path = workdir.join(&versioned_name);
        
        // Remove old version if exists
        if versioned_path.exists() {
            if let Err(e) = fs::remove_file(&versioned_path) {
                log::warn!("failed to remove old version {}: {}", versioned_path.display(), e);
                failed.push(partition.to_string());
                continue;
            }
        }
        
        // Rename to versioned name
        if let Err(e) = fs::rename(&path, &versioned_path) {
            log::error!("failed to rename {} to {}: {}", path.display(), versioned_path.display(), e);
            failed.push(partition.to_string());
        } else {
            log::info!("promoted: {} -> {}", path.display(), versioned_path.display());
            promoted.push(partition.to_string());
        }
    }
    
    Ok((promoted, failed))
}

/// Clean up temporary files after processing
fn cleanup_after_processing(workdir: &Path) -> Result<()> {
    // Remove transfer lists, patch files, etc.
    let patterns_to_remove = [
        "*.transfer.list",
        "*.patch.dat",
        "*.new.dat",
        "*.new.dat.br",
        "*.new.dat.lzma",
        "updater-script",
        "dynamic_partitions_op_list",
        "*.img", // Non-versioned .img files (source links)
    ];
    
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        
        let filename = entry.file_name().to_string_lossy().into_owned();
        
        // Check if file matches any removal pattern
        let should_remove = patterns_to_remove.iter().any(|pattern| {
            if pattern.starts_with("*.") {
                let ext = &pattern[1..]; // Remove leading *
                filename.ends_with(ext)
            } else {
                filename == *pattern
            }
        });
        
        // But keep versioned .img files (*.img.N)
        let is_versioned_img = filename.contains(".img.") &&
            filename.rsplitn(2, '.').next().and_then(|s| s.parse::<usize>().ok()).is_some();
        
        if should_remove && !is_versioned_img {
            if let Err(e) = fs::remove_file(&path) {
                log::warn!("cleanup: failed to remove {}: {}", path.display(), e);
            } else {
                log::debug!("cleanup: removed {}", path.display());
            }
        }
    }
    
    Ok(())
}

/// Clean up workdir, keeping only versioned .img files and essential metadata
fn cleanup_versioned_workdir(workdir: &Path, current_version: usize) -> Result<()> {
    // Keep files with version <= current_version
    for entry in fs::read_dir(workdir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        
        if path.is_dir() {
            // Remove subdirectories (META-INF, etc.)
            let _ = fs::remove_dir_all(&path);
            continue;
        }
        
        let filename = entry.file_name().to_string_lossy().into_owned();
        
        // Check if it's a versioned .img file (*.img.N)
        if let Some(idx) = filename.rfind(".img.") {
            // Extract version number from after ".img."
            let version_str = &filename[idx + 5..]; // 5 = len(".img.")
            if let Ok(version) = version_str.parse::<usize>() {
                if version <= current_version {
                    // Keep this version
                    continue;
                }
            }
        }
        
        // Also keep error.log if exists
        if filename == "error.log" {
            continue;
        }
        
        // Remove everything else
        let _ = fs::remove_file(&path);
    }
    
    log::info!("workdir cleaned, kept versioned .img files up to .{}", current_version);
    Ok(())
}
