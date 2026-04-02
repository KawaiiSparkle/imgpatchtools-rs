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

use anyhow::{bail, Context, Result};

use crate::core::edify::functions::{builtin_registry, run_script_offline};
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

    // Create top-level workdir.
    fs::create_dir_all(workdir).with_context(|| format!("create workdir {}", workdir.display()))?;

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

    let mut prev_workdir: Option<PathBuf> = None;
    let mut last_dp: Option<DynamicPartitionState> = None;

    for pkg in packages {
        let ota_dir = workdir.join(format!("ota_{:03}", pkg.index));
        let label = if pkg.is_full {
            "full OTA"
        } else {
            "incremental OTA"
        };

        println!("──────────────────────────────────────────────────");
        println!("[OTA #{}] Processing {} ({})", pkg.index, label, pkg.name);
        println!("  Source: {}", pkg.path.display());
        println!("  Workdir: {}", ota_dir.display());

        // Step 1: Extract OTA zip.
        println!("  [1/4] Extracting...");
        extract_ota_zip(&pkg.path, &ota_dir)
            .with_context(|| format!("extract OTA #{} from {}", pkg.index, pkg.path.display()))?;

        // Step 2: Copy source images from previous OTA (for incremental).
        if let Some(ref prev) = prev_workdir {
            println!(
                "  [2/4] Copying source images from OTA #{}...",
                pkg.index - 1
            );
            copy_source_images(prev, &ota_dir)?;
        } else {
            println!("  [2/4] No previous OTA (full package), skipping copy.");
        }

        // Step 3: Save partitions that should be excluded at this step.
        let excluded = partitions_excluded_at(pkg.index, config);
        if !excluded.is_empty() {
            println!("  [3/4] Excluding partitions: {}", excluded.join(", "));
            save_and_exclude_partitions(&excluded, &ota_dir)?;
        } else {
            println!("  [3/4] No partitions excluded at this step.");
        }

        // Step 4: Run edify script.
        println!("  [4/4] Running edify script...");
        let script_path = find_updater_script(&ota_dir)?;

        let script_content = fs::read_to_string(&script_path)
            .with_context(|| format!("read {}", script_path.display()))?;

        // Pre-scan: check if script references .dat.br or .dat.lzma files.
        let has_compressed_data =
            script_content.contains(".dat.br") || script_content.contains(".dat.lzma");

        let offline = has_compressed_data || !pkg.is_full;
        let registry = builtin_registry();
        let result = run_script_offline(
            &script_content,
            &registry,
            &ota_dir.to_string_lossy(),
            offline,
        )
        .with_context(|| format!("edify execution for OTA #{}", pkg.index))?;

        // Restore excluded partitions.
        if !excluded.is_empty() {
            restore_excluded_partitions(&excluded, &ota_dir)?;
        }

        // Run auto recovery patch (recovery-from-boot.p → recovery.img).
        crate::core::edify::cli::auto_patch_recovery(&ota_dir.to_string_lossy());

        // Track dynamic partitions.
        if let Some(ref dp) = result.dynamic_partitions {
            last_dp = Some(dp.clone());
        }

        prev_workdir = Some(ota_dir);
        println!("  OTA #{} completed successfully.", pkg.index);
        println!();
    }

    // Copy final images to output directory.
    let final_workdir = prev_workdir.as_ref().context("no OTA packages processed")?;
    let output_dir = Path::new(&config.output_dir);
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create output dir {}", output_dir.display()))?;

    println!("══════════════════════════════════════════════════════");
    println!("  Copying final images to: {}", output_dir.display());

    copy_final_images(final_workdir, output_dir)?;

    // Build super.img if dynamic partitions were detected.
    if !config.no_super {
        if let Some(ref dp) = last_dp {
            println!();
            println!("  Dynamic partitions detected — building super.img...");
            build_super_from_batch(dp, final_workdir, output_dir, config)?;
        } else {
            println!();
            println!("  No dynamic partitions detected — skipping super.img build.");
        }
    }

    println!();
    println!("╔══════════════════════════════════════════════════════╗");
    println!("║          Batch OTA Processing Complete               ║");
    println!("╚══════════════════════════════════════════════════════╝");
    println!();
    println!("  Output directory: {}", output_dir.display());

    // List output files.
    list_output_files(output_dir);

    Ok(())
}

// ---------------------------------------------------------------------------
// OTA zip extraction
// ---------------------------------------------------------------------------

fn extract_ota_zip(zip_path: &Path, dest_dir: &Path) -> Result<()> {
    // Try 7z first (handles various zip formats including Android OTA).
    let status = std::process::Command::new("7z")
        .arg("x")
        .arg(zip_path)
        .arg(format!("-o{}", dest_dir.display()))
        .arg("-y")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match status {
        Ok(s) if s.success() => {
            log::info!("extracted {} to {}", zip_path.display(), dest_dir.display());
            Ok(())
        }
        Ok(s) => {
            bail!(
                "7z exited with code {:?} while extracting {}",
                s.code(),
                zip_path.display()
            )
        }
        Err(e) => {
            bail!(
                "failed to run 7z (is it installed?): {}\n\
                 Please install 7-Zip: https://www.7-zip.org/",
                e
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Source image copying (for incremental OTAs)
// ---------------------------------------------------------------------------

fn copy_source_images(prev_dir: &Path, ota_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(prev_dir).context("read previous workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        // Only copy .img files.
        if path.extension().and_then(|e| e.to_str()) == Some("img") {
            let dest = ota_dir.join(entry.file_name());
            if !dest.exists() {
                fs::copy(&path, &dest)
                    .with_context(|| format!("copy {} → {}", path.display(), dest.display()))?;
                log::info!("copied source: {} → {}", path.display(), dest.display());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Partition exclusion (save + restore)
// ---------------------------------------------------------------------------

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
        } else if let Some(&cap_idx) = config.caps.get(part) {
            if ota_index > cap_idx {
                excluded.push(part.clone());
            }
        }
    }

    excluded.sort();
    excluded
}

fn save_and_exclude_partitions(excluded: &[String], ota_dir: &Path) -> Result<()> {
    let backup_dir = ota_dir.join(".batch_backup");
    fs::create_dir_all(&backup_dir).context("create backup dir")?;

    for part in excluded {
        let img_path = ota_dir.join(format!("{}.img", part));
        let bak_path = backup_dir.join(format!("{}.img", part));
        if img_path.exists() {
            fs::copy(&img_path, &bak_path)
                .with_context(|| format!("backup {}", img_path.display()))?;
            log::info!("backed up {}.img for exclusion", part);
        }
    }
    Ok(())
}

fn restore_excluded_partitions(excluded: &[String], ota_dir: &Path) -> Result<()> {
    let backup_dir = ota_dir.join(".batch_backup");

    for part in excluded {
        let img_path = ota_dir.join(format!("{}.img", part));
        let bak_path = backup_dir.join(format!("{}.img", part));
        if bak_path.exists() {
            fs::copy(&bak_path, &img_path)
                .with_context(|| format!("restore {}", img_path.display()))?;
            log::info!("restored {}.img after exclusion", part);
        }
    }

    // Clean up backup directory.
    let _ = fs::remove_dir_all(&backup_dir);
    Ok(())
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
        let end = rest
            .find(|c: char| c == '"' || c == ')' || c == ',' || c == ' ' || c == '\n')
            .unwrap_or(rest.len());
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
            if let Some(pos) = trimmed.rfind(|c: char| c == '"' || c == '(' || c == '/' || c == ' ')
            {
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

fn copy_final_images(workdir: &Path, output_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(workdir).context("read final workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if let Some(ext) = path.extension() {
            if ext == "img" {
                let dest = output_dir.join(entry.file_name());
                // Copy only if newer or doesn't exist.
                if !dest.exists() {
                    fs::copy(&path, &dest)
                        .with_context(|| format!("copy {} → {}", path.display(), dest.display()))?;
                    log::info!("output: {}", dest.display());
                } else {
                    log::info!("skip (exists): {}", dest.display());
                }
            }
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
    config: &BatchConfig,
) -> Result<()> {
    let super_args = SuperArgs {
        workdir: workdir.to_string_lossy().into(),
        output: output_dir.join("super.img").to_string_lossy().into(),
        dynamic_list: None,
        op_list: None,
        partitions: None,
        groups: Vec::new(),
        android_version: config.android_version.clone(),
        slots: 2,
        device_size: 0,
        metadata_size: 65536,
        format: config.format.clone(),
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
