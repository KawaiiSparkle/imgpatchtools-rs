//! CLI front-end for the `applypatch` subcommand — port of AOSP
//! `applypatch/applypatch_main.cpp`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;

use super::apply;

/// Arguments for the `applypatch` subcommand.
#[derive(Args, Debug, Clone)]
pub struct ApplypatchArgs {
    /// Path to the source (original) file or partition name.
    /// If update-script mode is used, this is the partition name.
    pub source: PathBuf,

    /// Path to the output (patched) file.
    /// Use "-" for in-place patching (source == target).
    pub target: PathBuf,

    /// Expected SHA-1 hex digest of the target file.
    /// If omitted and update-script mode is used, reads from script.
    pub target_sha1: Option<String>,

    /// Expected size of the target file in bytes.
    /// If omitted and update-script mode is used, reads from script.
    pub target_size: Option<u64>,

    /// Path to the patch file (bsdiff or imgdiff format).
    /// If omitted, auto-detects from current directory (patch/partition.img.p) or update-script.
    pub patch: Option<PathBuf>,

    /// Check-only mode: verify that `source` already matches `target_sha1`
    /// without applying any patch.
    #[arg(short, long)]
    pub check: bool,

    /// Read parameters from update-script instead of command line.
    /// Searches current directory or META-INF/com/google/android/ for update-script.
    #[arg(long)]
    pub from_script: bool,
}

/// Find update-script in current directory or META-INF/com/google/android/
fn find_update_script() -> Result<PathBuf> {
    let current_dir = std::env::current_dir()?;

    // Try current directory first
    let script_in_root = current_dir.join("update-script");
    if script_in_root.exists() {
        return Ok(script_in_root);
    }

    // Try META-INF/com/google/android/
    let script_in_meta = current_dir
        .join("META-INF")
        .join("com")
        .join("google")
        .join("android")
        .join("update-script");
    if script_in_meta.exists() {
        return Ok(script_in_meta);
    }

    anyhow::bail!("update-script not found in current directory or META-INF/com/google/android/")
}

/// Extract partition name from source path
fn extract_partition_name(source: &Path) -> String {
    let name = source
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "source".to_string());

    // Handle device paths like EMMC:/dev/block/.../by-name/boot
    if name.contains("EMMC:") || name.contains('/') {
        name.rsplit('/')
            .next()
            .and_then(|s| s.split("by-name/").last())
            .map(|s| s.split(':').next().unwrap_or(s).to_string())
            .unwrap_or(name)
    } else {
        name
    }
}

/// ApplyPatchInfo holds parsed information from update-script
#[derive(Debug, Clone)]
struct ApplyPatchInfo {
    source_spec: String,
    target_sha1: String,
    target_size: u64,
    source_sha1: String,
    patch_file: PathBuf,
}

/// Parse apply_patch calls from update-script for a given partition
fn parse_apply_patch_from_script(partition_name: &str) -> Result<ApplyPatchInfo> {
    let script_path = find_update_script()?;
    let content = std::fs::read_to_string(&script_path)
        .with_context(|| format!("Failed to read {}", script_path.display()))?;

    // Search for apply_patch calls in the script
    for line in content.lines() {
        let line = line.trim();

        // Skip comments and non-apply_patch lines
        if line.starts_with('#') || !line.contains("apply_patch") {
            continue;
        }

        // Check if this line contains the partition we're looking for
        if !line.contains(partition_name) && !line.contains(&partition_name.replace("_", "/")) {
            continue;
        }

        // Parse: apply_patch("source", "-", target_sha1, target_size, source_sha1, "patch/file.p")
        if let Some(info) = try_parse_apply_patch_line(line) {
            return Ok(info);
        }
    }

    anyhow::bail!(
        "apply_patch command not found for partition: {}",
        partition_name
    )
}

/// Try to parse an apply_patch line
fn try_parse_apply_patch_line(line: &str) -> Option<ApplyPatchInfo> {
    // Handle different formats:
    // apply_patch("source", "-", "sha1", size, "sha1", "patch")
    // apply_patch("source", "-", "sha1", size, "sha1:patch")

    let content = if let Some(start) = line.find("apply_patch(") {
        let args_start = start + "apply_patch(".len();
        if let Some(end) = line[args_start..].rfind(')') {
            &line[args_start..args_start + end]
        } else {
            return None;
        }
    } else {
        return None;
    };

    // Split by comma, but handle quoted strings
    let args = parse_args(content);
    if args.len() < 5 {
        return None;
    }

    let source_spec = args[0].clone();
    let target_sha1 = args[2].clone();
    let target_size: u64 = args[3].parse().ok()?;

    // Handle sha1:patch format or separate args
    let (source_sha1, patch_file) = if args.len() >= 6 {
        (args[4].clone(), PathBuf::from(&args[5]))
    } else if args[4].contains(':') {
        let parts: Vec<&str> = args[4].splitn(2, ':').collect();
        (parts[0].to_string(), PathBuf::from(parts[1]))
    } else {
        return None;
    };

    Some(ApplyPatchInfo {
        source_spec,
        target_sha1,
        target_size,
        source_sha1,
        patch_file,
    })
}

/// Parse comma-separated arguments, respecting quoted strings
fn parse_args(content: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let chars = content.chars().peekable();

    for c in chars {
        match c {
            '"' => {
                in_quotes = !in_quotes;
            }
            ',' if !in_quotes => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    // Remove surrounding quotes if present
                    let arg = if trimmed.starts_with('"') && trimmed.ends_with('"') {
                        trimmed[1..trimmed.len() - 1].to_string()
                    } else {
                        trimmed.to_string()
                    };
                    args.push(arg);
                }
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }

    // Don't forget the last argument
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        let arg = if trimmed.starts_with('"') && trimmed.ends_with('"') {
            trimmed[1..trimmed.len() - 1].to_string()
        } else {
            trimmed.to_string()
        };
        args.push(arg);
    }

    args
}

/// Resolve patch file path
fn resolve_patch_file(patch_file: &Path) -> Result<PathBuf> {
    if patch_file.exists() {
        return Ok(patch_file.to_path_buf());
    }

    let current_dir = std::env::current_dir()?;

    // Try in current directory
    let in_current = current_dir.join(patch_file);
    if in_current.exists() {
        return Ok(in_current);
    }

    // Try in patch/ subdirectory
    let in_patch_dir = current_dir
        .join("patch")
        .join(patch_file.file_name().unwrap_or_default());
    if in_patch_dir.exists() {
        return Ok(in_patch_dir);
    }

    anyhow::bail!("Patch file not found: {}", patch_file.display())
}

/// Execute the `applypatch` subcommand.
pub fn run(args: &ApplypatchArgs, verbose: bool) -> Result<()> {
    // Determine if we should read from update-script
    let use_script = args.from_script
        || (args.target_sha1.is_none() && args.target_size.is_none() && args.patch.is_none());

    if use_script {
        return run_from_script(args, verbose);
    }

    // Standard mode: all parameters provided
    let target_sha1 = args
        .target_sha1
        .clone()
        .ok_or_else(|| anyhow::anyhow!("target_sha1 required"))?;
    let target_size = args
        .target_size
        .ok_or_else(|| anyhow::anyhow!("target_size required"))?;
    let patch = args
        .patch
        .clone()
        .ok_or_else(|| anyhow::anyhow!("patch file required"))?;

    if verbose {
        log::info!("applypatch: source={}", args.source.display());
        log::info!("applypatch: target={}", args.target.display());
        log::info!("applypatch: expected SHA1={}", target_sha1);
        log::info!("applypatch: expected size={}", target_size);
        log::info!("applypatch: patch={}", patch.display());
        log::info!("applypatch: check_only={}", args.check);
    }

    if args.check {
        run_check(&args.source, &target_sha1)
    } else {
        run_apply(
            &args.source,
            &args.target,
            &target_sha1,
            target_size,
            &patch,
        )
    }
}

fn run_from_script(args: &ApplypatchArgs, verbose: bool) -> Result<()> {
    let partition_name = extract_partition_name(&args.source);
    log::info!(
        "Reading apply_patch parameters from update-script for partition: {}",
        partition_name
    );

    let info = parse_apply_patch_from_script(&partition_name)?;

    if verbose {
        log::info!("applypatch: source={}", info.source_spec);
        log::info!("applypatch: target={}", args.target.display());
        log::info!("applypatch: expected SHA1={}", info.target_sha1);
        log::info!("applypatch: expected size={}", info.target_size);
        log::info!("applypatch: patch={}", info.patch_file.display());
        log::info!("applypatch: source SHA1={}", info.source_sha1);
        log::info!("applypatch: check_only={}", args.check);
    }

    // Resolve source path
    let source_path = if args.source.exists() {
        args.source.clone()
    } else {
        // Try to find source image in current directory
        let current_dir = std::env::current_dir()?;
        let img_path = current_dir.join(format!("{}.img", partition_name));
        if img_path.exists() {
            img_path
        } else {
            PathBuf::from(&info.source_spec)
        }
    };

    // Resolve patch file
    let patch_path = resolve_patch_file(&info.patch_file)?;

    // Determine target path
    let target_path = if args.target.as_os_str() == "-" {
        source_path.clone()
    } else {
        args.target.clone()
    };

    if args.check {
        run_check(&source_path, &info.target_sha1)
    } else {
        run_apply(
            &source_path,
            &target_path,
            &info.target_sha1,
            info.target_size,
            &patch_path,
        )
    }
}

fn run_check(source: &Path, target_sha1: &str) -> Result<()> {
    let matches = apply::check_patch(source, target_sha1).context("check_patch failed")?;

    if matches {
        log::info!(
            "CHECK PASS: {} matches SHA1 {}",
            source.display(),
            target_sha1
        );
        Ok(())
    } else {
        anyhow::bail!(
            "CHECK FAIL: {} does not match SHA1 {}",
            source.display(),
            target_sha1
        )
    }
}

fn run_apply(
    source: &Path,
    target: &Path,
    target_sha1: &str,
    target_size: u64,
    patch: &Path,
) -> Result<()> {
    apply::apply_patch(source, target, target_sha1, target_size, patch)
}
