//! CLI front-end for the `applypatch` subcommand — port of AOSP
//! `applypatch/applypatch_main.cpp`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;

use super::apply;
use crate::core::ota::{self, ApplyPatchInfo};

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

    /// Read parameters (and the patch file) from an OTA zip package instead of
    /// the filesystem. The updater-script is located and parsed, and the patch
    /// referenced by `apply_patch` is extracted from the zip — no external
    /// unzip tool is required.
    #[arg(long, value_name = "OTA_ZIP")]
    pub ota: Option<PathBuf>,
}

/// Extract partition name from source path.
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

/// Resolve patch file path.
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

/// Extract a patch from an OTA zip, trying `patch/<name>` then `<name>`.
/// Returns the filesystem path of the extracted patch.
fn extract_patch_from_zip(ota_zip: &Path, patch_file: &Path, workdir: &Path) -> Result<PathBuf> {
    let name = patch_file
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .context("patch file has no name")?;

    let dest = workdir.join(&name);

    for candidate in [format!("patch/{name}"), name.clone()] {
        if ota::extract_from_zip(ota_zip, &candidate, &dest).is_ok() {
            return Ok(dest);
        }
    }

    anyhow::bail!(
        "patch entry not found in {} (tried patch/{} and {})",
        ota_zip.display(),
        name,
        name
    )
}

/// Execute the `applypatch` subcommand.
pub fn run(args: &ApplypatchArgs, verbose: bool) -> Result<()> {
    // Determine if we should read from update-script.
    let use_script = args.from_script
        || args.ota.is_some()
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

    // Locate and read the script: from an OTA zip if requested, else from disk.
    let info: ApplyPatchInfo = if let Some(ota_zip) = &args.ota {
        let script = ota::read_update_script_from_zip(ota_zip)?;
        ota::parse_apply_patch(&script, &partition_name)?
    } else {
        ota::parse_apply_patch_from_script(&partition_name)?
    };

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

    // Resolve patch file: extract from zip when an OTA package was given.
    let patch_path = if let Some(ota_zip) = &args.ota {
        let workdir = std::env::temp_dir().join("imgpatchtools-applypatch");
        std::fs::create_dir_all(&workdir)
            .with_context(|| format!("create {}", workdir.display()))?;
        extract_patch_from_zip(ota_zip, &info.patch_file, &workdir)?
    } else {
        resolve_patch_file(&info.patch_file)?
    };

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
