//! CLI front-end for the `blockimg` subcommand.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use super::update;
use super::verify;

/// Returns the default stash directory path (cross-platform).
fn default_stash_dir() -> PathBuf {
    std::env::temp_dir().join("imgpatchtools-stash")
}

/// Auto-detect partition files from current directory.
/// Given a partition name (e.g., "system"), finds:
/// - partition.transfer.list
/// - partition.new.dat (or .new.dat.br or .new.dat.lzma)
/// - partition.patch.dat
fn detect_partition_files(partition_name: &str) -> Result<(PathBuf, PathBuf, PathBuf)> {
    let current_dir = std::env::current_dir()?;

    // Transfer list file
    let transfer_list = current_dir.join(format!("{}.transfer.list", partition_name));
    if !transfer_list.exists() {
        anyhow::bail!("Transfer list not found: {}", transfer_list.display());
    }

    // New data file - try .new.dat.br first, then .new.dat.lzma, then .new.dat
    let new_data_br = current_dir.join(format!("{}.new.dat.br", partition_name));
    let new_data_lzma = current_dir.join(format!("{}.new.dat.lzma", partition_name));
    let new_data = current_dir.join(format!("{}.new.dat", partition_name));

    let new_data = if new_data_br.exists() {
        new_data_br
    } else if new_data_lzma.exists() {
        new_data_lzma
    } else if new_data.exists() {
        new_data
    } else {
        anyhow::bail!("New data file not found for partition: {} (tried .new.dat.br, .new.dat.lzma, .new.dat)", partition_name);
    };

    // Patch data file
    let patch_data = current_dir.join(format!("{}.patch.dat", partition_name));
    if !patch_data.exists() {
        anyhow::bail!("Patch data not found: {}", patch_data.display());
    }

    Ok((transfer_list, new_data, patch_data))
}

/// Extract partition name from target path.
/// If target is a file name without extension (e.g., "system"), use it directly.
/// If target has an extension, strip it.
fn extract_partition_name(target: &Path) -> String {
    let name = target
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "target".to_string());

    // If the name looks like a device path (contains "/" or "EMMC:"), extract the last part
    if name.contains('/') || name.contains("EMMC:") {
        name.rsplit('/')
            .next()
            .and_then(|s| s.split("by-name/").last())
            .map(|s| s.split(':').next().unwrap_or(s).to_string())
            .unwrap_or(name)
    } else {
        name
    }
}

/// Arguments for the `blockimg` subcommand.
#[derive(Args, Debug, Clone)]
pub struct BlockimgArgs {
    #[command(subcommand)]
    pub command: BlockimgCommand,
}

/// `blockimg` sub-subcommands.
#[derive(Subcommand, Debug, Clone)]
pub enum BlockimgCommand {
    /// Apply a block-based OTA update.
    /// If only target is provided, auto-detects companion files from current directory:
    /// target.transfer.list, target.new.dat(.br|.lzma), target.patch.dat
    Update {
        /// Path to the output target image file or partition name.
        target: PathBuf,

        /// Path to the transfer list file (e.g. system.transfer.list).
        /// If omitted, auto-detects from current directory.
        transfer_list: Option<PathBuf>,

        /// Path to the new-data file (.new.dat or .new.dat.br or .new.dat.lzma).
        /// If omitted, auto-detects from current directory.
        new_data: Option<PathBuf>,

        /// Path to the patch-data file (.patch.dat).
        /// If omitted, auto-detects from current directory.
        patch_data: Option<PathBuf>,

        /// Path to a separate source image (omit for full OTA or
        /// in-place incremental).
        #[arg(long)]
        source: Option<PathBuf>,

        /// Directory for temporary stash files (default: system temp dir).
        #[arg(long)]
        stash_dir: Option<PathBuf>,

        /// Path to a resume checkpoint file (last_command).
        #[arg(long)]
        resume_file: Option<PathBuf>,
    },

    /// Verify a target image against a transfer list (v4 hashes).
    Verify {
        /// Path to the target image to verify.
        target: PathBuf,

        /// Path to the transfer list file.
        transfer_list: PathBuf,
    },

    /// Compute the SHA-1 of specific block ranges in a file.
    RangeSha1 {
        /// Path to the image file or partition name.
        /// If ranges not provided, reads update-script to get ranges.
        file: PathBuf,

        /// Block ranges in AOSP format (e.g. "4,0,10,20,30").
        /// If omitted, reads from update-script.
        ranges: Option<String>,

        /// Block size in bytes.
        #[arg(long, default_value_t = 4096)]
        block_size: usize,
    },
}

/// Execute the `blockimg` subcommand.
pub fn run(args: &BlockimgArgs, verbose: bool) -> Result<()> {
    match &args.command {
        BlockimgCommand::Update {
            target,
            transfer_list,
            new_data,
            patch_data,
            source,
            stash_dir,
            resume_file,
        } => {
            let stash_dir = stash_dir.clone().unwrap_or_else(default_stash_dir);

            // Auto-detect files if not all provided
            let (transfer_list, new_data, patch_data) =
                if transfer_list.is_none() || new_data.is_none() || patch_data.is_none() {
                    let partition_name = extract_partition_name(target);
                    log::info!("Auto-detecting files for partition: {}", partition_name);
                    let (tl, nd, pd) = detect_partition_files(&partition_name)?;
                    (
                        transfer_list.clone().unwrap_or(tl),
                        new_data.clone().unwrap_or(nd),
                        patch_data.clone().unwrap_or(pd),
                    )
                } else {
                    (
                        transfer_list.clone().unwrap(),
                        new_data.clone().unwrap(),
                        patch_data.clone().unwrap(),
                    )
                };

            update::block_image_update(
                target,
                &transfer_list,
                &new_data,
                &patch_data,
                source.as_deref(),
                &stash_dir,
                verbose,
                resume_file.as_deref(),
            )
        }

        BlockimgCommand::Verify {
            target,
            transfer_list,
        } => {
            let ok =
                verify::block_image_verify(target, transfer_list).context("verification failed")?;
            if ok {
                println!("VERIFY: PASS");
                Ok(())
            } else {
                anyhow::bail!("VERIFY: FAIL — one or more hash mismatches")
            }
        }

        BlockimgCommand::RangeSha1 {
            file,
            ranges,
            block_size,
        } => {
            // If ranges not provided, try to read from update-script (along with expected SHA1)
            let (ranges, expected_sha1) = match ranges {
                Some(r) => (r.clone(), None),
                None => {
                    let partition_name = extract_partition_name(file);
                    log::info!("Reading ranges from update-script for partition: {}", partition_name);
                    let info = crate::core::edify::parser::read_range_sha1_info_from_script(&partition_name)?;
                    (info.ranges, info.expected_sha1)
                }
            };

            let file_path = if file.exists() {
                file.clone()
            } else {
                // Try to find the partition image in current directory
                let current_dir = std::env::current_dir()?;
                let img_path = current_dir.join(format!("{}.img", extract_partition_name(file)));
                if img_path.exists() {
                    img_path
                } else {
                    file.clone()
                }
            };

            let computed_sha1 =
                update::range_sha1(&file_path, &ranges, *block_size).context("range_sha1 failed")?;
            
            // Print the computed SHA1
            println!("Computed: {}", computed_sha1);
            
            // If expected SHA1 is available (from update-script), compare and show result
            if let Some(expected) = expected_sha1 {
                println!("Expected: {}", expected);
                if computed_sha1.eq_ignore_ascii_case(&expected) {
                    println!("Result: MATCH ✓");
                } else {
                    println!("Result: MISMATCH ✗");
                }
            }
            
            Ok(())
        }
    }
}
