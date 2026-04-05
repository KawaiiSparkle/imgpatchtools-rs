//! CLI front-end for the `blockimg` subcommand.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};

use super::update;
use super::verify;

/// Returns the default stash directory path (cross-platform).
fn default_stash_dir() -> PathBuf {
    std::env::temp_dir().join("imgpatchtools-stash")
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
    Update {
        /// Path to the output target image file.
        target: PathBuf,

        /// Path to the transfer list file (e.g. system.transfer.list).
        transfer_list: PathBuf,

        /// Path to the new-data file (.new.dat or .new.dat.br).
        new_data: PathBuf,

        /// Path to the patch-data file (.patch.dat).
        patch_data: PathBuf,

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
        /// Path to the image file.
        file: PathBuf,

        /// Block ranges in AOSP format (e.g. "4,0,10,20,30").
        ranges: String,

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
            update::block_image_update(
                target,
                transfer_list,
                new_data,
                patch_data,
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
            let sha1 =
                update::range_sha1(file, ranges, *block_size).context("range_sha1 failed")?;
            println!("{sha1}");
            Ok(())
        }
    }
}
