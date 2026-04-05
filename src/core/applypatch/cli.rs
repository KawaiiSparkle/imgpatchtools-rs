//! CLI front-end for the `applypatch` subcommand — port of AOSP
//! `applypatch/applypatch_main.cpp`.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use super::apply;

/// Arguments for the `applypatch` subcommand.
#[derive(Args, Debug, Clone)]
pub struct ApplypatchArgs {
    /// Path to the source (original) file.
    pub source: PathBuf,

    /// Path to the output (patched) file.
    pub target: PathBuf,

    /// Expected SHA-1 hex digest of the target file.
    pub target_sha1: String,

    /// Expected size of the target file in bytes.
    pub target_size: u64,

    /// Path to the patch file (bsdiff or imgdiff format).
    pub patch: PathBuf,

    /// Check-only mode: verify that `source` already matches `target_sha1`
    /// without applying any patch.
    #[arg(short, long)]
    pub check: bool,
}

/// Execute the `applypatch` subcommand.
pub fn run(args: &ApplypatchArgs, verbose: bool) -> Result<()> {
    if verbose {
        log::info!("applypatch: source={}", args.source.display());
        log::info!("applypatch: target={}", args.target.display());
        log::info!("applypatch: expected SHA1={}", args.target_sha1);
        log::info!("applypatch: expected size={}", args.target_size);
        log::info!("applypatch: patch={}", args.patch.display());
        log::info!("applypatch: check_only={}", args.check);
    }

    if args.check {
        run_check(args)
    } else {
        run_apply(args)
    }
}

fn run_check(args: &ApplypatchArgs) -> Result<()> {
    let matches =
        apply::check_patch(&args.source, &args.target_sha1).context("check_patch failed")?;

    if matches {
        log::info!(
            "CHECK PASS: {} matches SHA1 {}",
            args.source.display(),
            args.target_sha1
        );
        Ok(())
    } else {
        anyhow::bail!(
            "CHECK FAIL: {} does not match SHA1 {}",
            args.source.display(),
            args.target_sha1
        )
    }
}

fn run_apply(args: &ApplypatchArgs) -> Result<()> {
    apply::apply_patch(
        &args.source,
        &args.target,
        &args.target_sha1,
        args.target_size,
        &args.patch,
    )
}
