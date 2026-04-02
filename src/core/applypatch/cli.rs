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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::hash;
    use std::io::Write;

    fn to_offtin(val: i64) -> [u8; 8] {
        let magnitude = val.unsigned_abs();
        let mut buf = magnitude.to_le_bytes();
        if val < 0 {
            buf[7] |= 0x80;
        }
        buf
    }

    fn bz2_compress(data: &[u8]) -> Vec<u8> {
        let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    fn build_bsdiff_patch(source_data: &[u8], target_data: &[u8]) -> Vec<u8> {
        let add_len = source_data.len().min(target_data.len());
        let extra_len = target_data.len().saturating_sub(source_data.len());
        let mut diff = Vec::with_capacity(add_len);
        for i in 0..add_len {
            diff.push(target_data[i].wrapping_sub(source_data[i]));
        }
        let extra: Vec<u8> = if extra_len > 0 {
            target_data[add_len..].to_vec()
        } else {
            Vec::new()
        };
        let mut ctrl = Vec::new();
        ctrl.extend_from_slice(&to_offtin(add_len as i64));
        ctrl.extend_from_slice(&to_offtin(extra_len as i64));
        ctrl.extend_from_slice(&to_offtin(0));
        let ctrl_c = bz2_compress(&ctrl);
        let diff_c = bz2_compress(&diff);
        let extra_c = bz2_compress(&extra);
        let mut p = Vec::new();
        p.extend_from_slice(b"BSDIFF40");
        p.extend_from_slice(&to_offtin(ctrl_c.len() as i64));
        p.extend_from_slice(&to_offtin(diff_c.len() as i64));
        p.extend_from_slice(&to_offtin(target_data.len() as i64));
        p.extend_from_slice(&ctrl_c);
        p.extend_from_slice(&diff_c);
        p.extend_from_slice(&extra_c);
        p
    }

    fn write_temp(dir: &tempfile::TempDir, name: &str, data: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, data).unwrap();
        path
    }

    fn make_args(
        dir: &tempfile::TempDir,
        source_data: &[u8],
        target_data: &[u8],
        check: bool,
    ) -> (ApplypatchArgs, PathBuf) {
        let patch_data = build_bsdiff_patch(source_data, target_data);
        let source_path = write_temp(dir, "source.bin", source_data);
        let patch_path = write_temp(dir, "patch.bin", &patch_data);
        let target_path = dir.path().join("target.bin");
        let target_sha1 = hash::sha1_hex(target_data);
        let target_size = target_data.len() as u64;

        let args = ApplypatchArgs {
            source: source_path,
            target: target_path.clone(),
            target_sha1,
            target_size,
            patch: patch_path,
            check,
        };
        (args, target_path)
    }

    #[test]
    fn cli_run_apply_basic() {
        let dir = tempfile::tempdir().unwrap();
        let source = b"cli source";
        let target = b"cli target";
        let (args, target_path) = make_args(&dir, source, target, false);

        run(&args, false).unwrap();

        assert_eq!(std::fs::read(&target_path).unwrap(), target);
    }

    #[test]
    fn cli_run_apply_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let source = b"src";
        let target = b"tgt";
        let (args, target_path) = make_args(&dir, source, target, false);

        std::fs::write(&target_path, target).unwrap();
        run(&args, false).unwrap();
        assert_eq!(std::fs::read(&target_path).unwrap(), target);
    }

    #[test]
    fn cli_run_check_pass() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"already patched";
        let source_path = write_temp(&dir, "source.bin", data);

        let args = ApplypatchArgs {
            source: source_path,
            target: dir.path().join("dummy"),
            target_sha1: hash::sha1_hex(data),
            target_size: data.len() as u64,
            patch: dir.path().join("dummy.patch"),
            check: true,
        };

        run(&args, false).unwrap();
    }

    #[test]
    fn cli_run_check_fail() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = write_temp(&dir, "source.bin", b"wrong data");

        let args = ApplypatchArgs {
            source: source_path,
            target: dir.path().join("dummy"),
            target_sha1: "0000000000000000000000000000000000000000".to_string(),
            target_size: 0,
            patch: dir.path().join("dummy.patch"),
            check: true,
        };

        assert!(run(&args, false).is_err());
    }

    #[test]
    fn cli_run_bad_patch_fails() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = write_temp(&dir, "source.bin", b"hello");
        let patch_path = write_temp(&dir, "patch.bin", b"GARBAGE");

        let args = ApplypatchArgs {
            source: source_path,
            target: dir.path().join("target.bin"),
            target_sha1: "abc".to_string(),
            target_size: 5,
            patch: patch_path,
            check: false,
        };

        assert!(run(&args, false).is_err());
    }

    #[test]
    fn cli_run_verbose_does_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let source = b"v_src";
        let target = b"v_tgt";
        let (args, _) = make_args(&dir, source, target, false);

        run(&args, true).unwrap();
    }
}
