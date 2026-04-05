//! Core apply-patch logic — port of AOSP `applypatch/applypatch.cpp`.
//!
//! Implements the full patch-application pipeline:
//! 1. Idempotency check (skip if target already correct).
//! 2. Source loading.
//! 3. Patch type detection (BSDIFF40 / IMGDIFF2).
//! 4. Patch application.
//! 5. Output size + SHA-1 verification.
//! 6. Atomic write (temp file + rename).

use std::io::Write;
use std::path::Path;

use anyhow::{ensure, Context, Result};

use super::bspatch;
use super::imgpatch;
use super::types::{ApplyPatchError, FileContents, PatchType};
use crate::util::hash;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Apply a bsdiff or imgdiff patch, producing a target file that is
/// bit-exact with the expected SHA-1 digest.
///
/// # Idempotency
///
/// If `target_path` already exists **and** its SHA-1 matches
/// `target_sha1`, the function returns `Ok(())` immediately without
/// reading the source or patch. This matches AOSP `applypatch.cpp`
/// behaviour.
///
/// # Atomic write
///
/// The output is first written to a temporary file in the same directory
/// as `target_path`, then atomically renamed. This guarantees that a
/// crash or power loss can never leave a partially-written target.
///
/// # Errors
///
/// Returns an error if:
/// * The patch has an unrecognised magic header.
/// * The patched output length differs from `target_size`.
/// * The patched output SHA-1 differs from `target_sha1`.
/// * Any I/O or decompression failure occurs.
pub fn apply_patch(
    source_path: &Path,
    target_path: &Path,
    target_sha1: &str,
    target_size: u64,
    patch_path: &Path,
) -> Result<()> {
    // ---- 1. Idempotency: skip if target is already correct ----
    if target_already_correct(target_path, target_sha1)? {
        log::info!(
            "target {} already has SHA1 {}, skipping",
            target_path.display(),
            target_sha1
        );
        return Ok(());
    }

    // ---- 2. Load source ----
    let source = FileContents::from_file(source_path)
        .with_context(|| format!("failed to load source {}", source_path.display()))?;
    log::info!(
        "loaded source {}: {} bytes, SHA1={}",
        source_path.display(),
        source.len(),
        source.sha1()
    );

    // ---- 3. Load patch & detect type ----
    let patch = FileContents::from_file(patch_path)
        .with_context(|| format!("failed to load patch {}", patch_path.display()))?;
    let patch_type = PatchType::detect(patch.data()).ok_or_else(|| {
        ApplyPatchError::InvalidPatchFormat(format!(
            "unrecognised magic in {}",
            patch_path.display()
        ))
    })?;
    log::info!(
        "patch {}: {} bytes, type={}",
        patch_path.display(),
        patch.len(),
        patch_type
    );

    // ---- 4. Apply ----
    let result = apply_by_type(source.data(), patch.data(), patch_type)
        .context("patch application failed")?;

    // ---- 5. Verify output ----
    verify_result(&result, target_sha1, target_size)?;

    // ---- 6. Atomic write ----
    atomic_write(target_path, &result)
        .with_context(|| format!("failed to write target {}", target_path.display()))?;

    log::info!(
        "applied {} patch → {} ({} bytes, SHA1={})",
        patch_type,
        target_path.display(),
        result.len(),
        target_sha1,
    );

    Ok(())
}

/// Verify whether a file already matches the expected SHA-1 digest.
///
/// Returns `Ok(true)` if the file at `source_path` exists and its SHA-1
/// matches `target_sha1` (case-insensitive). Returns `Ok(false)` if the
/// file does not exist or its hash differs.
///
/// This is the read-only "check" mode used by `applypatch -c` in AOSP.
pub fn check_patch(source_path: &Path, target_sha1: &str) -> Result<bool> {
    match FileContents::from_file(source_path) {
        Ok(fc) => {
            let matches = fc.verify_sha1(target_sha1);
            if matches {
                log::info!(
                    "check: {} matches expected SHA1 {}",
                    source_path.display(),
                    target_sha1
                );
            } else {
                log::info!(
                    "check: {} SHA1 {} does not match expected {}",
                    source_path.display(),
                    fc.sha1(),
                    target_sha1
                );
            }
            Ok(matches)
        }
        Err(e) => {
            log::warn!("check: cannot read {}: {e:#}", source_path.display());
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// Check whether `target_path` already has the expected SHA-1.
///
/// If the file does not exist or cannot be read, returns `Ok(false)`
/// (not an error — we simply proceed to patching).
fn target_already_correct(target_path: &Path, expected_sha1: &str) -> Result<bool> {
    if !target_path.exists() {
        return Ok(false);
    }
    match FileContents::from_file(target_path) {
        Ok(fc) => Ok(fc.verify_sha1(expected_sha1)),
        Err(e) => {
            log::debug!(
                "could not read existing target {}: {e:#}",
                target_path.display()
            );
            Ok(false)
        }
    }
}

/// Dispatch to the correct patcher based on detected type.
fn apply_by_type(source: &[u8], patch: &[u8], patch_type: PatchType) -> Result<Vec<u8>> {
    match patch_type {
        PatchType::Bsdiff => bspatch::apply_bspatch(source, patch),
        PatchType::Imgdiff => imgpatch::apply_imgpatch(source, patch),
    }
}

/// Verify the patched output against expected size and SHA-1.
fn verify_result(result: &[u8], expected_sha1: &str, expected_size: u64) -> Result<()> {
    ensure!(
        result.len() as u64 == expected_size,
        "output size mismatch: expected {expected_size}, got {}",
        result.len()
    );

    let actual_sha1 = hash::sha1_hex(result);
    if !actual_sha1.eq_ignore_ascii_case(expected_sha1) {
        return Err(ApplyPatchError::HashMismatch {
            expected: expected_sha1.to_string(),
            actual: actual_sha1,
        }
        .into());
    }

    Ok(())
}

/// Write `data` to `target` atomically via a temporary file + rename.
///
/// The temporary file is created in the same directory as `target` to
/// ensure they reside on the same filesystem (required for atomic rename).
///
/// # Cross-platform
///
/// * **Unix**: `rename(2)` is atomic and replaces the target if it exists.
/// * **Windows**: `MoveFileExW(MOVEFILE_REPLACE_EXISTING)` provides the
///   same guarantee on NTFS.
fn atomic_write(target: &Path, data: &[u8]) -> Result<()> {
    let parent = target.parent().unwrap_or_else(|| Path::new("."));

    // Ensure parent directory exists.
    if !parent.exists() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let mut tmp =
        tempfile::NamedTempFile::new_in(parent).context("failed to create temporary file")?;

    tmp.write_all(data)
        .context("failed to write to temporary file")?;
    tmp.flush().context("failed to flush temporary file")?;

    // On Unix this also calls fsync. On Windows it calls
    // FlushFileBuffers.
    tmp.as_file()
        .sync_all()
        .context("failed to sync temporary file")?;

    tmp.persist(target)
        .with_context(|| format!("failed to rename temporary file to {}", target.display()))?;

    Ok(())
}
