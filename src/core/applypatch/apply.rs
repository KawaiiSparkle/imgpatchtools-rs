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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as IoWrite;

    // ---- Test helpers (duplicated from bspatch tests for isolation) --------

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

    /// Build a bsdiff patch: `source + diff = target`.
    fn build_bsdiff_patch(source: &[u8], target: &[u8]) -> Vec<u8> {
        let add_len = source.len().min(target.len());
        let extra_len = target.len().saturating_sub(source.len());

        let mut diff = Vec::with_capacity(add_len);
        for i in 0..add_len {
            diff.push(target[i].wrapping_sub(source[i]));
        }
        let extra: Vec<u8> = if extra_len > 0 {
            target[add_len..].to_vec()
        } else {
            Vec::new()
        };

        let mut ctrl_raw = Vec::new();
        ctrl_raw.extend_from_slice(&to_offtin(add_len as i64));
        ctrl_raw.extend_from_slice(&to_offtin(extra_len as i64));
        ctrl_raw.extend_from_slice(&to_offtin(0));

        let ctrl_comp = bz2_compress(&ctrl_raw);
        let diff_comp = bz2_compress(&diff);
        let extra_comp = bz2_compress(&extra);

        let mut patch = Vec::new();
        patch.extend_from_slice(b"BSDIFF40");
        patch.extend_from_slice(&to_offtin(ctrl_comp.len() as i64));
        patch.extend_from_slice(&to_offtin(diff_comp.len() as i64));
        patch.extend_from_slice(&to_offtin(target.len() as i64));
        patch.extend_from_slice(&ctrl_comp);
        patch.extend_from_slice(&diff_comp);
        patch.extend_from_slice(&extra_comp);
        patch
    }

    /// Write bytes to a file inside a temp directory, returning the dir
    /// handle and path.
    fn write_temp(dir: &tempfile::TempDir, name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, data).unwrap();
        path
    }

    // ---- apply_patch ------------------------------------------------------

    #[test]
    fn apply_patch_basic() {
        let dir = tempfile::tempdir().unwrap();

        let source_data = b"Hello, World!";
        let target_data = b"Hello, Rust!!";
        let patch_data = build_bsdiff_patch(source_data, target_data);

        let source_path = write_temp(&dir, "source.bin", source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        let target_path = dir.path().join("target.bin");

        let target_sha1 = hash::sha1_hex(target_data);
        let target_size = target_data.len() as u64;

        apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            target_size,
            &patch_path,
        )
        .unwrap();

        let result = std::fs::read(&target_path).unwrap();
        assert_eq!(result, target_data);
    }

    #[test]
    fn apply_patch_idempotent() {
        let dir = tempfile::tempdir().unwrap();

        let source_data = b"source";
        let target_data = b"target";
        let patch_data = build_bsdiff_patch(source_data, target_data);

        let source_path = write_temp(&dir, "source.bin", source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        let target_path = write_temp(&dir, "target.bin", target_data);

        let target_sha1 = hash::sha1_hex(target_data);
        let target_size = target_data.len() as u64;

        // Target already correct — should succeed without touching it.
        apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            target_size,
            &patch_path,
        )
        .unwrap();

        // File should still contain the same data.
        assert_eq!(std::fs::read(&target_path).unwrap(), target_data);
    }

    #[test]
    fn apply_patch_size_mismatch_fails() {
        let dir = tempfile::tempdir().unwrap();

        let source_data = b"hello";
        let target_data = b"world";
        let patch_data = build_bsdiff_patch(source_data, target_data);

        let source_path = write_temp(&dir, "source.bin", source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        let target_path = dir.path().join("target.bin");

        let target_sha1 = hash::sha1_hex(target_data);
        let wrong_size = 999u64; // intentionally wrong

        let result = apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            wrong_size,
            &patch_path,
        );
        assert!(result.is_err());
        assert!(!target_path.exists()); // should not have written anything
    }

    #[test]
    fn apply_patch_hash_mismatch_fails() {
        let dir = tempfile::tempdir().unwrap();

        let source_data = b"hello";
        let target_data = b"world";
        let patch_data = build_bsdiff_patch(source_data, target_data);

        let source_path = write_temp(&dir, "source.bin", source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        let target_path = dir.path().join("target.bin");

        let wrong_sha1 = "0000000000000000000000000000000000000000";
        let target_size = target_data.len() as u64;

        let result = apply_patch(
            &source_path,
            &target_path,
            wrong_sha1,
            target_size,
            &patch_path,
        );
        assert!(result.is_err());
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(err_msg.contains("hash mismatch"));
    }

    #[test]
    fn apply_patch_bad_patch_fails() {
        let dir = tempfile::tempdir().unwrap();

        let source_path = write_temp(&dir, "source.bin", b"hello");
        let patch_path = write_temp(&dir, "patch.bin", b"NOT_A_PATCH");
        let target_path = dir.path().join("target.bin");

        let result = apply_patch(&source_path, &target_path, "abc", 5, &patch_path);
        assert!(result.is_err());
    }

    #[test]
    fn apply_patch_missing_source_fails() {
        let dir = tempfile::tempdir().unwrap();

        let patch_path = write_temp(&dir, "patch.bin", b"BSDIFF40");
        let target_path = dir.path().join("target.bin");
        let source_path = dir.path().join("nonexistent.bin");

        let result = apply_patch(&source_path, &target_path, "abc", 5, &patch_path);
        assert!(result.is_err());
    }

    #[test]
    fn apply_patch_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();

        let source_data = b"AAAA";
        let target_data = b"BBBB";
        let patch_data = build_bsdiff_patch(source_data, target_data);

        let source_path = write_temp(&dir, "source.bin", source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        // Target is nested in a non-existent subdirectory.
        let target_path = dir.path().join("sub").join("dir").join("target.bin");

        let target_sha1 = hash::sha1_hex(target_data);
        let target_size = target_data.len() as u64;

        apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            target_size,
            &patch_path,
        )
        .unwrap();

        assert_eq!(std::fs::read(&target_path).unwrap(), target_data);
    }

    #[test]
    fn apply_patch_overwrites_wrong_target() {
        let dir = tempfile::tempdir().unwrap();

        let source_data = b"original";
        let target_data = b"patched!";
        let patch_data = build_bsdiff_patch(source_data, target_data);

        let source_path = write_temp(&dir, "source.bin", source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        // Pre-populate target with wrong content.
        let target_path = write_temp(&dir, "target.bin", b"wrong_content");

        let target_sha1 = hash::sha1_hex(target_data);
        let target_size = target_data.len() as u64;

        apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            target_size,
            &patch_path,
        )
        .unwrap();

        assert_eq!(std::fs::read(&target_path).unwrap(), target_data);
    }

    // ---- check_patch ------------------------------------------------------

    #[test]
    fn check_patch_matching() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"hello world";
        let path = write_temp(&dir, "file.bin", data);
        let sha1 = hash::sha1_hex(data);

        assert!(check_patch(&path, &sha1).unwrap());
    }

    #[test]
    fn check_patch_not_matching() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_temp(&dir, "file.bin", b"hello");
        let wrong = "0000000000000000000000000000000000000000";

        assert!(!check_patch(&path, wrong).unwrap());
    }

    #[test]
    fn check_patch_nonexistent() {
        let path = std::path::PathBuf::from("/no/such/file.bin");
        assert!(!check_patch(&path, "abc").unwrap());
    }

    #[test]
    fn check_patch_case_insensitive() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"test";
        let path = write_temp(&dir, "file.bin", data);
        let sha1_upper = hash::sha1_hex(data).to_ascii_uppercase();

        assert!(check_patch(&path, &sha1_upper).unwrap());
    }

    // ---- target_already_correct -------------------------------------------

    #[test]
    fn target_correct_true() {
        let dir = tempfile::tempdir().unwrap();
        let data = b"correct";
        let path = write_temp(&dir, "target.bin", data);
        let sha1 = hash::sha1_hex(data);

        assert!(target_already_correct(&path, &sha1).unwrap());
    }

    #[test]
    fn target_correct_false_wrong_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_temp(&dir, "target.bin", b"wrong");

        assert!(!target_already_correct(&path, "0000").unwrap());
    }

    #[test]
    fn target_correct_false_missing() {
        let path = std::path::PathBuf::from("/no/such/file");
        assert!(!target_already_correct(&path, "abc").unwrap());
    }

    // ---- verify_result ----------------------------------------------------

    #[test]
    fn verify_result_ok() {
        let data = b"hello";
        let sha1 = hash::sha1_hex(data);
        verify_result(data, &sha1, data.len() as u64).unwrap();
    }

    #[test]
    fn verify_result_size_mismatch() {
        let data = b"hello";
        let sha1 = hash::sha1_hex(data);
        assert!(verify_result(data, &sha1, 999).is_err());
    }

    #[test]
    fn verify_result_hash_mismatch() {
        let data = b"hello";
        assert!(verify_result(data, "bad_hash", 5).is_err());
    }

    // ---- atomic_write -----------------------------------------------------

    #[test]
    fn atomic_write_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        let data = b"atomic data";

        atomic_write(&path, data).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), data);
    }

    #[test]
    fn atomic_write_overwrites() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.bin");
        std::fs::write(&path, b"old").unwrap();

        atomic_write(&path, b"new").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"new");
    }

    #[test]
    fn atomic_write_creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a").join("b").join("out.bin");

        atomic_write(&path, b"nested").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"nested");
    }

    // ---- Full integration -------------------------------------------------

    #[test]
    fn full_pipeline_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        // Generate non-trivial data.
        let source_data: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();
        let target_data: Vec<u8> = (0..4096).map(|i| ((i * 7 + 13) % 251) as u8).collect();
        let patch_data = build_bsdiff_patch(&source_data, &target_data);

        let source_path = write_temp(&dir, "source.bin", &source_data);
        let patch_path = write_temp(&dir, "patch.bin", &patch_data);
        let target_path = dir.path().join("target.bin");

        let target_sha1 = hash::sha1_hex(&target_data);
        let target_size = target_data.len() as u64;

        // First application.
        apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            target_size,
            &patch_path,
        )
        .unwrap();

        let written = std::fs::read(&target_path).unwrap();
        assert_eq!(written, target_data);

        // Second application (idempotent).
        apply_patch(
            &source_path,
            &target_path,
            &target_sha1,
            target_size,
            &patch_path,
        )
        .unwrap();

        // Check mode.
        assert!(check_patch(&target_path, &target_sha1).unwrap());
        assert!(!check_patch(&source_path, &target_sha1).unwrap());
    }
}
