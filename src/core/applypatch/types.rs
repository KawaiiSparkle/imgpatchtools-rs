//! Data types for the apply-patch engine — port of AOSP `applypatch.h` /
//! `applypatch.cpp` type definitions.
//!
//! # Types
//!
//! | Type               | AOSP equivalent               |
//! |--------------------|-------------------------------|
//! | [`PatchType`]      | `BSDIFF` / `IMGDIFF` enums    |
//! | [`FileContents`]   | `FileContents` struct          |
//! | [`ApplyPatchError`]| Error conditions from patching |

use std::fmt;
use std::fs::File;
use std::path::Path;

use anyhow::Context;

use crate::util::hash;

// ---------------------------------------------------------------------------
// PatchType
// ---------------------------------------------------------------------------

/// Discriminator for the patch algorithm.
///
/// Corresponds to the AOSP `BSDIFF` / `IMGDIFF` constants used in
/// `applypatch.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatchType {
    /// Standard bsdiff format — used for generic binary patches.
    Bsdiff,
    /// Android imgdiff format — optimised for compressed boot / recovery
    /// images. Splits the image into chunks (raw, deflate, gzip) and
    /// applies bsdiff per-chunk, recompressing afterwards.
    Imgdiff,
}

impl PatchType {
    /// AOSP magic bytes at the start of a bsdiff patch header.
    pub const BSDIFF_MAGIC: &[u8; 8] = b"BSDIFF40";
    /// AOSP magic bytes at the start of an imgdiff patch header.
    pub const IMGDIFF_MAGIC: &[u8; 8] = b"IMGDIFF2";

    /// Detect the patch type from the first 8 bytes of a patch file.
    ///
    /// Returns `None` if the magic does not match either known format.
    pub fn detect(header: &[u8]) -> Option<Self> {
        if header.len() < 8 {
            return None;
        }
        match &header[..8] {
            b"BSDIFF40" => Some(Self::Bsdiff),
            b"IMGDIFF2" => Some(Self::Imgdiff),
            _ => None,
        }
    }

    /// Return the canonical name as used in AOSP log messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bsdiff => "BSDIFF",
            Self::Imgdiff => "IMGDIFF",
        }
    }
}

impl fmt::Display for PatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// FileContents
// ---------------------------------------------------------------------------

/// In-memory representation of a file with a pre-computed SHA-1 digest.
///
/// Corresponds to the AOSP `FileContents` struct from `applypatch.h`.
/// The digest is computed once at construction time and cached.
#[derive(Debug, Clone)]
pub struct FileContents {
    /// Raw file bytes.
    data: Vec<u8>,
    /// Lowercase hex-encoded SHA-1 digest of `data`.
    sha1: String,
}

impl FileContents {
    /// Load a file from disk into memory and compute its SHA-1 digest.
    ///
    /// Uses `memmap2` for zero-copy reading: the file is memory-mapped,
    /// the SHA-1 is computed directly over the mapping, and then the data
    /// is copied into an owned `Vec<u8>`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or mapped.
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("failed to open {}", path.display()))?;
        let metadata = file.metadata()
            .with_context(|| format!("failed to stat {}", path.display()))?;

        if metadata.len() == 0 {
            return Ok(Self::from_bytes(Vec::new()));
        }

        // SAFETY: File opened read-only; handle kept alive until we finish
        // reading. No concurrent writes occur from this process. External
        // modification is UB per the OS mmap contract — callers must not
        // modify the file while we map it.
        let mmap = unsafe {
            memmap2::Mmap::map(&file)
                .with_context(|| format!("failed to mmap {}", path.display()))?
        };

        let sha1 = hash::sha1_hex(&mmap);
        let data = mmap[..].to_vec();

        Ok(Self { data, sha1 })
    }

    /// Construct from an already-loaded byte vector, computing the SHA-1
    /// automatically.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        let sha1 = hash::sha1_hex(&data);
        Self { data, sha1 }
    }

    /// Construct from data and a pre-computed SHA-1 hex digest.
    ///
    /// **No validation** is performed — the caller must guarantee that
    /// `sha1` is the correct digest of `data`. This fast-path is used
    /// when the hash was already computed elsewhere (e.g. range-hash).
    pub fn from_parts(data: Vec<u8>, sha1: String) -> Self {
        Self { data, sha1 }
    }

    /// Shared reference to the raw bytes.
    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consume `self` and return the owned byte vector.
    #[inline]
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    /// The cached lowercase hex-encoded SHA-1 digest.
    #[inline]
    pub fn sha1(&self) -> &str {
        &self.sha1
    }

    /// Length of the data in bytes.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the data is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Compare the cached SHA-1 digest against an expected hex string
    /// (case-insensitive).
    ///
    /// Returns `true` if they match.
    pub fn verify_sha1(&self, expected: &str) -> bool {
        self.sha1.eq_ignore_ascii_case(expected)
    }
}

// ---------------------------------------------------------------------------
// ApplyPatchError
// ---------------------------------------------------------------------------

/// Typed errors for the apply-patch pipeline.
///
/// These are designed for programmatic matching by callers; the module's
/// public functions return `anyhow::Result` wrapping these where
/// appropriate. [`thiserror`] derives `std::error::Error`, `Display`, and
/// `From` conversions automatically.
#[derive(Debug, thiserror::Error)]
pub enum ApplyPatchError {
    /// An I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The hash of the produced output does not match the expected digest.
    #[error("hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        /// The expected hex digest (from the OTA metadata).
        expected: String,
        /// The actual hex digest computed from the output.
        actual: String,
    },

    /// The patch file header or structure is invalid.
    #[error("invalid patch format: {0}")]
    InvalidPatchFormat(String),

    /// The patch application algorithm reported a failure.
    #[error("patch failed: {0}")]
    PatchFailed(String),
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // ---- PatchType --------------------------------------------------------

    #[test]
    fn detect_bsdiff() {
        let header = b"BSDIFF40extra_stuff_here";
        assert_eq!(PatchType::detect(header), Some(PatchType::Bsdiff));
    }

    #[test]
    fn detect_imgdiff() {
        let header = b"IMGDIFF2remaining_data";
        assert_eq!(PatchType::detect(header), Some(PatchType::Imgdiff));
    }

    #[test]
    fn detect_unknown() {
        let header = b"GARBAGE!and_more";
        assert_eq!(PatchType::detect(header), None);
    }

    #[test]
    fn detect_too_short() {
        assert_eq!(PatchType::detect(b"BSD"), None);
        assert_eq!(PatchType::detect(b""), None);
    }

    #[test]
    fn patch_type_display() {
        assert_eq!(PatchType::Bsdiff.to_string(), "BSDIFF");
        assert_eq!(PatchType::Imgdiff.to_string(), "IMGDIFF");
    }

    #[test]
    fn patch_type_as_str() {
        assert_eq!(PatchType::Bsdiff.as_str(), "BSDIFF");
        assert_eq!(PatchType::Imgdiff.as_str(), "IMGDIFF");
    }

    #[test]
    fn magic_constants() {
        assert_eq!(PatchType::BSDIFF_MAGIC, b"BSDIFF40");
        assert_eq!(PatchType::IMGDIFF_MAGIC, b"IMGDIFF2");
    }

    // ---- FileContents::from_bytes ----------------------------------------

    #[test]
    fn from_bytes_empty() {
        let fc = FileContents::from_bytes(Vec::new());
        assert!(fc.is_empty());
        assert_eq!(fc.len(), 0);
        // SHA-1 of empty string
        assert_eq!(fc.sha1(), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn from_bytes_abc() {
        let fc = FileContents::from_bytes(b"abc".to_vec());
        assert_eq!(fc.len(), 3);
        assert_eq!(fc.data(), b"abc");
        assert_eq!(fc.sha1(), "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn from_bytes_computes_sha1_automatically() {
        let data = vec![0xFFu8; 1024];
        let fc = FileContents::from_bytes(data.clone());
        assert_eq!(fc.sha1(), hash::sha1_hex(&data));
    }

    // ---- FileContents::from_file -----------------------------------------

    #[test]
    fn from_file_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abc").unwrap();

        let fc = FileContents::from_file(&path).unwrap();
        assert_eq!(fc.data(), b"abc");
        assert_eq!(fc.sha1(), "a9993e364706816aba3e25717850c26c9cd0d89d");
    }

    #[test]
    fn from_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        File::create(&path).unwrap();

        let fc = FileContents::from_file(&path).unwrap();
        assert!(fc.is_empty());
        assert_eq!(fc.sha1(), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn from_file_nonexistent() {
        let result = FileContents::from_file(Path::new("/no/such/file"));
        assert!(result.is_err());
    }

    #[test]
    fn from_file_large() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");
        let data: Vec<u8> = (0u8..=255).cycle().take(256 * 1024).collect();
        std::fs::write(&path, &data).unwrap();

        let fc = FileContents::from_file(&path).unwrap();
        assert_eq!(fc.len(), data.len());
        assert_eq!(fc.sha1(), hash::sha1_hex(&data));
    }

    // ---- FileContents::from_parts ----------------------------------------

    #[test]
    fn from_parts_no_validation() {
        let fc = FileContents::from_parts(
            b"hello".to_vec(),
            "fake_hash_not_checked".to_string(),
        );
        assert_eq!(fc.sha1(), "fake_hash_not_checked");
        assert_eq!(fc.data(), b"hello");
    }

    // ---- FileContents::into_data -----------------------------------------

    #[test]
    fn into_data_ownership() {
        let original = vec![1u8, 2, 3, 4, 5];
        let fc = FileContents::from_bytes(original.clone());
        let reclaimed = fc.into_data();
        assert_eq!(reclaimed, original);
    }

    // ---- FileContents::verify_sha1 ---------------------------------------

    #[test]
    fn verify_sha1_match() {
        let fc = FileContents::from_bytes(b"abc".to_vec());
        assert!(fc.verify_sha1("a9993e364706816aba3e25717850c26c9cd0d89d"));
    }

    #[test]
    fn verify_sha1_case_insensitive() {
        let fc = FileContents::from_bytes(b"abc".to_vec());
        assert!(fc.verify_sha1("A9993E364706816ABA3E25717850C26C9CD0D89D"));
    }

    #[test]
    fn verify_sha1_mismatch() {
        let fc = FileContents::from_bytes(b"abc".to_vec());
        assert!(!fc.verify_sha1("0000000000000000000000000000000000000000"));
    }

    #[test]
    fn verify_sha1_empty_expected() {
        let fc = FileContents::from_bytes(b"abc".to_vec());
        assert!(!fc.verify_sha1(""));
    }

    // ---- ApplyPatchError --------------------------------------------------

    #[test]
    fn error_io_from() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let patch_err: ApplyPatchError = io_err.into();
        assert!(matches!(patch_err, ApplyPatchError::Io(_)));
        assert!(patch_err.to_string().contains("file missing"));
    }

    #[test]
    fn error_hash_mismatch_display() {
        let err = ApplyPatchError::HashMismatch {
            expected: "aaa".to_string(),
            actual: "bbb".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("aaa"));
        assert!(msg.contains("bbb"));
        assert!(msg.contains("hash mismatch"));
    }

    #[test]
    fn error_invalid_format_display() {
        let err = ApplyPatchError::InvalidPatchFormat("bad magic".to_string());
        assert!(err.to_string().contains("bad magic"));
    }

    #[test]
    fn error_patch_failed_display() {
        let err = ApplyPatchError::PatchFailed("bspatch returned -1".to_string());
        assert!(err.to_string().contains("bspatch returned -1"));
    }

    #[test]
    fn error_is_std_error() {
        fn assert_std_error<T: std::error::Error>() {}
        assert_std_error::<ApplyPatchError>();
    }

    #[test]
    fn error_io_source_chain() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let patch_err: ApplyPatchError = io_err.into();
        // The source() should return the inner io::Error.
        let source = std::error::Error::source(&patch_err);
        assert!(source.is_some());
    }

    // ---- anyhow integration -----------------------------------------------

    #[test]
    fn error_into_anyhow() {
        let err = ApplyPatchError::PatchFailed("boom".to_string());
        let anyhow_err: anyhow::Error = err.into();
        assert!(anyhow_err.to_string().contains("boom"));
    }

    #[test]
    fn error_downcast_from_anyhow() {
        let err = ApplyPatchError::HashMismatch {
            expected: "aaa".to_string(),
            actual: "bbb".to_string(),
        };
        let anyhow_err: anyhow::Error = err.into();
        let downcasted = anyhow_err.downcast_ref::<ApplyPatchError>();
        assert!(downcasted.is_some());
        assert!(matches!(
            downcasted,
            Some(ApplyPatchError::HashMismatch { .. })
        ));
    }

    // ---- FileContents with real file round-trip ---------------------------

    #[test]
    fn file_roundtrip_verify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rt.bin");

        let data = b"The quick brown fox jumps over the lazy dog";
        let mut f = File::create(&path).unwrap();
        f.write_all(data).unwrap();
        drop(f);

        let fc = FileContents::from_file(&path).unwrap();
        let expected_sha1 = hash::sha1_hex(data);
        assert!(fc.verify_sha1(&expected_sha1));
        assert_eq!(fc.data(), data);
    }
}