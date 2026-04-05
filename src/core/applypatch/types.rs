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
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let metadata = file
            .metadata()
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
