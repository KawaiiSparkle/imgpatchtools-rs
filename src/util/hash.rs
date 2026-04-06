//! Cryptographic hash utilities — SHA-1 and SHA-256.
//!
//! Provides functions for hashing byte slices, files (via `memmap2`), and
//! block-range–selected regions of memory-mapped images. The range-based
//! hashers mirror the AOSP `blockimg.cpp` `HashBlocks` / `range_sha1`
//! logic exactly: blocks are fed to the digest in range-set order, one
//! contiguous range at a time, to produce a single hex-encoded digest.
//!
//! Parallel batch hashing of independent range-sets is supported via
//! [`rayon`].

use std::fs::File;
use std::path::Path;

use anyhow::{ensure, Context, Result};
use rayon::prelude::*;
use sha1::digest::Digest;
use sha1::Sha1;
use sha2::Sha256;

use crate::util::rangeset::RangeSet;

// ---------------------------------------------------------------------------
// Public type re-export (used by callers that want to name the enum)
// ---------------------------------------------------------------------------

/// Supported hash algorithm selector.
///
/// Used by the generic [`hash_ranges`] helper so callers can choose the
/// algorithm at runtime (e.g. based on transfer-list version).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    /// SHA-1 — used by transfer-list v3.
    Sha1,
    /// SHA-256 — used by transfer-list v4+.
    Sha256,
}

// ---------------------------------------------------------------------------
// Byte-slice hashers
// ---------------------------------------------------------------------------

/// Compute the SHA-1 digest of `data` and return it as a lowercase hex string.
///
/// Corresponds to the simple `SHA1()` call in AOSP.
#[inline]
pub fn sha1_hex(data: &[u8]) -> String {
    hex_digest::<Sha1>(data)
}

/// Compute the SHA-256 digest of `data` and return it as a lowercase hex
/// string.
#[inline]
pub fn sha256_hex(data: &[u8]) -> String {
    hex_digest::<Sha256>(data)
}

// ---------------------------------------------------------------------------
// File hashers (memmap2)
// ---------------------------------------------------------------------------

/// Compute the SHA-1 digest of a file, using memory-mapping for efficiency.
///
/// Empty files are handled without mapping (returns the SHA-1 of `b""`).
pub fn sha1_file(path: &Path) -> Result<String> {
    hash_file::<Sha1>(path)
}

/// Compute the SHA-256 digest of a file, using memory-mapping for efficiency.
///
/// Empty files are handled without mapping (returns the SHA-256 of `b""`).
pub fn sha256_file(path: &Path) -> Result<String> {
    hash_file::<Sha256>(path)
}

// ---------------------------------------------------------------------------
// Range-based hashers (AOSP HashBlocks equivalent)
// ---------------------------------------------------------------------------

/// Compute the SHA-1 digest over the blocks selected by `ranges`.
///
/// This is the **core hashing primitive** used throughout the block-image
/// engine and corresponds to AOSP `blockimg.cpp`'s `range_sha1` /
/// `HashBlocks` logic:
///
/// 1. For each `(start, end)` pair in `ranges` **in order**:
///    - Slice `data[start * block_size .. end * block_size]`
///    - Feed the slice into the running SHA-1 context.
/// 2. Finalize and return the lowercase hex digest.
///
/// # Errors
///
/// Returns an error if any range extends beyond `data.len()`.
pub fn sha1_ranges(data: &[u8], ranges: &RangeSet, block_size: usize) -> Result<String> {
    hash_ranges::<Sha1>(data, ranges, block_size)
}

/// Compute the SHA-256 digest over the blocks selected by `ranges`.
///
/// Identical to [`sha1_ranges`] but using SHA-256. Required by transfer-list
/// v4 and later.
pub fn sha256_ranges(data: &[u8], ranges: &RangeSet, block_size: usize) -> Result<String> {
    hash_ranges::<Sha256>(data, ranges, block_size)
}

/// Compute a hash over `ranges` using whichever algorithm is specified.
///
/// Convenience wrapper that selects between [`sha1_ranges`] and
/// [`sha256_ranges`] at runtime.
pub fn hash_ranges_by_algorithm(
    data: &[u8],
    ranges: &RangeSet,
    block_size: usize,
    algo: HashAlgorithm,
) -> Result<String> {
    match algo {
        HashAlgorithm::Sha1 => sha1_ranges(data, ranges, block_size),
        HashAlgorithm::Sha256 => sha256_ranges(data, ranges, block_size),
    }
}

// ---------------------------------------------------------------------------
// Verification helpers
// ---------------------------------------------------------------------------

/// Compute SHA-1 of `data` and compare against `expected_hex` (case-
/// insensitive).
///
/// Returns `true` if and only if the digests match.
pub fn verify_sha1(data: &[u8], expected_hex: &str) -> bool {
    sha1_hex(data).eq_ignore_ascii_case(expected_hex)
}

/// Compute SHA-256 of `data` and compare against `expected_hex` (case-
/// insensitive).
pub fn verify_sha256(data: &[u8], expected_hex: &str) -> bool {
    sha256_hex(data).eq_ignore_ascii_case(expected_hex)
}

/// Compute SHA-1 of the blocks selected by `ranges` and compare against
/// `expected_hex`.
pub fn verify_sha1_ranges(
    data: &[u8],
    ranges: &RangeSet,
    block_size: usize,
    expected_hex: &str,
) -> Result<bool> {
    let actual = sha1_ranges(data, ranges, block_size)?;
    Ok(actual.eq_ignore_ascii_case(expected_hex))
}

/// Compute SHA-256 of the blocks selected by `ranges` and compare against
/// `expected_hex`.
pub fn verify_sha256_ranges(
    data: &[u8],
    ranges: &RangeSet,
    block_size: usize,
    expected_hex: &str,
) -> Result<bool> {
    let actual = sha256_ranges(data, ranges, block_size)?;
    Ok(actual.eq_ignore_ascii_case(expected_hex))
}

// ---------------------------------------------------------------------------
// Parallel batch hashing
// ---------------------------------------------------------------------------

/// Compute SHA-1 digests for multiple independent range-sets in parallel.
///
/// Uses [`rayon`] to distribute work across CPU cores. Each range-set
/// produces one hex digest. The order of the returned `Vec` matches the
/// order of the input slice.
///
/// This is the main parallelisation point called by `BlockImageVerify` when
/// it needs to check many stash / source hashes concurrently.
pub fn sha1_ranges_batch(
    data: &[u8],
    range_sets: &[&RangeSet],
    block_size: usize,
) -> Result<Vec<String>> {
    range_sets
        .par_iter()
        .map(|rs| sha1_ranges(data, rs, block_size))
        .collect()
}

/// SHA-256 parallel batch variant — see [`sha1_ranges_batch`].
pub fn sha256_ranges_batch(
    data: &[u8],
    range_sets: &[&RangeSet],
    block_size: usize,
) -> Result<Vec<String>> {
    range_sets
        .par_iter()
        .map(|rs| sha256_ranges(data, rs, block_size))
        .collect()
}

// ---------------------------------------------------------------------------
// Private generic helpers
// ---------------------------------------------------------------------------

/// Generic hex-digest over a byte slice.
fn hex_digest<D: Digest>(data: &[u8]) -> String {
    let mut hasher = D::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Generic file hasher using `memmap2`.
fn hash_file<D: Digest>(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;

    let metadata = file
        .metadata()
        .with_context(|| format!("failed to stat {}", path.display()))?;

    if metadata.len() == 0 {
        return Ok(hex_digest::<D>(&[]));
    }

    // SAFETY: The file is opened read-only and we do not modify it while the
    // mapping is live. No other thread in this process writes to the file.
    // External modification would be a user error, not a library bug.
    // `memmap2` is a required project dependency specifically for this
    // high-performance I/O path.
    let mmap = unsafe {
        memmap2::Mmap::map(&file).with_context(|| format!("failed to mmap {}", path.display()))?
    };

    Ok(hex_digest::<D>(&mmap))
}

/// Generic range-based hasher — the AOSP `HashBlocks` implementation.
///
/// Feeds each range `[start * block_size, end * block_size)` into the
/// digest **in order**, then finalizes.
fn hash_ranges<D: Digest>(data: &[u8], ranges: &RangeSet, block_size: usize) -> Result<String> {
    ensure!(block_size > 0, "block_size must be positive");

    let mut hasher = D::new();

    for (start, end) in ranges {
        let byte_start = checked_block_offset(start, block_size)?;
        let byte_end = checked_block_offset(end, block_size)?;

        ensure!(
            byte_end <= data.len(),
            "range [{start}, {end}) × {block_size} = [{byte_start}, {byte_end}) \
             exceeds data length {}",
            data.len()
        );

        hasher.update(&data[byte_start..byte_end]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Multiply a block number by block size with overflow check.
#[inline]
fn checked_block_offset(block: u64, block_size: usize) -> Result<usize> {
    let bs = block_size as u64;
    let byte_offset = block.checked_mul(bs).context("block offset overflow")?;
    usize::try_from(byte_offset).context("block offset exceeds addressable range")
}
