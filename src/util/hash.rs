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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // ---- Known test vectors -----------------------------------------------
    // FIPS 180-4 / NIST reference values.

    const SHA1_EMPTY: &str = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
    const SHA1_ABC: &str = "a9993e364706816aba3e25717850c26c9cd0d89d";

    const SHA256_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    // ---- sha1_hex / sha256_hex -------------------------------------------

    #[test]
    fn sha1_hex_empty() {
        assert_eq!(sha1_hex(b""), SHA1_EMPTY);
    }

    #[test]
    fn sha1_hex_abc() {
        assert_eq!(sha1_hex(b"abc"), SHA1_ABC);
    }

    #[test]
    fn sha256_hex_empty() {
        assert_eq!(sha256_hex(b""), SHA256_EMPTY);
    }

    #[test]
    fn sha256_hex_abc() {
        assert_eq!(sha256_hex(b"abc"), SHA256_ABC);
    }

    #[test]
    fn hex_output_is_lowercase() {
        let h = sha1_hex(b"test");
        assert_eq!(h, h.to_ascii_lowercase());
    }

    // ---- verify_sha1 / verify_sha256 ------------------------------------

    #[test]
    fn verify_sha1_correct() {
        assert!(verify_sha1(b"abc", SHA1_ABC));
    }

    #[test]
    fn verify_sha1_wrong() {
        assert!(!verify_sha1(b"abc", SHA256_ABC));
    }

    #[test]
    fn verify_sha1_case_insensitive() {
        assert!(verify_sha1(b"abc", &SHA1_ABC.to_ascii_uppercase()));
    }

    #[test]
    fn verify_sha256_correct() {
        assert!(verify_sha256(b"abc", SHA256_ABC));
    }

    #[test]
    fn verify_sha256_case_insensitive() {
        assert!(verify_sha256(b"abc", &SHA256_ABC.to_ascii_uppercase()));
    }

    // ---- sha1_file / sha256_file -----------------------------------------

    #[test]
    fn sha1_file_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(sha1_file(&path).unwrap(), SHA1_ABC);
    }

    #[test]
    fn sha256_file_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"abc").unwrap();
        assert_eq!(sha256_file(&path).unwrap(), SHA256_ABC);
    }

    #[test]
    fn sha1_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        File::create(&path).unwrap();
        assert_eq!(sha1_file(&path).unwrap(), SHA1_EMPTY);
    }

    #[test]
    fn sha256_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.bin");
        File::create(&path).unwrap();
        assert_eq!(sha256_file(&path).unwrap(), SHA256_EMPTY);
    }

    #[test]
    fn sha1_file_nonexistent() {
        assert!(sha1_file(Path::new("/no/such/file.bin")).is_err());
    }

    #[test]
    fn sha1_file_large() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");
        let data: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        std::fs::write(&path, &data).unwrap();
        assert_eq!(sha1_file(&path).unwrap(), sha1_hex(&data));
    }

    // ---- sha1_ranges / sha256_ranges ------------------------------------

    /// Build a fake image: block N is filled with byte (N % 256).
    fn make_image(num_blocks: usize, block_size: usize) -> Vec<u8> {
        let mut img = Vec::with_capacity(num_blocks * block_size);
        for blk in 0..num_blocks {
            let byte = (blk % 256) as u8;
            img.extend(std::iter::repeat(byte).take(block_size));
        }
        img
    }

    #[test]
    fn sha1_ranges_single_range() {
        let bs = 4096;
        let img = make_image(10, bs);
        let rs = RangeSet::parse("2,0,10").unwrap();
        let h = sha1_ranges(&img, &rs, bs).unwrap();
        assert_eq!(h, sha1_hex(&img));
    }

    #[test]
    fn sha1_ranges_multiple_ranges() {
        let bs = 4096;
        let img = make_image(10, bs);
        let rs = RangeSet::parse("4,1,3,5,8").unwrap();
        let h = sha1_ranges(&img, &rs, bs).unwrap();

        let mut expected_data = Vec::new();
        expected_data.extend_from_slice(&img[1 * bs..3 * bs]);
        expected_data.extend_from_slice(&img[5 * bs..8 * bs]);
        assert_eq!(h, sha1_hex(&expected_data));
    }

    #[test]
    fn sha256_ranges_basic() {
        let bs = 512;
        let img = make_image(8, bs);
        let rs = RangeSet::parse("2,2,6").unwrap();
        let h = sha256_ranges(&img, &rs, bs).unwrap();

        let expected_data = &img[2 * bs..6 * bs];
        assert_eq!(h, sha256_hex(expected_data));
    }

    #[test]
    fn sha1_ranges_empty_rangeset() {
        let img = make_image(4, 4096);
        let rs = RangeSet::new();
        let h = sha1_ranges(&img, &rs, 4096).unwrap();
        assert_eq!(h, SHA1_EMPTY);
    }

    #[test]
    fn sha1_ranges_out_of_bounds() {
        let img = make_image(4, 4096);
        let rs = RangeSet::parse("2,0,100").unwrap();
        assert!(sha1_ranges(&img, &rs, 4096).is_err());
    }

    #[test]
    fn sha1_ranges_zero_block_size() {
        let img = vec![0u8; 100];
        let rs = RangeSet::parse("2,0,1").unwrap();
        assert!(sha1_ranges(&img, &rs, 0).is_err());
    }

    // ---- verify range-based hashers --------------------------------------

    #[test]
    fn verify_sha1_ranges_correct() {
        let bs = 4096;
        let img = make_image(4, bs);
        let rs = RangeSet::parse("2,0,4").unwrap();
        let expected = sha1_hex(&img);
        assert!(verify_sha1_ranges(&img, &rs, bs, &expected).unwrap());
    }

    #[test]
    fn verify_sha256_ranges_correct() {
        let bs = 4096;
        let img = make_image(4, bs);
        let rs = RangeSet::parse("2,1,3").unwrap();
        let expected = sha256_hex(&img[bs..3 * bs]);
        assert!(verify_sha256_ranges(&img, &rs, bs, &expected).unwrap());
    }

    // ---- hash_ranges_by_algorithm ----------------------------------------

    #[test]
    fn hash_ranges_by_algorithm_sha1() {
        let bs = 4096;
        let img = make_image(4, bs);
        let rs = RangeSet::parse("2,0,4").unwrap();
        let h = hash_ranges_by_algorithm(&img, &rs, bs, HashAlgorithm::Sha1).unwrap();
        assert_eq!(h, sha1_hex(&img));
    }

    #[test]
    fn hash_ranges_by_algorithm_sha256() {
        let bs = 4096;
        let img = make_image(4, bs);
        let rs = RangeSet::parse("2,0,4").unwrap();
        let h = hash_ranges_by_algorithm(&img, &rs, bs, HashAlgorithm::Sha256).unwrap();
        assert_eq!(h, sha256_hex(&img));
    }

    // ---- parallel batch --------------------------------------------------

    #[test]
    fn sha1_ranges_batch_basic() {
        let bs = 4096;
        let img = make_image(10, bs);
        let rs1 = RangeSet::parse("2,0,3").unwrap();
        let rs2 = RangeSet::parse("2,5,8").unwrap();
        let rs3 = RangeSet::parse("2,0,10").unwrap();

        let results = sha1_ranges_batch(&img, &[&rs1, &rs2, &rs3], bs).unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], sha1_ranges(&img, &rs1, bs).unwrap());
        assert_eq!(results[1], sha1_ranges(&img, &rs2, bs).unwrap());
        assert_eq!(results[2], sha1_ranges(&img, &rs3, bs).unwrap());
    }

    #[test]
    fn sha256_ranges_batch_basic() {
        let bs = 512;
        let img = make_image(8, bs);
        let rs1 = RangeSet::parse("2,0,4").unwrap();
        let rs2 = RangeSet::parse("2,4,8").unwrap();

        let results = sha256_ranges_batch(&img, &[&rs1, &rs2], bs).unwrap();

        assert_eq!(results[0], sha256_ranges(&img, &rs1, bs).unwrap());
        assert_eq!(results[1], sha256_ranges(&img, &rs2, bs).unwrap());
    }

    #[test]
    fn sha1_ranges_batch_empty() {
        let img = make_image(4, 4096);
        let results: Vec<String> = sha1_ranges_batch(&img, &[], 4096).unwrap();
        assert!(results.is_empty());
    }

    // ---- Bit-exact ordering test -----------------------------------------

    #[test]
    fn range_order_matters() {
        let bs = 4096;
        let img = make_image(10, bs);
        let rs_fwd = RangeSet::parse("4,0,2,5,7").unwrap();

        let mut rev_data = Vec::new();
        rev_data.extend_from_slice(&img[5 * bs..7 * bs]);
        rev_data.extend_from_slice(&img[0 * bs..2 * bs]);
        let rev_hash = sha1_hex(&rev_data);

        let fwd_hash = sha1_ranges(&img, &rs_fwd, bs).unwrap();

        assert_ne!(fwd_hash, rev_hash);
    }

    // ---- Large file hash via mmap ----------------------------------------

    #[test]
    fn sha1_file_matches_sha1_hex() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("match.bin");
        let mut f = File::create(&path).unwrap();
        let data = vec![0xABu8; 65536];
        f.write_all(&data).unwrap();
        drop(f);
        assert_eq!(sha1_file(&path).unwrap(), sha1_hex(&data));
    }

    // ---- Edge: single-byte block size ------------------------------------

    #[test]
    fn ranges_with_block_size_one() {
        let data = b"Hello, World!";
        let rs = RangeSet::parse("4,0,5,7,12").unwrap();
        let h = sha1_ranges(data, &rs, 1).unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(b"Hello");
        expected.extend_from_slice(b"World");
        assert_eq!(h, sha1_hex(&expected));
    }
}
