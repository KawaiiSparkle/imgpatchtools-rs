//! bsdiff patch application — complete port of AOSP `applypatch/bspatch.cpp`.
//!
//! Applies a BSDIFF40-format patch to a source buffer, producing a target
//! buffer that is bit-exact with the AOSP `bspatch` output.
//!
//! The patch format (Colin Percival, 2003):
//! ```text
//! Header (32 bytes):
//!   [0..8)   "BSDIFF40" magic
//!   [8..16)  ctrl_len   (offtin-encoded)
//!   [16..24) diff_len   (offtin-encoded)
//!   [24..32) new_size   (offtin-encoded)
//!
//! Payload:
//!   [32 .. 32+ctrl_len)                bzip2-compressed control tuples
//!   [32+ctrl_len .. 32+ctrl_len+diff_len)  bzip2-compressed diff data
//!   [32+ctrl_len+diff_len .. end)      bzip2-compressed extra data
//! ```

use std::io::Read;

use anyhow::{ensure, Context, Result};

/// BSDIFF40 magic signature.
pub const BSDIFF_MAGIC: &[u8; 8] = b"BSDIFF40";

/// Size of the bsdiff header in bytes.
const HEADER_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Apply a BSDIFF40 patch to `source`, returning the patched output.
///
/// This is a convenience wrapper that calls [`apply_bspatch_at`] with
/// `patch_offset = 0`.
pub fn apply_bspatch(source: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    apply_bspatch_at(source, patch, 0)
}

/// Apply a BSDIFF40 patch starting at byte offset `patch_offset` within
/// `patch`.
///
/// This is the form used by the imgpatch engine, where each chunk stores a
/// bsdiff sub-patch at a specific offset within the larger imgdiff blob.
///
/// # Algorithm (matches AOSP `bspatch.cpp` exactly)
///
/// 1. Parse header: validate magic, read `ctrl_len`, `diff_len`, `new_size`.
/// 2. Decompress three bzip2 streams: control, diff, extra.
/// 3. Main loop: for each control triple `(add_len, copy_len, seek_adj)`:
///    a. Add `diff[dp..dp+add_len]` to `source[sp..sp+add_len]` → target
///    b. Copy `extra[ep..ep+copy_len]` → target
///    c. Adjust source pointer by `seek_adj`
pub fn apply_bspatch_at(source: &[u8], patch: &[u8], patch_offset: usize) -> Result<Vec<u8>> {
    let header = parse_header(patch, patch_offset)?;
    let (ctrl, diff, extra) = decompress_sections(patch, patch_offset, &header)?;
    apply_patch_loop(source, &ctrl, &diff, &extra, header.new_size)
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

/// Parsed bsdiff header fields.
struct BsdiffHeader {
    ctrl_len: usize,
    diff_len: usize,
    new_size: usize,
}

/// Validate magic and extract the three header fields.
fn parse_header(patch: &[u8], offset: usize) -> Result<BsdiffHeader> {
    ensure!(
        patch.len() >= offset + HEADER_SIZE,
        "patch too short: need at least {} bytes at offset {offset}, have {}",
        HEADER_SIZE,
        patch.len()
    );

    let hdr = &patch[offset..offset + HEADER_SIZE];

    ensure!(
        &hdr[..8] == BSDIFF_MAGIC,
        "bad bsdiff magic: expected BSDIFF40, got {:?}",
        &hdr[..8]
    );

    let ctrl_len = offtin(&hdr[8..16]);
    let diff_len = offtin(&hdr[16..24]);
    let new_size = offtin(&hdr[24..32]);

    ensure!(ctrl_len >= 0, "negative ctrl_len: {ctrl_len}");
    ensure!(diff_len >= 0, "negative diff_len: {diff_len}");
    ensure!(new_size >= 0, "negative new_size: {new_size}");

    let ctrl_len = ctrl_len as usize;
    let diff_len = diff_len as usize;
    let new_size = new_size as usize;

    // Ensure the patch is large enough to contain all three sections.
    let payload_start = offset + HEADER_SIZE;
    let min_patch_len = payload_start
        .checked_add(ctrl_len)
        .and_then(|v| v.checked_add(diff_len))
        .context("patch section lengths overflow")?;

    ensure!(
        patch.len() >= min_patch_len,
        "patch truncated: need {min_patch_len} bytes, have {}",
        patch.len()
    );

    Ok(BsdiffHeader {
        ctrl_len,
        diff_len,
        new_size,
    })
}

// ---------------------------------------------------------------------------
// Decompression
// ---------------------------------------------------------------------------

/// Decompress the three bzip2-compressed sections of a bsdiff patch.
///
/// Returns `(control_bytes, diff_bytes, extra_bytes)`.
fn decompress_sections(
    patch: &[u8],
    offset: usize,
    header: &BsdiffHeader,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let payload = &patch[offset + HEADER_SIZE..];

    let ctrl_compressed = &payload[..header.ctrl_len];
    let diff_compressed = &payload[header.ctrl_len..header.ctrl_len + header.diff_len];
    let extra_compressed = &payload[header.ctrl_len + header.diff_len..];

    let ctrl =
        decompress_bz2(ctrl_compressed).context("failed to decompress bsdiff control block")?;
    let diff = decompress_bz2(diff_compressed).context("failed to decompress bsdiff diff block")?;
    let extra =
        decompress_bz2(extra_compressed).context("failed to decompress bsdiff extra block")?;

    Ok((ctrl, diff, extra))
}

/// Decompress a bzip2 stream into a `Vec<u8>`.
fn decompress_bz2(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = bzip2::read::BzDecoder::new(data);
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .context("bzip2 decompression failed")?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Patch application loop
// ---------------------------------------------------------------------------

/// Execute the bsdiff patch algorithm.
///
/// This is a faithful transliteration of the main loop from AOSP
/// `bspatch.cpp`.
fn apply_patch_loop(
    source: &[u8],
    ctrl: &[u8],
    diff: &[u8],
    extra: &[u8],
    new_size: usize,
) -> Result<Vec<u8>> {
    let mut output = vec![0u8; new_size];
    let old_size = source.len() as i64;

    // Cursor positions.
    let mut new_pos: usize = 0; // position in output
    let mut old_pos: i64 = 0; // position in source (signed — may go negative)
    let mut ctrl_pos: usize = 0; // position in control data
    let mut diff_pos: usize = 0; // position in diff data
    let mut extra_pos: usize = 0; // position in extra data

    while new_pos < new_size {
        // ---- Read control triple ----
        ensure!(
            ctrl_pos + 24 <= ctrl.len(),
            "control data exhausted at new_pos={new_pos}"
        );
        let add_len = offtin(&ctrl[ctrl_pos..ctrl_pos + 8]);
        let copy_len = offtin(&ctrl[ctrl_pos + 8..ctrl_pos + 16]);
        let seek_adj = offtin(&ctrl[ctrl_pos + 16..ctrl_pos + 24]);
        ctrl_pos += 24;

        ensure!(add_len >= 0, "negative add_len in control tuple");
        ensure!(copy_len >= 0, "negative copy_len in control tuple");
        let add_len = add_len as usize;
        let copy_len = copy_len as usize;

        // Bounds checks.
        ensure!(
            new_pos + add_len <= new_size,
            "add_len overflows output: new_pos={new_pos}, add_len={add_len}, new_size={new_size}"
        );
        ensure!(
            diff_pos + add_len <= diff.len(),
            "add_len overflows diff data"
        );

        // ---- Apply diff block ----
        apply_diff_block(
            source,
            old_size,
            diff,
            &mut output,
            new_pos,
            old_pos,
            diff_pos,
            add_len,
        );
        new_pos += add_len;
        diff_pos += add_len;
        old_pos += add_len as i64;

        // ---- Copy extra block ----
        ensure!(new_pos + copy_len <= new_size, "copy_len overflows output");
        ensure!(
            extra_pos + copy_len <= extra.len(),
            "copy_len overflows extra data"
        );
        output[new_pos..new_pos + copy_len]
            .copy_from_slice(&extra[extra_pos..extra_pos + copy_len]);
        new_pos += copy_len;
        extra_pos += copy_len;

        // ---- Adjust source pointer ----
        old_pos += seek_adj;
    }

    Ok(output)
}

/// Apply the diff block: `output[np+i] = diff[dp+i] + source[op+i]` (wrapping).
///
/// When `old_pos + i` is out of bounds of source, the source byte is treated
/// as zero — matching AOSP behaviour.
fn apply_diff_block(
    source: &[u8],
    old_size: i64,
    diff: &[u8],
    output: &mut [u8],
    new_pos: usize,
    old_pos: i64,
    diff_pos: usize,
    add_len: usize,
) {
    for i in 0..add_len {
        let src_idx = old_pos + i as i64;
        let src_byte = if src_idx >= 0 && src_idx < old_size {
            source[src_idx as usize]
        } else {
            0
        };
        output[new_pos + i] = src_byte.wrapping_add(diff[diff_pos + i]);
    }
}

// ---------------------------------------------------------------------------
// offtin — the bsdiff signed-integer encoding
// ---------------------------------------------------------------------------

/// Decode an 8-byte bsdiff "offtin" value.
///
/// Format: little-endian magnitude in bytes 0..7, with the sign bit in bit 7
/// of byte 7. This is **not** standard two's-complement.
///
/// Matches AOSP `bspatch.cpp`'s `offtin()` exactly.
fn offtin(buf: &[u8]) -> i64 {
    debug_assert!(buf.len() >= 8);

    let mut y: i64 = (buf[7] & 0x7F) as i64;
    y = (y << 8) | buf[6] as i64;
    y = (y << 8) | buf[5] as i64;
    y = (y << 8) | buf[4] as i64;
    y = (y << 8) | buf[3] as i64;
    y = (y << 8) | buf[2] as i64;
    y = (y << 8) | buf[1] as i64;
    y = (y << 8) | buf[0] as i64;

    if buf[7] & 0x80 != 0 {
        y = -y;
    }
    y
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bzip2::write::BzEncoder;
    use bzip2::Compression;
    use std::io::Write;

    // ---- offtin -----------------------------------------------------------

    #[test]
    fn offtin_zero() {
        assert_eq!(offtin(&[0, 0, 0, 0, 0, 0, 0, 0]), 0);
    }

    #[test]
    fn offtin_one() {
        assert_eq!(offtin(&[1, 0, 0, 0, 0, 0, 0, 0]), 1);
    }

    #[test]
    fn offtin_negative_one() {
        // Magnitude = 1 in LE, sign bit set in byte 7.
        assert_eq!(offtin(&[1, 0, 0, 0, 0, 0, 0, 0x80]), -1);
    }

    #[test]
    fn offtin_256() {
        assert_eq!(offtin(&[0, 1, 0, 0, 0, 0, 0, 0]), 256);
    }

    #[test]
    fn offtin_large_positive() {
        // 0x0000_0001_0000_0000 = 4294967296
        assert_eq!(offtin(&[0, 0, 0, 0, 1, 0, 0, 0]), 4_294_967_296);
    }

    #[test]
    fn offtin_large_negative() {
        assert_eq!(offtin(&[0, 0, 0, 0, 1, 0, 0, 0x80]), -4_294_967_296);
    }

    #[test]
    fn offtin_max_positive() {
        // Magnitude = 0x7FFF_FFFF_FFFF_FFFF (max i64)
        assert_eq!(
            offtin(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
            i64::MAX
        );
    }

    // ---- Helper: build a bsdiff patch from raw sections -------------------

    /// Encode `val` in bsdiff's offtin format.
    fn to_offtin(val: i64) -> [u8; 8] {
        let magnitude = val.unsigned_abs();
        let mut buf = magnitude.to_le_bytes();
        if val < 0 {
            buf[7] |= 0x80;
        }
        buf
    }

    /// Compress `data` with bzip2.
    fn bz2_compress(data: &[u8]) -> Vec<u8> {
        let mut enc = BzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    /// Build a minimal BSDIFF40 patch from raw control tuples, diff, and
    /// extra data. Control tuples are `(add_len, copy_len, seek_adj)`.
    fn build_bsdiff_patch(
        new_size: usize,
        controls: &[(i64, i64, i64)],
        diff: &[u8],
        extra: &[u8],
    ) -> Vec<u8> {
        // Build control block: each tuple is 3×8 = 24 bytes.
        let mut ctrl_raw = Vec::with_capacity(controls.len() * 24);
        for &(x, y, z) in controls {
            ctrl_raw.extend_from_slice(&to_offtin(x));
            ctrl_raw.extend_from_slice(&to_offtin(y));
            ctrl_raw.extend_from_slice(&to_offtin(z));
        }

        let ctrl_comp = bz2_compress(&ctrl_raw);
        let diff_comp = bz2_compress(diff);
        let extra_comp = bz2_compress(extra);

        let mut patch = Vec::new();
        patch.extend_from_slice(BSDIFF_MAGIC);
        patch.extend_from_slice(&to_offtin(ctrl_comp.len() as i64));
        patch.extend_from_slice(&to_offtin(diff_comp.len() as i64));
        patch.extend_from_slice(&to_offtin(new_size as i64));
        patch.extend_from_slice(&ctrl_comp);
        patch.extend_from_slice(&diff_comp);
        patch.extend_from_slice(&extra_comp);
        patch
    }

    // ---- apply_bspatch ----------------------------------------------------

    #[test]
    fn identity_patch() {
        // Source = "hello", target = "hello".
        // Control: add_len=5 (copy source), copy_len=0, seek_adj=0.
        // Diff = [0; 5] (no change).
        let source = b"hello";
        let diff = vec![0u8; 5];
        let extra = vec![];
        let patch = build_bsdiff_patch(5, &[(5, 0, 0)], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, source);
    }

    #[test]
    fn simple_diff() {
        // Source = [1, 2, 3, 4, 5], target = [2, 4, 6, 8, 10].
        // Diff = target - source = [1, 2, 3, 4, 5].
        let source = &[1u8, 2, 3, 4, 5];
        let diff: Vec<u8> = (0..5).map(|i| (i + 1) as u8).collect();
        let extra = vec![];
        let patch = build_bsdiff_patch(5, &[(5, 0, 0)], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, vec![2, 4, 6, 8, 10]);
    }

    #[test]
    fn pure_extra() {
        // Source = anything (ignored), target = "world".
        // Control: add_len=0, copy_len=5, seek_adj=0.
        let source = b"aaaaa";
        let diff = vec![];
        let extra = b"world".to_vec();
        let patch = build_bsdiff_patch(5, &[(0, 5, 0)], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, b"world");
    }

    #[test]
    fn mixed_diff_and_extra() {
        // Source = "ABCD", target = "ABCDextra"
        // Step 1: add_len=4 (diff=[0;4] → copy source), copy_len=5 (extra="extra")
        let source = b"ABCD";
        let diff = vec![0u8; 4];
        let extra = b"extra".to_vec();
        let patch = build_bsdiff_patch(9, &[(4, 5, 0)], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, b"ABCDextra");
    }

    #[test]
    fn seek_adjustment() {
        // Source = "0123456789", target = "01234ABCDE56789"
        // Step 1: add_len=5, diff=[0;5] → "01234", copy_len=5, extra="ABCDE", seek=-5
        // Step 2: add_len=5, diff=[0;5] → "56789", copy_len=0, seek=0
        let source = b"0123456789";
        let diff = vec![0u8; 10];
        let extra = b"ABCDE".to_vec();
        let controls = vec![(5, 5, -5), (5, 0, 0)];
        let patch = build_bsdiff_patch(15, &controls, &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, b"01234ABCDE56789");
    }

    #[test]
    fn empty_to_nonempty() {
        // Source = "", target = "hello".
        // All data comes from extra.
        let source = b"";
        let diff = vec![];
        let extra = b"hello".to_vec();
        let patch = build_bsdiff_patch(5, &[(0, 5, 0)], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn nonempty_to_empty() {
        // Source = "hello", target = "".
        let source = b"hello";
        let diff = vec![];
        let extra = vec![];
        let patch = build_bsdiff_patch(0, &[], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn wrapping_add_overflow() {
        // source = [0xFF], diff = [0x02] → 0xFF + 0x02 = 0x01 (wrapping).
        let source = &[0xFFu8];
        let diff = vec![0x02];
        let extra = vec![];
        let patch = build_bsdiff_patch(1, &[(1, 0, 0)], &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, vec![0x01]);
    }

    #[test]
    fn bad_magic_fails() {
        let mut patch = vec![0u8; 64];
        patch[..8].copy_from_slice(b"NOTBSDIF");
        assert!(apply_bspatch(b"", &patch).is_err());
    }

    #[test]
    fn truncated_patch_fails() {
        assert!(apply_bspatch(b"", b"BSDIFF40").is_err());
    }

    #[test]
    fn patch_offset_basic() {
        // Prepend 16 garbage bytes, then the real patch.
        let source = b"abcde";
        let diff = vec![0u8; 5];
        let extra = vec![];
        let real_patch = build_bsdiff_patch(5, &[(5, 0, 0)], &diff, &extra);

        let mut padded = vec![0xFFu8; 16];
        padded.extend_from_slice(&real_patch);

        let result = apply_bspatch_at(source, &padded, 16).unwrap();
        assert_eq!(result, source);
    }

    #[test]
    fn to_offtin_roundtrip() {
        for val in [0i64, 1, -1, 256, -256, i64::MAX, i64::MIN + 1] {
            assert_eq!(offtin(&to_offtin(val)), val);
        }
    }

    #[test]
    fn multiple_control_tuples() {
        // Source = "HelloWorld!", target = "Hello World!!"
        // Tuple 1: add 5 from source (diff=0), copy 1 extra " ", seek=0
        // Tuple 2: add 6 from source (diff=0), copy 1 extra "!", seek=0
        let source = b"HelloWorld!";
        let diff = vec![0u8; 11];
        let extra = b" !".to_vec();
        let controls = vec![(5, 1, 0), (6, 1, 0)];
        let patch = build_bsdiff_patch(13, &controls, &diff, &extra);

        let result = apply_bspatch(source, &patch).unwrap();
        assert_eq!(result, b"Hello World!!");
    }
}
