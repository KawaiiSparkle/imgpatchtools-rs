//! Raw deflate compression with exact `deflateInit2` parameter control.
//!
//! `flate2`'s public API does not expose `mem_level` or `strategy`, so
//! bit-exact recompression of AOSP imgdiff chunks requires calling zlib
//! directly through `libz-sys`.

use anyhow::{bail, Result};
use libz_sys as zlib;
use std::mem::MaybeUninit;

// ---------------------------------------------------------------------------
// zlib constants
// ---------------------------------------------------------------------------

const Z_OK: i32 = 0;
const Z_STREAM_END: i32 = 1;
const Z_FINISH: i32 = 4;
const Z_DEFLATED: i32 = 8;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Compress `data` using raw deflate (no zlib/gzip header) with the exact
/// `deflateInit2` parameters recorded in an AOSP imgdiff patch chunk.
///
/// # Parameter handling
///
/// * `level`       – 0–9 or -1 (treated as 6, zlib default).
/// * `window_bits` – The value **as stored in the patch**. AOSP imgdiff stores
///                   the positive absolute value (e.g. 15). Some older OTA
///                   generators store the already-negated value (e.g. -15).
///                   Both cases are handled: we always end up passing a
///                   negative value in the range [-8, -15] to `deflateInit2`.
/// * `mem_level`   – 1–9.
/// * `strategy`    – 0–4.
pub fn deflate_raw_exact(
    data: &[u8],
    level: i32,
    window_bits: i32,
    mem_level: i32,
    strategy: i32,
) -> Result<Vec<u8>> {
    // Resolve default compression level.
    let level = if level < 0 { 6 } else { level.clamp(0, 9) };

    // Resolve window_bits to a raw-deflate value:
    //   - If stored as positive (typical AOSP): negate it.
    //   - If stored as negative (some generators): keep it.
    // Either way, clamp the magnitude to [8, 15] as zlib requires.
    let raw_window_bits: i32 = if window_bits > 0 {
        // positive → negate for raw deflate, clamp magnitude to [8,15]
        -(window_bits.clamp(8, 15))
    } else if window_bits < 0 {
        // already negative → clamp magnitude to [8,15]
        -((-window_bits).clamp(8, 15))
    } else {
        // zero → use safe default
        -15
    };

    // Clamp other params to zlib-valid ranges.
    let mem_level = mem_level.clamp(1, 9);
    let strategy  = strategy.clamp(0, 4);

    log::debug!(
        "deflate_raw_exact: level={level} window_bits(raw)={raw_window_bits} \
         mem_level={mem_level} strategy={strategy} input_len={}",
        data.len()
    );

    unsafe {
        deflate_raw_unsafe(data, level, raw_window_bits, mem_level, strategy)
    }
}

// ---------------------------------------------------------------------------
// Unsafe zlib wrapper
// ---------------------------------------------------------------------------

unsafe fn deflate_raw_unsafe(
    data: &[u8],
    level: i32,
    raw_window_bits: i32, // already negative, already clamped
    mem_level: i32,
    strategy: i32,
) -> Result<Vec<u8>> {
    // ---- Initialise z_stream -----------------------------------------------
    let mut strm = MaybeUninit::<zlib::z_stream>::zeroed();
    let strm_ptr = strm.as_mut_ptr();

    let ret = zlib::deflateInit2_(
        strm_ptr,
        level,
        Z_DEFLATED,
        raw_window_bits,
        mem_level,
        strategy,
        zlib::zlibVersion(),
        std::mem::size_of::<zlib::z_stream>() as i32,
    );

    if ret != Z_OK {
        bail!(
            "deflateInit2 failed with code {ret} \
             (level={level} window={raw_window_bits} mem={mem_level} strategy={strategy})"
        );
    }

    let strm = strm.assume_init_mut();

    // ---- Allocate output buffer (deflateBound gives worst-case size) --------
    let bound = zlib::deflateBound(strm, data.len() as zlib::uLong) as usize;
    let mut output = vec![0u8; bound];

    strm.next_in   = data.as_ptr() as *mut u8;
    strm.avail_in  = data.len() as zlib::uInt;
    strm.next_out  = output.as_mut_ptr();
    strm.avail_out = output.len() as zlib::uInt;

    let ret = zlib::deflate(strm, Z_FINISH);
    let produced = strm.total_out as usize;
    zlib::deflateEnd(strm);

    if ret != Z_STREAM_END {
        bail!(
            "deflate(Z_FINISH) returned {ret}, expected Z_STREAM_END; \
             produced {produced} bytes from {} bytes input",
            data.len()
        );
    }

    output.truncate(produced);
    Ok(output)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Decompress, FlushDecompress};

    fn inflate_raw(data: &[u8], hint_len: usize) -> Vec<u8> {
        let mut dec = Decompress::new(false);
        let cap = hint_len.max(data.len() * 4).max(64);
        let mut out = vec![0u8; cap];

        loop {
            let before = dec.total_out() as usize;
            match dec.decompress(data, &mut out[before..], FlushDecompress::Finish).unwrap() {
                flate2::Status::StreamEnd => break,
                _ => {
                    let cur = out.len();
                    out.resize(cur * 2, 0);
                }
            }
        }

        let n = dec.total_out() as usize;
        out.truncate(n);
        out
    }

    // ---- Positive window_bits (normal AOSP OTA) ----------------------------

    #[test]
    fn roundtrip_positive_window_bits() {
        let data = b"Hello, imgpatch bit-exact test!";
        let c = deflate_raw_exact(data, 6, 15, 8, 0).unwrap();
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    // ---- Negative window_bits (some OTA generators) ------------------------

    #[test]
    fn roundtrip_negative_window_bits() {
        let data = b"negative window_bits variant";
        // -15 should behave identically to +15
        let c_neg = deflate_raw_exact(data, 6, -15, 8, 0).unwrap();
        let c_pos = deflate_raw_exact(data,  6,  15, 8, 0).unwrap();
        assert_eq!(c_neg, c_pos, "±window_bits must produce identical output");
    }

    // ---- Zero window_bits (edge case, fall back to -15) --------------------

    #[test]
    fn roundtrip_zero_window_bits() {
        let data = b"zero window_bits edge case";
        let c = deflate_raw_exact(data, 6, 0, 8, 0).unwrap();
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    // ---- Small window_bits (7 → clamped to 8) ------------------------------

    #[test]
    fn small_window_bits_clamped() {
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        // window_bits=7 is invalid for zlib; must be clamped to 8
        let c = deflate_raw_exact(&data, 6, 7, 8, 0).unwrap();
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    // ---- mem_level variations ----------------------------------------------

    #[test]
    fn roundtrip_mem_level_1() {
        let data: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let c = deflate_raw_exact(&data, 6, 15, 1, 0).unwrap();
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    #[test]
    fn roundtrip_mem_level_9() {
        let data: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
        let c = deflate_raw_exact(&data, 6, 15, 9, 0).unwrap();
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    // ---- strategy variations -----------------------------------------------

    #[test]
    fn roundtrip_strategy_huffman_only() {
        let data: Vec<u8> = (0..512).map(|i| i as u8).collect();
        let c = deflate_raw_exact(&data, 6, 15, 8, 2).unwrap(); // Z_HUFFMAN_ONLY
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    #[test]
    fn roundtrip_strategy_rle() {
        let data: Vec<u8> = vec![0xAB; 512];
        let c = deflate_raw_exact(&data, 6, 15, 8, 3).unwrap(); // Z_RLE
        assert_eq!(inflate_raw(&c, data.len()), data);
    }

    // ---- Default level (-1) ------------------------------------------------

    #[test]
    fn default_level_equals_6() {
        let data = b"default level test";
        let cm1 = deflate_raw_exact(data, -1, 15, 8, 0).unwrap();
        let c6  = deflate_raw_exact(data,  6, 15, 8, 0).unwrap();
        assert_eq!(cm1, c6);
    }

    // ---- Determinism -------------------------------------------------------

    #[test]
    fn deterministic() {
        let data: Vec<u8> = (0..4096).map(|i| (i * 7 % 251) as u8).collect();
        let c1 = deflate_raw_exact(&data, 6, 15, 8, 0).unwrap();
        let c2 = deflate_raw_exact(&data, 6, 15, 8, 0).unwrap();
        assert_eq!(c1, c2);
    }

    // ---- Empty input -------------------------------------------------------

    #[test]
    fn empty_input() {
        let c = deflate_raw_exact(&[], 6, 15, 8, 0).unwrap();
        assert_eq!(inflate_raw(&c, 0), &[] as &[u8]);
    }

    // ---- Large input -------------------------------------------------------

    #[test]
    fn large_input() {
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();
        let c = deflate_raw_exact(&data, 6, 15, 8, 0).unwrap();
        assert_eq!(inflate_raw(&c, data.len()), data);
    }
}