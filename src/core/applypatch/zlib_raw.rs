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
    let strategy = strategy.clamp(0, 4);

    log::debug!(
        "deflate_raw_exact: level={level} window_bits(raw)={raw_window_bits} \
         mem_level={mem_level} strategy={strategy} input_len={}",
        data.len()
    );

    unsafe { deflate_raw_unsafe(data, level, raw_window_bits, mem_level, strategy) }
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

    strm.next_in = data.as_ptr() as *mut u8;
    strm.avail_in = data.len() as zlib::uInt;
    strm.next_out = output.as_mut_ptr();
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
