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
/// 2. Stream three bzip2 streams: control, diff, extra.
/// 3. Main loop: for each control triple `(add_len, copy_len, seek_adj)`:
///    a. Add `diff` stream chunks to `source[sp..sp+add_len]` directly into target
///    b. Copy `extra` stream chunks directly into target
///    c. Adjust source pointer by `seek_adj`
pub fn apply_bspatch_at(source: &[u8], patch: &[u8], patch_offset: usize) -> Result<Vec<u8>> {
    let header = parse_header(patch, patch_offset)?;

    let payload = &patch[patch_offset + HEADER_SIZE..];
    let ctrl_compressed = &payload[..header.ctrl_len];
    let diff_compressed = &payload[header.ctrl_len..header.ctrl_len + header.diff_len];
    let extra_compressed = &payload[header.ctrl_len + header.diff_len..];

    apply_patch_stream(
        source,
        ctrl_compressed,
        diff_compressed,
        extra_compressed,
        header.new_size,
    )
}

// ---------------------------------------------------------------------------
// Buffer-reuse API (matches C++ pattern)
// ---------------------------------------------------------------------------

/// Get the output size from patch header without applying.
/// Useful for pre-allocating reusable buffers.
pub fn get_output_size(patch: &[u8], patch_offset: usize) -> Result<usize> {
    let header = parse_header(patch, patch_offset)?;
    Ok(header.new_size)
}

/// Apply patch into a pre-allocated buffer.
///
/// The buffer must have at least `get_output_size()` bytes.
/// This matches C++ `applypatch` behavior where output buffer is reused.
pub fn apply_bspatch_into(
    source: &[u8],
    patch: &[u8],
    patch_offset: usize,
    output: &mut [u8],
) -> Result<()> {
    let header = parse_header(patch, patch_offset)?;
    ensure!(
        output.len() >= header.new_size,
        "output buffer too small: {} < {}",
        output.len(),
        header.new_size
    );

    let payload = &patch[patch_offset + HEADER_SIZE..];
    let ctrl_compressed = &payload[..header.ctrl_len];
    let diff_compressed = &payload[header.ctrl_len..header.ctrl_len + header.diff_len];
    let extra_compressed = &payload[header.ctrl_len + header.diff_len..];

    apply_patch_stream_into(
        source,
        ctrl_compressed,
        diff_compressed,
        extra_compressed,
        &mut output[..header.new_size],
    )
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
// Streaming Patch application loop
// ---------------------------------------------------------------------------

/// Execute the bsdiff patch algorithm using stream decoders.
///
/// This entirely avoids allocating massive intermediary buffers for the
/// uncompressed control, diff, and extra streams.
fn apply_patch_stream(
    source: &[u8],
    ctrl_compressed: &[u8],
    diff_compressed: &[u8],
    extra_compressed: &[u8],
    new_size: usize,
) -> Result<Vec<u8>> {
    let mut output = vec![0u8; new_size];
    apply_patch_stream_into(
        source,
        ctrl_compressed,
        diff_compressed,
        extra_compressed,
        &mut output,
    )?;
    Ok(output)
}

/// Apply patch into pre-allocated output buffer (zero-allocation path).
fn apply_patch_stream_into(
    source: &[u8],
    ctrl_compressed: &[u8],
    diff_compressed: &[u8],
    extra_compressed: &[u8],
    output: &mut [u8],
) -> Result<()> {
    let new_size = output.len();
    let mut ctrl_stream = bzip2::read::BzDecoder::new(ctrl_compressed);
    let mut diff_stream = bzip2::read::BzDecoder::new(diff_compressed);
    let mut extra_stream = bzip2::read::BzDecoder::new(extra_compressed);

    let old_size = source.len() as i64;

    // Cursor positions.
    let mut new_pos: usize = 0; // position in output
    let mut old_pos: i64 = 0; // position in source (signed — may go negative)
    let mut ctrl_buf = [0u8; 24];

    while new_pos < new_size {
        // ---- Read control triple ----
        ctrl_stream
            .read_exact(&mut ctrl_buf)
            .context("failed to read control tuple (patch may be truncated or corrupted)")?;

        let add_len = offtin(&ctrl_buf[0..8]);
        let copy_len = offtin(&ctrl_buf[8..16]);
        let seek_adj = offtin(&ctrl_buf[16..24]);

        ensure!(add_len >= 0, "negative add_len in control tuple: {add_len}");
        ensure!(
            copy_len >= 0,
            "negative copy_len in control tuple: {copy_len}"
        );
        let add_len = add_len as usize;
        let copy_len = copy_len as usize;

        // ---- Apply diff block ----
        ensure!(
            new_pos + add_len <= new_size,
            "add_len overflows output: new_pos={new_pos}, add_len={add_len}, new_size={new_size}"
        );

        // Stream directly into the output buffer chunk.
        let diff_chunk = &mut output[new_pos..new_pos + add_len];
        diff_stream
            .read_exact(diff_chunk)
            .context("failed to read diff data from stream")?;

        // Add source bytes to the diff chunk (wrapping).
        for i in 0..add_len {
            let src_idx = old_pos + i as i64;
            let src_byte = if src_idx >= 0 && src_idx < old_size {
                source[src_idx as usize]
            } else {
                0
            };
            diff_chunk[i] = diff_chunk[i].wrapping_add(src_byte);
        }

        new_pos += add_len;
        old_pos += add_len as i64;

        // ---- Copy extra block ----
        ensure!(
            new_pos + copy_len <= new_size,
            "copy_len overflows output: new_pos={new_pos}, copy_len={copy_len}, new_size={new_size}"
        );

        // Stream directly into the output buffer chunk.
        let extra_chunk = &mut output[new_pos..new_pos + copy_len];
        extra_stream
            .read_exact(extra_chunk)
            .context("failed to read extra data from stream")?;

        new_pos += copy_len;

        // ---- Adjust source pointer ----
        old_pos += seek_adj;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Streaming patch application (matches C++ SinkFn pattern)
// ---------------------------------------------------------------------------

/// Sink function type for streaming patch output.
/// Matches C++ `SinkFn` signature: fn(data: &[u8]) -> Result<()>
pub type SinkFn<'a> = &'a mut dyn FnMut(&[u8]) -> Result<()>;

/// Apply a BSDIFF40 patch with streaming output.
///
/// This is the **performance-optimized path** that matches C++ `bspatch`
/// behavior: output is streamed through `sink` callback instead of being
/// collected into a Vec. Eliminates large buffer allocation.
///
/// # Arguments
/// * `source` - Source data buffer
/// * `patch` - Patch data buffer
/// * `patch_offset` - Offset within patch where BSDIFF40 header starts
/// * `sink` - Callback that receives output chunks (must write all data)
///
/// # Example
/// ```ignore
/// let mut output = Vec::new();
/// apply_bspatch_stream(source, patch, 0, &mut |chunk| {
///     output.extend_from_slice(chunk);
///     Ok(())
/// })?;
/// ```
pub fn apply_bspatch_stream(
    source: &[u8],
    patch: &[u8],
    patch_offset: usize,
    mut sink: SinkFn<'_>,
) -> Result<()> {
    let header = parse_header(patch, patch_offset)?;

    let payload = &patch[patch_offset + HEADER_SIZE..];
    let ctrl_compressed = &payload[..header.ctrl_len];
    let diff_compressed = &payload[header.ctrl_len..header.ctrl_len + header.diff_len];
    let extra_compressed = &payload[header.ctrl_len + header.diff_len..];

    apply_patch_stream_sink(
        source,
        ctrl_compressed,
        diff_compressed,
        extra_compressed,
        header.new_size,
        &mut sink,
    )
}

/// Internal: streaming patch application with sink.
fn apply_patch_stream_sink(
    source: &[u8],
    ctrl_compressed: &[u8],
    diff_compressed: &[u8],
    extra_compressed: &[u8],
    new_size: usize,
    sink: &mut SinkFn<'_>,
) -> Result<()> {
    let mut ctrl_stream = bzip2::read::BzDecoder::new(ctrl_compressed);
    let mut diff_stream = bzip2::read::BzDecoder::new(diff_compressed);
    let mut extra_stream = bzip2::read::BzDecoder::new(extra_compressed);

    let old_size = source.len() as i64;
    let mut new_pos: usize = 0;
    let mut old_pos: i64 = 0;
    let mut ctrl_buf = [0u8; 24];

    // Reusable output chunk for diff application (avoids per-iteration alloc)
    let mut diff_chunk = vec![0u8; 64 * 1024];  // 64KB reusable buffer

    while new_pos < new_size {
        // Read control triple
        ctrl_stream
            .read_exact(&mut ctrl_buf)
            .context("failed to read control tuple")?;

        let add_len = offtin(&ctrl_buf[0..8]) as usize;
        let copy_len = offtin(&ctrl_buf[8..16]) as usize;
        let seek_adj = offtin(&ctrl_buf[16..24]);

        // Apply diff in chunks using reusable buffer
        let mut diff_remaining = add_len;
        while diff_remaining > 0 {
            let chunk_size = diff_remaining.min(diff_chunk.len());
            
            // Ensure buffer is large enough
            if diff_chunk.len() < chunk_size {
                diff_chunk.resize(chunk_size, 0);
            }
            
            diff_stream
                .read_exact(&mut diff_chunk[..chunk_size])
                .context("failed to read diff data")?;

            // Add source bytes (optimized: use pointer arithmetic)
            for i in 0..chunk_size {
                let src_idx = old_pos + i as i64;
                let src_byte = if src_idx >= 0 && src_idx < old_size {
                    source[src_idx as usize]
                } else {
                    0
                };
                diff_chunk[i] = diff_chunk[i].wrapping_add(src_byte);
            }

            sink(&diff_chunk[..chunk_size])
                .context("sink failed during diff write")?;

            diff_remaining -= chunk_size;
            new_pos += chunk_size;
            old_pos += chunk_size as i64;
        }

        // Copy extra data in chunks
        let mut extra_remaining = copy_len;
        while extra_remaining > 0 {
            let chunk_size = extra_remaining.min(diff_chunk.len());
            
            if diff_chunk.len() < chunk_size {
                diff_chunk.resize(chunk_size, 0);
            }
            
            extra_stream
                .read_exact(&mut diff_chunk[..chunk_size])
                .context("failed to read extra data")?;

            sink(&diff_chunk[..chunk_size])
                .context("sink failed during extra write")?;

            extra_remaining -= chunk_size;
            new_pos += chunk_size;
        }

        old_pos += seek_adj;
    }

    Ok(())
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
