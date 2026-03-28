//! imgdiff patch application — complete port of AOSP `applypatch/imgpatch.cpp`.
//!
//! Applies an IMGDIFF2-format patch to a source image buffer, producing a
//! target buffer that is bit-exact with the AOSP `imgpatch` output.
//!
//! Each chunk type has a dedicated processor:
//!
//! | Chunk       | Processing                                                |
//! |-------------|-----------------------------------------------------------|
//! | `NORMAL`    | bsdiff on raw source bytes                                |
//! | `RAW`       | copy literal bytes from the patch                         |
//! | `DEFLATE`   | inflate → bsdiff → deflate (with recorded parameters)     |
//! | `GZIP`      | copy target gzip header → inflate → bsdiff → deflate →   |
//! |             | copy target gzip footer                                   |

use anyhow::{bail, ensure, Context, Result};
use flate2::{Decompress, FlushDecompress};
use super::bspatch;
use super::imgdiff_format::{
    parse_gzip_header_len, parse_imgdiff_patch, DeflateParams, ImgdiffChunk,
};
use super::zlib_raw;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Apply an IMGDIFF2 patch to `source`, returning the patched output.
///
/// If the patch does not start with `IMGDIFF2` magic, falls back to
/// treating the entire blob as a BSDIFF40 patch (matching AOSP
/// `ApplyImagePatch` behaviour).
pub fn apply_imgpatch(source: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    if patch.len() < 8 || &patch[..7] != b"IMGDIFF" {
        return bspatch::apply_bspatch(source, patch);
    }

    let (_num_chunks, chunks) = parse_imgdiff_patch(patch)?;
    let mut output = Vec::new();

    for (idx, chunk) in chunks.iter().enumerate() {
        process_chunk(source, patch, chunk, &mut output)
            .with_context(|| format!("imgpatch chunk {idx} failed"))?;
    }

    Ok(output)
}

// ---------------------------------------------------------------------------
// Per-chunk processors
// ---------------------------------------------------------------------------

fn process_chunk(
    source: &[u8],
    patch: &[u8],
    chunk: &ImgdiffChunk,
    output: &mut Vec<u8>,
) -> Result<()> {
    match chunk {
        ImgdiffChunk::Normal {
            src_start,
            src_len,
            patch_offset,
        } => process_normal(source, patch, *src_start, *src_len, *patch_offset, output),

        ImgdiffChunk::Raw {
            data_offset,
            data_len,
        } => process_raw(patch, *data_offset, *data_len, output),

        ImgdiffChunk::Deflate {
            src_start,
            src_len,
            patch_offset,
            src_expanded_len,
            target_expanded_len,
            params,
        } => process_deflate(
            source,
            patch,
            *src_start,
            *src_len,
            *patch_offset,
            *src_expanded_len,
            *target_expanded_len,
            params,
            output,
        ),

        ImgdiffChunk::Gzip {
            src_start,
            src_len,
            patch_offset,
            src_expanded_len,
            target_expanded_len,
            params,
            gzip_header,
            gzip_footer,
        } => process_gzip(
            source,
            patch,
            *src_start,
            *src_len,
            *patch_offset,
            *src_expanded_len,
            *target_expanded_len,
            params,
            gzip_header,
            gzip_footer,
            output,
        ),
    }
}

// ---------------------------------------------------------------------------
// CHUNK_NORMAL
// ---------------------------------------------------------------------------

fn process_normal(
    source: &[u8],
    patch: &[u8],
    src_start: u64,
    src_len: u64,
    patch_offset: u64,
    output: &mut Vec<u8>,
) -> Result<()> {
    let src_slice = source_slice(source, src_start, src_len)?;
    let patched = bspatch::apply_bspatch_at(src_slice, patch, patch_offset as usize)
        .context("NORMAL chunk: bsdiff failed")?;
    output.extend_from_slice(&patched);
    Ok(())
}

// ---------------------------------------------------------------------------
// CHUNK_RAW
// ---------------------------------------------------------------------------

fn process_raw(
    patch: &[u8],
    data_offset: usize,
    data_len: usize,
    output: &mut Vec<u8>,
) -> Result<()> {
    ensure!(
        data_offset + data_len <= patch.len(),
        "RAW chunk: data [{data_offset}, {}) out of bounds (patch len {})",
        data_offset + data_len,
        patch.len()
    );
    output.extend_from_slice(&patch[data_offset..data_offset + data_len]);
    Ok(())
}

// ---------------------------------------------------------------------------
// CHUNK_DEFLATE
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn process_deflate(
    source: &[u8],
    patch: &[u8],
    src_start: u64,
    src_len: u64,
    patch_offset: u64,
    src_expanded_len: u64,
    _target_expanded_len: u64,
    params: &DeflateParams,
    output: &mut Vec<u8>,
) -> Result<()> {
    // 1. Inflate the source raw-deflate stream.
    let src_compressed = source_slice(source, src_start, src_len)?;
    let inflated = inflate_raw(src_compressed, src_expanded_len as usize)
        .context("DEFLATE chunk: inflate source")?;

    // 2. Apply bsdiff to the inflated data.
    let patched = bspatch::apply_bspatch_at(&inflated, patch, patch_offset as usize)
        .context("DEFLATE chunk: bsdiff")?;

    // 3. Recompress with the exact parameters from the patch.
    let recompressed = deflate_raw_exact(&patched, params)
        .context("DEFLATE chunk: recompress")?;

    output.extend_from_slice(&recompressed);
    Ok(())
}

// ---------------------------------------------------------------------------
// CHUNK_GZIP
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn process_gzip(
    source: &[u8],
    patch: &[u8],
    src_start: u64,
    src_len: u64,
    patch_offset: u64,
    src_expanded_len: u64,
    _target_expanded_len: u64,
    params: &DeflateParams,
    target_gzip_header: &[u8],
    target_gzip_footer: &[u8; 8],
    output: &mut Vec<u8>,
) -> Result<()> {
    // 1. Write the TARGET's gzip header verbatim.
    output.extend_from_slice(target_gzip_header);

    // 2. Locate the raw deflate stream within the SOURCE gzip entry.
    let gzip_entry = source_slice(source, src_start, src_len)?;
    let src_hdr_len = parse_gzip_header_len(gzip_entry)
        .context("GZIP chunk: parse source gzip header")?;

    ensure!(
        gzip_entry.len() >= src_hdr_len + 8,
        "GZIP chunk: source entry too short (len={}, hdr={src_hdr_len})",
        gzip_entry.len()
    );

    // The raw deflate data sits between the header and the 8-byte footer.
    let deflate_data = &gzip_entry[src_hdr_len..gzip_entry.len() - 8];

    // 3. Inflate the raw deflate stream.
    let inflated = inflate_raw(deflate_data, src_expanded_len as usize)
        .context("GZIP chunk: inflate source")?;

    // 4. Apply bsdiff to the inflated data.
    let patched = bspatch::apply_bspatch_at(&inflated, patch, patch_offset as usize)
        .context("GZIP chunk: bsdiff")?;

    // 5. Recompress with the exact parameters recorded in the patch.
    let recompressed = deflate_raw_exact(&patched, params)
        .context("GZIP chunk: recompress")?;

    output.extend_from_slice(&recompressed);

    // 6. Write the TARGET's gzip footer (CRC32 + ISIZE).
    output.extend_from_slice(target_gzip_footer);

    Ok(())
}

// ---------------------------------------------------------------------------
// Compression / decompression helpers
// ---------------------------------------------------------------------------

/// Inflate a raw deflate stream (no zlib/gzip header) to the expected size.
///
/// Uses `flate2::Decompress` with `zlib_header = false`.
fn inflate_raw(compressed: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    let mut dec = Decompress::new(false);

    // Allocate the exact expected size; grow if necessary.
    let capacity = expected_len.max(compressed.len() * 4).max(64);
    let mut output = vec![0u8; capacity];

    loop {
        let before_out = dec.total_out() as usize;

        let status = dec
            .decompress(compressed, &mut output[before_out..], FlushDecompress::Finish)
            .context("raw inflate failed")?;

        match status {
            flate2::Status::StreamEnd => break,
            flate2::Status::Ok | flate2::Status::BufError => {
                // Need more space.
                let current = output.len();
                output.resize(current * 2, 0);
            }
        }
    }

    let produced = dec.total_out() as usize;

    if expected_len > 0 && produced != expected_len {
        bail!(
            "inflate produced {produced} bytes but expected {expected_len}"
        );
    }

    output.truncate(produced);
    Ok(output)
}

/// Deflate `data` using the **exact** parameters from the imgdiff chunk.
///
/// Delegates to [`zlib_raw::deflate_raw_exact`] which calls `deflateInit2`
/// directly through `libz-sys`, giving full control over `mem_level` and
/// `strategy`. This is required for bit-exact output.
fn deflate_raw_exact(data: &[u8], params: &DeflateParams) -> Result<Vec<u8>> {
    log::debug!(
        "deflate_raw_exact: level={} window={} mem_level={} strategy={}",
        params.level,
        params.window_bits,
        params.mem_level,
        params.strategy
    );

    zlib_raw::deflate_raw_exact(
        data,
        params.level,
        params.window_bits,
        params.mem_level,
        params.strategy,
    )
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

/// Bounds-checked extraction of a source sub-slice.
fn source_slice(source: &[u8], start: u64, len: u64) -> Result<&[u8]> {
    let s = start as usize;
    let e = s
        .checked_add(len as usize)
        .context("source slice range overflow")?;
    ensure!(
        e <= source.len(),
        "source slice [{s}, {e}) exceeds source length {}",
        source.len()
    );
    Ok(&source[s..e])
}