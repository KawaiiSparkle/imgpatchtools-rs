//! imgdiff patch format definitions and parser — port of AOSP `imgdiff.h` /
//! `imgdiff.cpp` / `imgpatch.cpp` header parsing.
//!
//! The IMGDIFF2 format splits an image into typed chunks. Each chunk
//! carries metadata and an optional embedded bsdiff sub-patch.
//!
//! # Wire format
//!
//! ```text
//! [0..8)    "IMGDIFF2"  magic
//! [8..12)   num_chunks  (u32 LE)
//!
//! For each chunk, sequentially:
//!   [+0..+4)  chunk_type  (u32 LE)
//!
//!   CHUNK_NORMAL (0):
//!     +4   src_start    (u64 LE)
//!     +12  src_len      (u64 LE)
//!     +20  patch_offset (u64 LE)   — offset into the patch blob
//!
//!   CHUNK_RAW (3):
//!     +4   raw_data_len (u32 LE)
//!     +8   raw_data     [raw_data_len bytes]
//!
//!   CHUNK_DEFLATE (2):
//!     +4   src_start           (u64 LE)
//!     +12  src_len             (u64 LE)
//!     +20  patch_offset        (u64 LE)
//!     +28  src_expanded_len    (u64 LE)
//!     +36  target_expanded_len (u64 LE)
//!     +44  level    (i32 LE)
//!     +48  method   (i32 LE)
//!     +52  window_bits (i32 LE)
//!     +56  mem_level   (i32 LE)
//!     +60  strategy    (i32 LE)
//!
//!   CHUNK_GZIP (1):
//!     Same as DEFLATE, followed by:
//!     +64  gzip_header_len (u32 LE)
//!     +68  gzip_header     [gzip_header_len bytes]
//!     +N   gzip_footer     [8 bytes]
//! ```

use anyhow::{Context, Result, bail, ensure};
use byteorder::{ByteOrder, LittleEndian};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic signature at the start of an IMGDIFF2 patch.
pub const IMGDIFF_MAGIC: &[u8; 8] = b"IMGDIFF2";

/// Fixed header size: 8-byte magic + 4-byte chunk count.
const IMGDIFF_HEADER_SIZE: usize = 12;

/// Chunk type: uncompressed data — apply bsdiff directly.
pub const CHUNK_NORMAL: u32 = 0;

/// Chunk type: gzip-wrapped deflate — decompress, patch, recompress with
/// gzip header/footer.
pub const CHUNK_GZIP: u32 = 1;

/// Chunk type: raw deflate — decompress, patch, recompress.
pub const CHUNK_DEFLATE: u32 = 2;

/// Chunk type: raw byte copy from the patch itself (no source needed).
pub const CHUNK_RAW: u32 = 3;

// ---------------------------------------------------------------------------
// ChunkType
// ---------------------------------------------------------------------------

/// Discriminated chunk type tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChunkType {
    /// Uncompressed source data; bsdiff applied directly.
    Normal,
    /// Gzip-wrapped deflate; decompress → bsdiff → recompress + gzip
    /// header/footer.
    Gzip,
    /// Raw deflate stream; decompress → bsdiff → recompress.
    Deflate,
    /// Literal bytes stored in the patch; copied directly to output.
    Raw,
}

impl ChunkType {
    /// Convert a u32 type tag to the enum.
    pub fn from_u32(val: u32) -> Result<Self> {
        match val {
            CHUNK_NORMAL => Ok(Self::Normal),
            CHUNK_GZIP => Ok(Self::Gzip),
            CHUNK_DEFLATE => Ok(Self::Deflate),
            CHUNK_RAW => Ok(Self::Raw),
            other => bail!("unknown imgdiff chunk type: {other}"),
        }
    }

    /// Return the u32 wire value.
    pub fn as_u32(&self) -> u32 {
        match self {
            Self::Normal => CHUNK_NORMAL,
            Self::Gzip => CHUNK_GZIP,
            Self::Deflate => CHUNK_DEFLATE,
            Self::Raw => CHUNK_RAW,
        }
    }
}

// ---------------------------------------------------------------------------
// DeflateParams
// ---------------------------------------------------------------------------

/// Deflate (re-)compression parameters stored in DEFLATE and GZIP chunks.
///
/// These are the exact `deflateInit2` parameters that the **target** file was
/// compressed with. The imgpatch engine must use these when recompressing to
/// achieve bit-exact output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeflateParams {
    /// Compression level (0–9, or -1 for default).
    pub level: i32,
    /// Compression method (always 8 = Z_DEFLATED in practice).
    pub method: i32,
    /// log₂ of window size (typically 15 for max, stored as absolute value).
    pub window_bits: i32,
    /// Memory level (1–9; AOSP default is 8).
    pub mem_level: i32,
    /// Compression strategy (0 = Z_DEFAULT_STRATEGY, etc.).
    pub strategy: i32,
}

// ---------------------------------------------------------------------------
// ImgdiffChunk
// ---------------------------------------------------------------------------

/// Metadata for a single chunk in an IMGDIFF2 patch.
///
/// Each variant carries only the fields relevant to its type, exactly
/// matching the AOSP wire format.
#[derive(Debug, Clone)]
pub enum ImgdiffChunk {
    /// Normal (uncompressed) chunk.
    Normal {
        /// Byte offset in the source image.
        src_start: u64,
        /// Byte length in the source image.
        src_len: u64,
        /// Offset within the patch blob where the bsdiff sub-patch lives.
        patch_offset: u64,
    },

    /// Raw literal chunk — bytes embedded directly in the patch.
    Raw {
        /// Offset within the **patch blob** where the raw data begins.
        data_offset: usize,
        /// Length of the raw data in bytes.
        data_len: usize,
    },

    /// Raw-deflate chunk (no gzip wrapper).
    Deflate {
        /// Byte offset in the source image.
        src_start: u64,
        /// Byte length of the compressed source data.
        src_len: u64,
        /// Offset within the patch blob for the bsdiff sub-patch.
        patch_offset: u64,
        /// Uncompressed size of the source deflate stream.
        src_expanded_len: u64,
        /// Expected uncompressed size of the target deflate stream.
        target_expanded_len: u64,
        /// Recompression parameters.
        params: DeflateParams,
    },

    /// Gzip-wrapped deflate chunk.
    Gzip {
        /// Byte offset in the source image.
        src_start: u64,
        /// Byte length of the gzip entry in the source (header + data +
        /// footer).
        src_len: u64,
        /// Offset within the patch blob for the bsdiff sub-patch.
        patch_offset: u64,
        /// Uncompressed size of the source deflate stream.
        src_expanded_len: u64,
        /// Expected uncompressed size of the target deflate stream.
        target_expanded_len: u64,
        /// Recompression parameters.
        params: DeflateParams,
        /// Target file's gzip header bytes.
        gzip_header: Vec<u8>,
        /// Target file's gzip footer (8 bytes: CRC32 LE + ISIZE LE).
        gzip_footer: [u8; 8],
    },
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parse a complete IMGDIFF2 patch and return the chunk list.
///
/// Returns `(num_chunks, chunks)` where `num_chunks` is the declared chunk
/// count from the header (always equals `chunks.len()` on success).
pub fn parse_imgdiff_patch(patch: &[u8]) -> Result<(u32, Vec<ImgdiffChunk>)> {
    ensure!(
        patch.len() >= IMGDIFF_HEADER_SIZE,
        "imgdiff patch too short ({} bytes)",
        patch.len()
    );
    ensure!(
        &patch[..8] == IMGDIFF_MAGIC,
        "bad imgdiff magic: expected IMGDIFF2"
    );

    let num_chunks = LittleEndian::read_u32(&patch[8..12]);
    let mut pos = IMGDIFF_HEADER_SIZE;
    let mut chunks = Vec::with_capacity(num_chunks as usize);

    for i in 0..num_chunks {
        let chunk = parse_one_chunk(patch, &mut pos)
            .with_context(|| format!("failed to parse imgdiff chunk {i}"))?;
        chunks.push(chunk);
    }

    Ok((num_chunks, chunks))
}

/// Parse a single chunk starting at `*pos`, advancing `*pos` past the
/// consumed bytes.
fn parse_one_chunk(patch: &[u8], pos: &mut usize) -> Result<ImgdiffChunk> {
    let chunk_type_val = read_u32(patch, pos)?;
    let chunk_type = ChunkType::from_u32(chunk_type_val)?;

    match chunk_type {
        ChunkType::Normal => parse_normal(patch, pos),
        ChunkType::Raw => parse_raw(patch, pos),
        ChunkType::Deflate => parse_deflate(patch, pos),
        ChunkType::Gzip => parse_gzip(patch, pos),
    }
}

fn parse_normal(patch: &[u8], pos: &mut usize) -> Result<ImgdiffChunk> {
    let src_start = read_u64(patch, pos)?;
    let src_len = read_u64(patch, pos)?;
    let patch_offset = read_u64(patch, pos)?;
    Ok(ImgdiffChunk::Normal {
        src_start,
        src_len,
        patch_offset,
    })
}

fn parse_raw(patch: &[u8], pos: &mut usize) -> Result<ImgdiffChunk> {
    let raw_data_len = read_u32(patch, pos)? as usize;
    let data_offset = *pos;
    ensure!(
        *pos + raw_data_len <= patch.len(),
        "RAW chunk data overflows patch (need {raw_data_len} bytes at offset {data_offset})"
    );
    *pos += raw_data_len;
    Ok(ImgdiffChunk::Raw {
        data_offset,
        data_len: raw_data_len,
    })
}

fn parse_deflate(patch: &[u8], pos: &mut usize) -> Result<ImgdiffChunk> {
    let src_start = read_u64(patch, pos)?;
    let src_len = read_u64(patch, pos)?;
    let patch_offset = read_u64(patch, pos)?;
    let src_expanded_len = read_u64(patch, pos)?;
    let target_expanded_len = read_u64(patch, pos)?;
    let params = read_deflate_params(patch, pos)?;

    Ok(ImgdiffChunk::Deflate {
        src_start,
        src_len,
        patch_offset,
        src_expanded_len,
        target_expanded_len,
        params,
    })
}

fn parse_gzip(patch: &[u8], pos: &mut usize) -> Result<ImgdiffChunk> {
    let src_start = read_u64(patch, pos)?;
    let src_len = read_u64(patch, pos)?;
    let patch_offset = read_u64(patch, pos)?;
    let src_expanded_len = read_u64(patch, pos)?;
    let target_expanded_len = read_u64(patch, pos)?;
    let params = read_deflate_params(patch, pos)?;

    // Target's gzip header.
    let gzip_header_len = read_u32(patch, pos)? as usize;
    ensure!(
        *pos + gzip_header_len <= patch.len(),
        "GZIP chunk header data overflows patch"
    );
    let gzip_header = patch[*pos..*pos + gzip_header_len].to_vec();
    *pos += gzip_header_len;

    // Target's gzip footer (CRC32 + ISIZE = 8 bytes).
    ensure!(*pos + 8 <= patch.len(), "GZIP chunk footer overflows patch");
    let mut gzip_footer = [0u8; 8];
    gzip_footer.copy_from_slice(&patch[*pos..*pos + 8]);
    *pos += 8;

    Ok(ImgdiffChunk::Gzip {
        src_start,
        src_len,
        patch_offset,
        src_expanded_len,
        target_expanded_len,
        params,
        gzip_header,
        gzip_footer,
    })
}

fn read_deflate_params(patch: &[u8], pos: &mut usize) -> Result<DeflateParams> {
    let level = read_i32(patch, pos)?;
    let method = read_i32(patch, pos)?;
    let window_bits = read_i32(patch, pos)?;
    let mem_level = read_i32(patch, pos)?;
    let strategy = read_i32(patch, pos)?;
    Ok(DeflateParams {
        level,
        method,
        window_bits,
        mem_level,
        strategy,
    })
}

// ---------------------------------------------------------------------------
// Primitive readers
// ---------------------------------------------------------------------------

fn read_u32(data: &[u8], pos: &mut usize) -> Result<u32> {
    ensure!(
        *pos + 4 <= data.len(),
        "unexpected end of patch reading u32"
    );
    let val = LittleEndian::read_u32(&data[*pos..*pos + 4]);
    *pos += 4;
    Ok(val)
}

fn read_i32(data: &[u8], pos: &mut usize) -> Result<i32> {
    ensure!(
        *pos + 4 <= data.len(),
        "unexpected end of patch reading i32"
    );
    let val = LittleEndian::read_i32(&data[*pos..*pos + 4]);
    *pos += 4;
    Ok(val)
}

fn read_u64(data: &[u8], pos: &mut usize) -> Result<u64> {
    ensure!(
        *pos + 8 <= data.len(),
        "unexpected end of patch reading u64"
    );
    let val = LittleEndian::read_u64(&data[*pos..*pos + 8]);
    *pos += 8;
    Ok(val)
}

// ---------------------------------------------------------------------------
// Gzip header utilities
// ---------------------------------------------------------------------------

/// Parse a gzip header and return its total length in bytes.
///
/// The gzip header format (RFC 1952):
/// - 10-byte fixed header (magic, method, flags, mtime, xfl, os)
/// - Optional FEXTRA field
/// - Optional FNAME field (null-terminated)
/// - Optional FCOMMENT field (null-terminated)
/// - Optional FHCRC (2-byte CRC16)
///
/// This is used by the imgpatch engine to locate the start of the raw
/// deflate stream within a gzip entry in the source image.
pub fn parse_gzip_header_len(data: &[u8]) -> Result<usize> {
    ensure!(data.len() >= 10, "gzip header too short");
    ensure!(
        data[0] == 0x1F && data[1] == 0x8B,
        "bad gzip magic: {:02x} {:02x}",
        data[0],
        data[1]
    );

    let flags = data[3];
    let mut pos = 10usize;

    // FEXTRA
    if flags & 0x04 != 0 {
        ensure!(pos + 2 <= data.len(), "gzip FEXTRA truncated");
        let xlen = LittleEndian::read_u16(&data[pos..pos + 2]) as usize;
        pos += 2 + xlen;
    }

    // FNAME (null-terminated)
    if flags & 0x08 != 0 {
        pos = find_null(data, pos).context("gzip FNAME not terminated")?;
    }

    // FCOMMENT (null-terminated)
    if flags & 0x10 != 0 {
        pos = find_null(data, pos).context("gzip FCOMMENT not terminated")?;
    }

    // FHCRC
    if flags & 0x02 != 0 {
        pos += 2;
    }

    ensure!(pos <= data.len(), "gzip header extends past data");
    Ok(pos)
}

/// Find the next null byte starting at `start` and return the position
/// **after** the null.
fn find_null(data: &[u8], start: usize) -> Option<usize> {
    data[start..]
        .iter()
        .position(|&b| b == 0)
        .map(|i| start + i + 1)
}
