//! Android Sparse Image format writer.
//!
//! Implements the sparse image format used by `fastboot`, `simg2img`, and
//! 7-Zip to represent block device images compactly — zero regions are
//! stored as `DONT_CARE` chunk headers (12 bytes each) instead of actual
//! zeroes.
//!
//! Reference: `system/core/libsparse/sparse_format.h` in AOSP.

use anyhow::{Context, Result, bail};
use std::io::Write;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const SPARSE_HEADER_MAGIC: u32 = 0xED26FF3A;
pub const SPARSE_MAJOR_VERSION: u16 = 1;
pub const SPARSE_MINOR_VERSION: u16 = 0;
pub const SPARSE_HEADER_SIZE: u16 = 28;
pub const CHUNK_HEADER_SIZE: u16 = 12;

pub const CHUNK_TYPE_RAW: u16 = 0xCAC1;
pub const CHUNK_TYPE_FILL: u16 = 0xCAC2;
pub const CHUNK_TYPE_DONT_CARE: u16 = 0xCAC3;

// ---------------------------------------------------------------------------
// Sparse Header parsing (for detection)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct SparseHeader {
    pub magic: u32,
    pub major_version: u16,
    pub minor_version: u16,
    pub file_header_size: u16,
    pub chunk_header_size: u16,
    pub block_size: u32,
    pub total_blocks: u32,
    pub total_chunks: u32,
    pub image_checksum: u32,
}

impl SparseHeader {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 28 {
            bail!("Sparse header too short: {} bytes", data.len());
        }

        let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        if magic != SPARSE_HEADER_MAGIC {
            bail!("Invalid sparse magic: {:08x}", magic);
        }

        Ok(Self {
            magic,
            major_version: u16::from_le_bytes([data[4], data[5]]),
            minor_version: u16::from_le_bytes([data[6], data[7]]),
            file_header_size: u16::from_le_bytes([data[8], data[9]]),
            chunk_header_size: u16::from_le_bytes([data[10], data[11]]),
            block_size: u32::from_le_bytes([data[12], data[13], data[14], data[15]]),
            total_blocks: u32::from_le_bytes([data[16], data[17], data[18], data[19]]),
            total_chunks: u32::from_le_bytes([data[20], data[21], data[22], data[23]]),
            image_checksum: u32::from_le_bytes([data[24], data[25], data[26], data[27]]),
        })
    }
}

// ---------------------------------------------------------------------------
// Header serialization
// ---------------------------------------------------------------------------

/// Write the 28-byte sparse file header.
pub fn write_sparse_header<W: Write>(
    w: &mut W,
    block_size: u32,
    total_blocks: u32,
    total_chunks: u32,
) -> Result<()> {
    w.write_all(&SPARSE_HEADER_MAGIC.to_le_bytes())?;
    w.write_all(&SPARSE_MAJOR_VERSION.to_le_bytes())?;
    w.write_all(&SPARSE_MINOR_VERSION.to_le_bytes())?;
    w.write_all(&SPARSE_HEADER_SIZE.to_le_bytes())?;
    w.write_all(&CHUNK_HEADER_SIZE.to_le_bytes())?;
    w.write_all(&block_size.to_le_bytes())?;
    w.write_all(&total_blocks.to_le_bytes())?;
    w.write_all(&total_chunks.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?; // image_checksum (unused)
    Ok(())
}

/// Write a 12-byte chunk header.
fn write_chunk_header<W: Write>(
    w: &mut W,
    chunk_type: u16,
    chunk_blocks: u32,
    total_sz: u32,
) -> Result<()> {
    w.write_all(&chunk_type.to_le_bytes())?;
    w.write_all(&0u16.to_le_bytes())?; // reserved
    w.write_all(&chunk_blocks.to_le_bytes())?;
    w.write_all(&total_sz.to_le_bytes())?;
    Ok(())
}

/// Write a DONT_CARE chunk (no data payload — just the 12-byte header).
pub fn write_dont_care_chunk<W: Write>(w: &mut W, num_blocks: u32) -> Result<()> {
    write_chunk_header(
        w,
        CHUNK_TYPE_DONT_CARE,
        num_blocks,
        CHUNK_HEADER_SIZE as u32,
    )
    .context("write DONT_CARE chunk header")
}

/// Write a RAW chunk header.  The caller must write exactly
/// `num_blocks * block_size` bytes of payload immediately after this.
pub fn write_raw_chunk_header<W: Write>(w: &mut W, num_blocks: u32, block_size: u32) -> Result<()> {
    let data_bytes = num_blocks as u64 * block_size as u64;
    let total_sz = CHUNK_HEADER_SIZE as u64 + data_bytes;
    write_chunk_header(w, CHUNK_TYPE_RAW, num_blocks, total_sz as u32)
        .context("write RAW chunk header")
}

/// Write a FILL chunk (4-byte fill value repeated for `num_blocks` blocks).
pub fn write_fill_chunk<W: Write>(w: &mut W, num_blocks: u32, fill_value: u32) -> Result<()> {
    let total_sz = CHUNK_HEADER_SIZE as u32 + 4;
    write_chunk_header(w, CHUNK_TYPE_FILL, num_blocks, total_sz)?;
    w.write_all(&fill_value.to_le_bytes())?;
    Ok(())
}
