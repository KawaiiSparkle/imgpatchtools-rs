//! High-performance, memory-mapped block I/O.
//!
//! Provides a `BlockFile` abstraction for reading and writing non-contiguous
//! block ranges, which is the core I/O pattern for `block_image_update`.

use anyhow::{ensure, Context, Result};
use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::path::Path;
use crate::util::rangeset::RangeSet;

// ---------------------------------------------------------------------------
// BlockFile
// ---------------------------------------------------------------------------

/// A memory-mapped file handle optimized for block-based access.
pub struct BlockFile {
    /// Underlying memory map.
    inner: MmapMut,
    /// Block size in bytes (typically 4096).
    block_size: usize,
    /// Total file length in bytes (cached at open time).
    file_len: u64,
    /// Kept alive so the OS does not invalidate the mapping.
    _file: File,
}

impl BlockFile {
    pub fn total_blocks(&self) -> u64 {
    self.file_len / self.block_size as u64
}
    /// Open an existing file for read/write access.
    pub fn open(path: &Path, block_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("failed to open block file: {}", path.display()))?;
        
        let file_len = file.metadata()?.len();
        let inner = unsafe { MmapOptions::new().map_mut(&file)? };
        
        Ok(Self { inner, block_size, file_len, _file: file })
    }

    /// Create a new file of a specific size, or open and truncate an existing one.
    pub fn create(path: &Path, num_blocks: u64, block_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .with_context(|| format!("failed to create block file: {}", path.display()))?;
        
        let file_len = num_blocks as u64 * block_size as u64;
        file.set_len(file_len)?;
        
        let inner = unsafe { MmapOptions::new().map_mut(&file)? };
        
        Ok(Self { inner, block_size, file_len, _file: file })
    }

    /// Read a set of non-contiguous block ranges into a single contiguous buffer.
    pub fn read_ranges(&self, ranges: &RangeSet) -> Result<Vec<u8>> {
        let total_blocks = ranges.blocks();
        let mut buffer = vec![0u8; (total_blocks as usize) * self.block_size];
        let mut buffer_offset = 0;

        for (start, end) in ranges.iter() {
            let num_blocks = end - start;
            let read_len = (num_blocks as usize) * self.block_size;
            let file_offset = (start as usize) * self.block_size;

            ensure!(
                (file_offset + read_len) as u64 <= self.file_len,
                "read range [{}, {}) exceeds file bounds (len {})",
                file_offset, file_offset + read_len, self.file_len
            );

            buffer[buffer_offset..buffer_offset + read_len]
                .copy_from_slice(&self.inner[file_offset..file_offset + read_len]);
            
            buffer_offset += read_len;
        }
        Ok(buffer)
    }

    /// Write a contiguous buffer of data to a set of non-contiguous block ranges.
    pub fn write_ranges(&mut self, ranges: &RangeSet, data: &[u8]) -> Result<()> {
        let required_len = (ranges.blocks() as usize) * self.block_size;
        ensure!(
            data.len() == required_len,
            "write_ranges: data length {} does not match required length {}",
            data.len(), required_len
        );

        let mut data_offset = 0;
        for (start, end) in ranges.iter() {
            let num_blocks = end - start;
            let write_len = (num_blocks as usize) * self.block_size;
            let file_offset = (start as usize) * self.block_size;

            ensure!(
                (file_offset + write_len) as u64 <= self.file_len,
                "write range [{}, {}) exceeds file bounds (len {})",
                file_offset, file_offset + write_len, self.file_len
            );

            self.inner[file_offset..file_offset + write_len]
                .copy_from_slice(&data[data_offset..data_offset + write_len]);
            
            data_offset += write_len;
        }
        Ok(())
    }

    /// Fill a set of block ranges with zeroes.
    pub fn zero_ranges(&mut self, ranges: &RangeSet) -> Result<()> {
        for (start, end) in ranges.iter() {
            let file_offset = (start as usize) * self.block_size;
            let write_len = ((end - start) as usize) * self.block_size;

            ensure!(
                (file_offset + write_len) as u64 <= self.file_len,
                "zero range [{}, {}) exceeds file bounds (len {})",
                file_offset, file_offset + write_len, self.file_len
            );

            self.inner[file_offset..file_offset + write_len].fill(0);
        }
        Ok(())
    }

    /// Flush memory-mapped changes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush().context("failed to flush mmap")
    }
}