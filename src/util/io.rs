//! High-performance block I/O — File-based seek/read/write.
//!
//! Replaces the previous mmap-based implementation to avoid virtual-memory
//! pressure on machines with limited physical RAM.  A 9.3 GB system.img
//! no longer consumes 9.3 GB of address space; memory usage is bounded by
//! the caller's buffer sizes (typically a few MiB at most).

use crate::util::rangeset::RangeSet;
use anyhow::{ensure, Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Reusable zero-fill chunk size (1 MiB).
const ZERO_BUF_SIZE: usize = 1024 * 1024;

// ---------------------------------------------------------------------------
// BlockFile
// ---------------------------------------------------------------------------

/// A file handle optimized for block-based access via standard I/O.
///
/// All positioning is done with explicit `seek` calls.  Memory usage is
/// determined entirely by the caller's buffers — the file itself is never
/// mapped into memory.
pub struct BlockFile {
    /// Underlying file handle.
    file: File,
    /// Block size in bytes (typically 4096).
    block_size: usize,
    /// Total file length in bytes (cached, updated on resize).
    file_len: u64,
}

impl BlockFile {
    /// Number of whole blocks in the file.
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
        Ok(Self {
            file,
            block_size,
            file_len,
        })
    }

    /// Create (or open) a file with a specific block count.
    pub fn create(path: &Path, num_blocks: u64, block_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .with_context(|| format!("failed to create block file: {}", path.display()))?;

        let file_len = num_blocks * block_size as u64;
        file.set_len(file_len)?;

        Ok(Self {
            file,
            block_size,
            file_len,
        })
    }

    /// Extend the file if it is smaller than `required_len`.
    ///
    /// The OS zero-fills the extended region, matching the semantics of
    /// `BLKDISCARD` on a real block device.
    pub fn ensure_size(&mut self, required_len: u64) -> Result<()> {
        if required_len <= self.file_len {
            return Ok(());
        }

        log::info!(
            "extending target file from {} to {} bytes (+{} bytes / +{} blocks)",
            self.file_len,
            required_len,
            required_len - self.file_len,
            (required_len - self.file_len) / self.block_size as u64,
        );

        self.file
            .set_len(required_len)
            .context("set_len for file extension")?;
        self.file_len = required_len;

        Ok(())
    }

    /// Read a set of non-contiguous block ranges into a single contiguous buffer.
    ///
    /// Uses `&self` — positioned reads via `&File` (which implements `Read +
    /// Seek`) allow shared access without `&mut`.
    pub fn read_ranges(&self, ranges: &RangeSet) -> Result<Vec<u8>> {
        let total_blocks = ranges.blocks();
        let mut buffer = vec![0u8; (total_blocks as usize) * self.block_size];
        let mut buffer_offset = 0usize;

        // `&File` implements Read + Seek, enabling positioned I/O without
        // requiring &mut self.  This is safe in single-threaded code.
        let mut f = &self.file;

        for (start, end) in ranges.iter() {
            let num_blocks = end - start;
            let read_len = (num_blocks as usize) * self.block_size;
            let file_offset = (start as u64) * (self.block_size as u64);

            ensure!(
                file_offset + read_len as u64 <= self.file_len,
                "read range [{}, {}) exceeds file bounds (len {})",
                file_offset,
                file_offset + read_len as u64,
                self.file_len
            );

            f.seek(SeekFrom::Start(file_offset))
                .with_context(|| format!("seek to {} for read", file_offset))?;
            f.read_exact(&mut buffer[buffer_offset..buffer_offset + read_len])
                .with_context(|| format!("read {} bytes at offset {}", read_len, file_offset))?;

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
            data.len(),
            required_len
        );

        let mut data_offset = 0usize;
        for (start, end) in ranges.iter() {
            let num_blocks = end - start;
            let write_len = (num_blocks as usize) * self.block_size;
            let file_offset = (start as u64) * (self.block_size as u64);

            ensure!(
                file_offset + write_len as u64 <= self.file_len,
                "write range [{}, {}) exceeds file bounds (len {})",
                file_offset,
                file_offset + write_len as u64,
                self.file_len
            );

            self.file
                .seek(SeekFrom::Start(file_offset))
                .with_context(|| format!("seek to {} for write", file_offset))?;
            self.file
                .write_all(&data[data_offset..data_offset + write_len])
                .with_context(|| format!("write {} bytes at offset {}", write_len, file_offset))?;

            data_offset += write_len;
        }
        Ok(())
    }

    /// Fill a set of block ranges with zeroes.
    ///
    /// If any range extends beyond the current file size, the file is
    /// automatically grown to accommodate it (matching AOSP block-device
    /// semantics).  Uses a fixed 1 MiB zero buffer — memory usage is
    /// constant regardless of range size.
    pub fn zero_ranges(&mut self, ranges: &RangeSet) -> Result<()> {
        // Determine whether the file needs to grow.
        let mut max_needed: u64 = self.file_len;
        for (_start, end) in ranges.iter() {
            let range_end = (end as u64) * (self.block_size as u64);
            if range_end > max_needed {
                max_needed = range_end;
            }
        }
        if max_needed > self.file_len {
            self.ensure_size(max_needed)?;
        }

        let zeros = vec![0u8; ZERO_BUF_SIZE];

        for (start, end) in ranges.iter() {
            let file_offset = (start as u64) * (self.block_size as u64);
            let total_len = ((end - start) as usize) * self.block_size;

            ensure!(
                file_offset + total_len as u64 <= self.file_len,
                "zero range [{}, {}) exceeds file bounds (len {})",
                file_offset,
                file_offset + total_len as u64,
                self.file_len
            );

            self.file.seek(SeekFrom::Start(file_offset))?;

            let mut remaining = total_len;
            while remaining > 0 {
                let chunk = remaining.min(ZERO_BUF_SIZE);
                self.file.write_all(&zeros[..chunk])?;
                remaining -= chunk;
            }
        }
        Ok(())
    }

    /// Flush all buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush().context("failed to flush file")?;
        self.file.sync_all().context("failed to sync file to disk")
    }
}
