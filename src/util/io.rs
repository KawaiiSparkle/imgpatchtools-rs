//! High-performance block I/O — File-based seek/read/write with mmap fallback.
//!
//! Uses mmap for large sequential reads to reduce syscall overhead,
//! while keeping buffered I/O for writes and small random access.

use crate::util::rangeset::RangeSet;
use anyhow::{ensure, Context, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::OnceLock;

/// Reusable zero-fill chunk size (64 MiB - maximize throughput).
const ZERO_BUF_SIZE: usize = 64 * 1024 * 1024;

/// Buffered I/O buffer size for non-mmap reads/writes (256 KiB).
/// Larger than OS default (4-8 KiB) to reduce syscall overhead on
/// constrained hardware (i5-3470s, SATA SSD).
const BUF_IO_SIZE: usize = 256 * 1024;

/// Global zero buffer - allocate once, reuse forever.
static ZERO_BUFFER: OnceLock<Vec<u8>> = OnceLock::new();

fn get_zero_buffer() -> &'static [u8] {
    ZERO_BUFFER.get_or_init(|| vec![0u8; ZERO_BUF_SIZE])
}

/// Threshold for using mmap: files larger than this use mmap for reads.
/// Set to 16 MB to avoid virtual memory pressure on constrained systems
/// (1.8 GB available RAM). Large files (10+ GB source images) are better
/// served by explicit buffered I/O with large buffers.
const MMAP_THRESHOLD: u64 = 16 * 1024 * 1024; // 16 MB

// ---------------------------------------------------------------------------
// BlockFile
// ---------------------------------------------------------------------------

/// A file handle optimized for block-based access.
///
/// Uses mmap for large file reads (above MMAP_THRESHOLD) to reduce syscall
/// overhead, and buffered I/O for writes and small files.
pub struct BlockFile {
    /// Underlying file handle.
    file: File,
    /// Optional memory map for large file reads.
    mmap: Option<memmap2::Mmap>,
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

        #[cfg(target_os = "windows")]
        crate::util::platform::set_sequential_hint(&file);

        let file_len = file.metadata()?.len();

        // Use mmap for large files to reduce syscall overhead
        let mmap = if file_len >= MMAP_THRESHOLD {
            unsafe {
                memmap2::Mmap::map(&file)
                    .map_err(|e| log::debug!("mmap failed: {}, falling back to buffered I/O", e))
                    .ok()
            }
        } else {
            None
        };

        Ok(Self {
            file,
            mmap,
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

        // New files don't use mmap until they grow large enough
        Ok(Self {
            file,
            mmap: None,
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
    /// Uses mmap for fast access if available, falls back to buffered I/O.
    pub fn read_ranges(&self, ranges: &RangeSet) -> Result<Vec<u8>> {
        let total_blocks = ranges.blocks();
        let mut buffer = vec![0u8; (total_blocks as usize) * self.block_size];
        let mut buffer_offset = 0usize;

        // Use mmap for fast reads if available
        if let Some(ref mmap) = self.mmap {
            for (start, end) in ranges.iter() {
                let num_blocks = end - start;
                let read_len = (num_blocks as usize) * self.block_size;
                let file_offset = (start as usize) * self.block_size;

                ensure!(
                    file_offset + read_len <= self.file_len as usize,
                    "read range [{}, {}) exceeds file bounds (len {})",
                    file_offset,
                    file_offset + read_len,
                    self.file_len
                );

                buffer[buffer_offset..buffer_offset + read_len]
                    .copy_from_slice(&mmap[file_offset..file_offset + read_len]);
                buffer_offset += read_len;
            }
        } else {
            // Fall back to buffered I/O with large buffer to reduce syscall overhead
            let mut f = std::io::BufReader::with_capacity(BUF_IO_SIZE, &self.file);
            for (start, end) in ranges.iter() {
                let num_blocks = end - start;
                let read_len = (num_blocks as usize) * self.block_size;
                let file_offset = start * (self.block_size as u64);

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
                    .with_context(|| {
                        format!("read {} bytes at offset {}", read_len, file_offset)
                    })?;

                buffer_offset += read_len;
            }
        }
        Ok(buffer)
    }

    /// Read ranges block-by-block and pass them to a callback (avoids allocating memory).
    pub fn chunked_read_ranges<F>(&self, ranges: &RangeSet, mut cb: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let mut buf = vec![0u8; ZERO_BUF_SIZE]; // Use 8 MiB stream buffer
        let mut f = &self.file;
        for (start, end) in ranges.iter() {
            let file_offset = start * (self.block_size as u64);
            let mut total_len = ((end - start) as usize) * self.block_size;

            f.seek(SeekFrom::Start(file_offset))?;

            while total_len > 0 {
                let chunk = total_len.min(buf.len());
                f.read_exact(&mut buf[..chunk])?;
                cb(&buf[..chunk])?;
                total_len -= chunk;
            }
        }
        Ok(())
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
            let file_offset = start * (self.block_size as u64);

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

    /// Sequentially stream data from a reader into non-contiguous target block ranges.
    pub fn write_ranges_from_reader<F>(
        &mut self,
        ranges: &RangeSet,
        reader: &mut dyn Read,
        mut progress_cb: F,
    ) -> Result<()>
    where
        F: FnMut(u64), // blocks processed
    {
        let mut max_needed: u64 = self.file_len;
        for (_start, end) in ranges.iter() {
            let range_end = end * (self.block_size as u64);
            if range_end > max_needed {
                max_needed = range_end;
            }
        }
        if max_needed > self.file_len {
            self.ensure_size(max_needed)?;
        }

        let mut buf = vec![0u8; ZERO_BUF_SIZE];
        for (start, end) in ranges.iter() {
            let file_offset = start * (self.block_size as u64);
            let mut total_len = ((end - start) as usize) * self.block_size;

            self.file.seek(SeekFrom::Start(file_offset))?;

            while total_len > 0 {
                let chunk = total_len.min(buf.len());
                reader.read_exact(&mut buf[..chunk])?;
                self.file.write_all(&buf[..chunk])?;
                total_len -= chunk;

                if chunk.is_multiple_of(self.block_size) {
                    progress_cb((chunk / self.block_size) as u64);
                }
            }
        }
        Ok(())
    }

    /// Write ranges using a custom read callback (for parallel readers).
    /// The callback should fill the provided buffer and return Ok(()) on success.
    pub fn write_ranges_with_callback<F, R>(
        &mut self,
        ranges: &RangeSet,
        mut read_fn: F,
        mut progress_cb: R,
    ) -> Result<()>
    where
        F: FnMut(&mut [u8]) -> Result<()>,
        R: FnMut(u64),
    {
        let mut max_needed: u64 = self.file_len;
        for (_start, end) in ranges.iter() {
            let range_end = end * (self.block_size as u64);
            if range_end > max_needed {
                max_needed = range_end;
            }
        }
        if max_needed > self.file_len {
            self.ensure_size(max_needed)?;
        }

        let mut buf = vec![0u8; ZERO_BUF_SIZE];
        for (start, end) in ranges.iter() {
            let file_offset = start * (self.block_size as u64);
            let mut total_len = ((end - start) as usize) * self.block_size;

            self.file.seek(SeekFrom::Start(file_offset))?;

            while total_len > 0 {
                let chunk = total_len.min(buf.len());
                read_fn(&mut buf[..chunk])?;
                self.file.write_all(&buf[..chunk])?;
                total_len -= chunk;

                if chunk.is_multiple_of(self.block_size) {
                    progress_cb((chunk / self.block_size) as u64);
                }
            }
        }
        Ok(())
    }

    /// Directly copy ranges from a source BlockFile (bypassing large memory allocations).
    pub fn copy_ranges(&mut self, ranges: &RangeSet, src: &BlockFile) -> Result<()> {
        let mut max_needed: u64 = self.file_len;
        for (_start, end) in ranges.iter() {
            let range_end = end * (self.block_size as u64);
            if range_end > max_needed {
                max_needed = range_end;
            }
        }
        if max_needed > self.file_len {
            self.ensure_size(max_needed)?;
        }

        // Use source mmap if available for faster reads
        if let Some(ref src_mmap) = src.mmap {
            for (start, end) in ranges.iter() {
                let file_offset = (start as usize) * src.block_size;
                let total_len = ((end - start) as usize) * src.block_size;

                self.file.seek(SeekFrom::Start(file_offset as u64))?;
                self.file
                    .write_all(&src_mmap[file_offset..file_offset + total_len])?;
            }
        } else {
            // Use large buffer for maximum throughput
            let mut buf = vec![0u8; ZERO_BUF_SIZE];
            let mut src_f = &src.file;

            for (start, end) in ranges.iter() {
                let file_offset = start * (self.block_size as u64);
                let mut total_len = ((end - start) as usize) * self.block_size;

                self.file.seek(SeekFrom::Start(file_offset))?;
                src_f.seek(SeekFrom::Start(file_offset))?;

                while total_len > 0 {
                    let chunk = total_len.min(buf.len());
                    src_f.read_exact(&mut buf[..chunk])?;
                    self.file.write_all(&buf[..chunk])?;
                    total_len -= chunk;
                }
            }
        }
        Ok(())
    }

    pub fn zero_ranges(&mut self, ranges: &RangeSet) -> Result<()> {
        self.zero_ranges_with_progress(ranges, |_| {})
    }

    /// Fill a set of block ranges with zeroes, providing live progress updates.
    ///
    /// On Windows (方案5), attempts to use `FSCTL_SET_SPARSE` + `FSCTL_SET_ZERO_DATA`
    /// for each range, which de-allocates disk blocks instead of writing zeroes.
    /// Falls back to write-zero if the sparse API is unavailable.
    pub fn zero_ranges_with_progress<F>(
        &mut self,
        ranges: &RangeSet,
        mut progress_cb: F,
    ) -> Result<()>
    where
        F: FnMut(u64),
    {
        let mut max_needed: u64 = self.file_len;
        for (_start, end) in ranges.iter() {
            let range_end = end * (self.block_size as u64);
            if range_end > max_needed {
                max_needed = range_end;
            }
        }
        if max_needed > self.file_len {
            self.ensure_size(max_needed)?;
        }

        // 方案5: Try Windows sparse file API first
        #[cfg(target_os = "windows")]
        {
            let _ = crate::util::platform::set_sparse(&self.file);
            let mut use_sparse = true;

            for (start, end) in ranges.iter() {
                let file_offset = (start as u64) * (self.block_size as u64);
                let byte_len = ((end - start) as u64) * (self.block_size as u64);

                if use_sparse {
                    match crate::util::platform::zero_data(&self.file, file_offset, byte_len) {
                        Ok(()) => {
                            let blocks = end - start;
                            progress_cb(blocks);
                            continue;
                        }
                        Err(_) => {
                            log::debug!("sparse zero failed, falling back to write-zero");
                            use_sparse = false;
                            // fall through to write-zero path
                        }
                    }
                }

                // Fallback: write zeroes
                let zeros = get_zero_buffer();
                let mut remaining = byte_len as usize;
                self.file.seek(SeekFrom::Start(file_offset))?;
                while remaining > 0 {
                    let chunk = remaining.min(zeros.len());
                    self.file.write_all(&zeros[..chunk])?;
                    remaining -= chunk;
                    if chunk % self.block_size == 0 {
                        progress_cb((chunk / self.block_size) as u64);
                    }
                }
            }
            return Ok(());
        }

        // Non-Windows: traditional write-zero path
        let zeros = get_zero_buffer();

        for (start, end) in ranges.iter() {
            let file_offset = start * (self.block_size as u64);
            let mut remaining = ((end - start) as usize) * self.block_size;

            self.file.seek(SeekFrom::Start(file_offset))?;

            while remaining > 0 {
                let chunk = remaining.min(zeros.len());
                self.file.write_all(&zeros[..chunk])?;
                remaining -= chunk;
                if chunk.is_multiple_of(self.block_size) {
                    progress_cb((chunk / self.block_size) as u64);
                }
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
