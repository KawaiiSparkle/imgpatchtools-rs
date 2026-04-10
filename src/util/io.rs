//! High-performance block I/O — File-based seek/read/write with mmap fallback.
//!
//! Uses mmap for large sequential reads to reduce syscall overhead,
//! while keeping buffered I/O for writes and small random access.

use crate::util::rangeset::RangeSet;
use anyhow::{Context, Result, ensure};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::OnceLock;

/// Reusable zero-fill chunk size (64 MiB - maximize throughput).
const ZERO_BUF_SIZE: usize = 64 * 1024 * 1024;

/// Buffered I/O buffer size for non-mmap reads/writes (512 KiB).
/// Larger than OS default (4-8 KiB) to reduce syscall overhead on
/// constrained hardware (i5-3470s, SATA SSD).
const BUF_IO_SIZE: usize = 512 * 1024;

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

/// Threshold for using mmap writes: writes larger than this use mmap.
/// Mmap writes are faster for large sequential writes but have setup cost.
const MMAP_WRITE_THRESHOLD: usize = 128 * 1024 * 1024; // 128 MB

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

    /// Get mutable reference to underlying file for direct I/O.
    ///
    /// # Safety
    /// Caller must ensure proper seeking and block-aligned access.
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Get block size
    pub fn block_size(&self) -> usize {
        self.block_size
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

        // Drop old mmap if size changed significantly
        if self.mmap.is_some() && required_len > MMAP_THRESHOLD {
            self.mmap = None; // Will be re-mapped on next read
        }

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
        let mut buf = vec![0u8; ZERO_BUF_SIZE]; // Use 64 MiB stream buffer
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
    /// Automatically selects mmap path for large writes.
    pub fn write_ranges(&mut self, ranges: &RangeSet, data: &[u8]) -> Result<()> {
        let required_len = (ranges.blocks() as usize) * self.block_size;
        ensure!(
            data.len() == required_len,
            "write_ranges: data length {} does not match required length {}",
            data.len(),
            required_len
        );

        // Use mmap for large sequential writes (much faster)
        if data.len() >= MMAP_WRITE_THRESHOLD && ranges.iter().count() == 1 {
            return self.write_ranges_mmap(ranges, data);
        }

        // Standard buffered I/O for small or scattered writes
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

    /// Write ranges using memory-mapped I/O (optimized for large sequential writes).
    ///
    /// CRITICAL: Uses synchronous flush to ensure Bit-Exact consistency.
    /// Async flush would risk data loss on crash and verification failures.
    fn write_ranges_mmap(&mut self, ranges: &RangeSet, data: &[u8]) -> Result<()> {
        // Remap file as mutable
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&self.file)? };

        let mut data_offset = 0usize;
        for (start, end) in ranges.iter() {
            let num_blocks = end - start;
            let write_len = (num_blocks as usize) * self.block_size;
            let file_offset = (start as usize) * self.block_size;

            ensure!(
                file_offset + write_len <= self.file_len as usize,
                "mmap write range [{}, {}) exceeds file bounds (len {})",
                file_offset,
                file_offset + write_len,
                self.file_len
            );

            // Direct memory copy - no system calls
            mmap[file_offset..file_offset + write_len]
                .copy_from_slice(&data[data_offset..data_offset + write_len]);

            data_offset += write_len;
        }

        // CRITICAL FIX: Synchronous flush ensures data is written to disk
        // before returning. This guarantees Bit-Exact consistency and
        // prevents race conditions with subsequent read operations.
        mmap.flush().context("sync flush of mmap writes")?;

        // Additional safety: sync file metadata to ensure durability
        self.file
            .sync_data()
            .context("sync_data after mmap write")?;

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

        // Use larger buffer for maximum throughput
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
        let total_bytes: usize = ranges
            .iter()
            .map(|(s, e)| ((e - s) as usize) * self.block_size)
            .sum();

        // Use mmap for large sequential writes
        if total_bytes >= MMAP_WRITE_THRESHOLD && ranges.range_count() == 1 {
            return self.write_ranges_with_callback_mmap(ranges, read_fn, progress_cb);
        }

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

    /// Mmap-optimized version of write_ranges_with_callback for large writes.
    ///
    /// CRITICAL: Uses synchronous flush to ensure Bit-Exact consistency.
    fn write_ranges_with_callback_mmap<F, R>(
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

        // Create mutable mmap for writing
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&self.file)? };
        let mut progress_blocks = 0u64;

        for (start, end) in ranges.iter() {
            let file_offset = (start as usize) * self.block_size;
            let total_len = ((end - start) as usize) * self.block_size;
            let mut written = 0usize;

            // Write in chunks using callback
            let mut temp_buf = vec![0u8; ZERO_BUF_SIZE];
            while written < total_len {
                let chunk = (total_len - written).min(temp_buf.len());
                read_fn(&mut temp_buf[..chunk])?;

                // Copy to mmap
                mmap[file_offset + written..file_offset + written + chunk]
                    .copy_from_slice(&temp_buf[..chunk]);

                written += chunk;
                if chunk.is_multiple_of(self.block_size) {
                    progress_blocks += (chunk / self.block_size) as u64;
                    progress_cb(progress_blocks);
                }
            }
        }

        // CRITICAL FIX: Synchronous flush ensures Bit-Exact consistency
        mmap.flush().context("sync flush of mmap callback writes")?;
        self.file
            .sync_data()
            .context("sync_data after mmap callback write")?;

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
            // For single large range, use mmap-to-mmap copy
            if ranges.range_count() == 1 && src_mmap.len() >= MMAP_WRITE_THRESHOLD {
                return self.copy_ranges_mmap(ranges, src_mmap);
            }

            // Otherwise fall back to chunked copy
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

    /// Mmap-to-mmap copy for maximum performance (zero kernel copies).
    ///
    /// CRITICAL: Uses synchronous flush to ensure Bit-Exact consistency.
    fn copy_ranges_mmap(&mut self, ranges: &RangeSet, src_mmap: &memmap2::Mmap) -> Result<()> {
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&self.file)? };

        for (start, end) in ranges.iter() {
            let file_offset = (start as usize) * self.block_size;
            let total_len = ((end - start) as usize) * self.block_size;

            ensure!(
                file_offset + total_len <= mmap.len(),
                "mmap copy range exceeds destination bounds"
            );
            ensure!(
                file_offset + total_len <= src_mmap.len(),
                "mmap copy range exceeds source bounds"
            );

            // Single memcpy - no system calls
            mmap[file_offset..file_offset + total_len]
                .copy_from_slice(&src_mmap[file_offset..file_offset + total_len]);
        }

        // CRITICAL FIX: Synchronous flush ensures Bit-Exact consistency
        mmap.flush().context("sync flush of mmap copy")?;
        self.file.sync_data().context("sync_data after mmap copy")?;

        Ok(())
    }

    pub fn zero_ranges(&mut self, ranges: &RangeSet) -> Result<()> {
        self.zero_ranges_with_progress(ranges, |_| {})
    }

    /// Fill a set of block ranges with zeroes, providing live progress updates.
    ///
    /// On Windows, attempts to use `FSCTL_SET_SPARSE` + `FSCTL_SET_ZERO_DATA`
    /// for each range, which de-allocates disk blocks instead of writing zeroes.
    /// Falls back to write-zero if the sparse API is unavailable.
    #[allow(unreachable_code)] // Platform-specific cfg blocks cause false positives
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

        // Try Windows sparse file API first
        #[cfg(target_os = "windows")]
        {
            let _ = crate::util::platform::set_sparse(&self.file);
            let mut use_sparse = true;

            for (start, end) in ranges.iter() {
                let file_offset = start * (self.block_size as u64);
                let byte_len = (end - start) * (self.block_size as u64);

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
                    if chunk.is_multiple_of(self.block_size) {
                        progress_cb((chunk / self.block_size) as u64);
                    }
                }
            }
            return Ok(());
        }

        // Code below is only compiled on non-Windows platforms.
        // On Windows, the function returns in the block above.
        #[cfg(not(target_os = "windows"))]
        {
            // Try mmap zero for large contiguous ranges
            let total_zero_bytes: usize = ranges
                .iter()
                .map(|(s, e)| ((e - s) as usize) * self.block_size)
                .sum();

            if total_zero_bytes >= MMAP_WRITE_THRESHOLD && ranges.range_count() == 1 {
                return self.zero_ranges_mmap(ranges, progress_cb);
            }

            // Traditional write-zero path for non-Windows
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
            return Ok(());
        }

        #[cfg(target_os = "windows")]
        {
            // This is unreachable on Windows because the function returns above,
            // but we need it for type checking on all platforms.
            unreachable!()
        }
    }

    /// Mmap-based zero fill for large ranges (much faster than write-zero).
    ///
    /// CRITICAL: Uses synchronous flush to ensure Bit-Exact consistency.
    #[cfg(not(target_os = "windows"))]
    fn zero_ranges_mmap<F>(&mut self, ranges: &RangeSet, mut progress_cb: F) -> Result<()>
    where
        F: FnMut(u64),
    {
        let mut mmap = unsafe { memmap2::MmapMut::map_mut(&self.file)? };

        for (start, end) in ranges.iter() {
            let file_offset = (start as usize) * self.block_size;
            let total_len = ((end - start) as usize) * self.block_size;

            // Fill with zeroes using memset (libc)
            unsafe {
                std::ptr::write_bytes(mmap.as_mut_ptr().add(file_offset), 0, total_len);
            }

            progress_cb(end - start);
        }

        // CRITICAL FIX: Synchronous flush ensures Bit-Exact consistency
        mmap.flush().context("sync flush of mmap zero")?;
        self.file.sync_data().context("sync_data after mmap zero")?;

        Ok(())
    }

    /// Flush all buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush().context("failed to flush file")?;
        self.file.sync_all().context("failed to sync file to disk")
    }
}
