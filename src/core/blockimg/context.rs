use crate::util::io::BlockFile;
use crate::util::progress::ProgressReporter;
use crate::util::rangeset::RangeSet;
use anyhow::{ensure, Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use std::borrow::Cow;
use std::fs::File;
use std::io::{self, Read};
use std::sync::Arc;
use std::thread;

/// Buffer size for producer-consumer channel (64 MiB chunks).
const CHANNEL_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Channel capacity - number of in-flight chunks.
/// Larger queue allows decompressor to run further ahead.
const CHANNEL_CAPACITY: usize = 8;

/// Decompression read buffer size (512 KiB).
const DECOMP_BUF_SIZE: usize = 512 * 1024;

/// Background thread decompressor for new data.
/// Uses bounded channel for natural backpressure (sender blocks when full).
pub struct ParallelNewDataReader {
    /// Bounded channel receiver for data chunks.
    receiver: Receiver<Vec<u8>>,
    /// Current buffer being consumed.
    buffer: Vec<u8>,
    /// Position in current buffer.
    buffer_pos: usize,
    /// Total bytes received.
    total_received: usize,
    /// Background thread handle.
    _thread_handle: Option<std::thread::JoinHandle<()>>,
    /// 诊断信息：后台线程解压的总字节数
    pub diag_bytes_decompressed: Arc<std::sync::atomic::AtomicU64>,
    /// 诊断信息：后台线程是否已完成
    pub diag_thread_finished: Arc<std::sync::atomic::AtomicBool>,
}

impl ParallelNewDataReader {
    /// Open new data with background decompressor thread.
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let path = path.to_path_buf();

        // Determine file type
        let (file_path, ext) = match File::open(&path) {
            Ok(_) => (
                path.clone(),
                path.extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("")
                    .to_lowercase(),
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let br_path = std::path::PathBuf::from(format!("{}.br", path.display()));
                let lzma_path = std::path::PathBuf::from(format!("{}.lzma", path.display()));
                let xz_path = std::path::PathBuf::from(format!("{}.xz", path.display()));

                if File::open(&br_path).is_ok() {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        br_path.display()
                    );
                    (br_path, "br".to_string())
                } else if File::open(&lzma_path).is_ok() {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        lzma_path.display()
                    );
                    (lzma_path, "lzma".to_string())
                } else if File::open(&xz_path).is_ok() {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        xz_path.display()
                    );
                    (xz_path, "xz".to_string())
                } else {
                    return Err(e.into());
                }
            }
            Err(e) => return Err(e.into()),
        };

        log::info!(
            "starting background decompressor thread for: {}",
            file_path.display()
        );

        // Create bounded channel for backpressure
        // Sender blocks when full instead of dropping data
        let (tx, rx) = bounded::<Vec<u8>>(CHANNEL_CAPACITY);
        
        // 创建诊断统计
        let diag_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let diag_finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let diag_bytes_clone = Arc::clone(&diag_bytes);
        let diag_finished_clone = Arc::clone(&diag_finished);

        // Spawn background thread for decompression
        let handle = thread::spawn(move || {
            let result = Self::decompressor_thread(
                file_path, ext, tx, diag_bytes_clone
            );
            if let Err(e) = result {
                log::error!("background decompressor thread failed: {}", e);
            }
            diag_finished_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            log::debug!("background decompressor thread finished");
        });

        Ok(Self {
            receiver: rx,
            buffer: Vec::new(),
            buffer_pos: 0,
            total_received: 0,
            _thread_handle: Some(handle),
            diag_bytes_decompressed: diag_bytes,
            diag_thread_finished: diag_finished,
        })
    }

    /// Background thread: decompress data and send through channel.
    /// Sender blocks when channel is full (natural backpressure).
    fn decompressor_thread(
        path: std::path::PathBuf,
        ext: String,
        sender: Sender<Vec<u8>>,
        bytes_decompressed: Arc<std::sync::atomic::AtomicU64>,
    ) -> Result<()> {
        let file = File::open(&path)?;

        let mut reader: Box<dyn Read + Send> = match ext.as_str() {
            "br" => {
                log::info!(
                    "background: using Brotli decompressor (buf={} KiB)",
                    DECOMP_BUF_SIZE / 1024
                );
                Box::new(brotli::Decompressor::new(file, DECOMP_BUF_SIZE))
            }
            "lzma" | "xz" => {
                log::info!(
                    "background: using XZ/LZMA decompressor (buf={} KiB)",
                    DECOMP_BUF_SIZE / 1024
                );
                Box::new(xz2::read::XzDecoder::new(
                    std::io::BufReader::with_capacity(DECOMP_BUF_SIZE, file),
                ))
            }
            _ => {
                log::info!(
                    "background: using raw file reader (buf={} KiB)",
                    DECOMP_BUF_SIZE / 1024
                );
                Box::new(std::io::BufReader::with_capacity(DECOMP_BUF_SIZE, file))
            }
        };

        // Read and send chunks - sender blocks when channel is full
        let mut total_decompressed: u64 = 0;
        
        loop {
            let mut chunk = vec![0u8; CHANNEL_CHUNK_SIZE];
            match reader.read(&mut chunk) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    chunk.truncate(n);
                    total_decompressed += n as u64;
                    
                    // Block until channel has space (never drops data)
                    if sender.send(chunk).is_err() {
                        log::error!("decompressor: channel closed, stopping");
                        break;
                    }
                }
                Err(e) => {
                    log::error!("background decompressor read error: {}", e);
                    break;
                }
            }
        }

        // Update diagnostic stats
        bytes_decompressed.store(total_decompressed, std::sync::atomic::Ordering::SeqCst);
        
        log::info!(
            "background thread: decompressed {} MB",
            total_decompressed / 1_048_576
        );
        Ok(())
    }

    /// Fill internal buffer from channel.
    fn refill_buffer(&mut self) -> Result<()> {
        match self.receiver.recv() {
            Ok(chunk) => {
                self.total_received += chunk.len();
                self.buffer = chunk;
                self.buffer_pos = 0;
                Ok(())
            }
            Err(_) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "background decompressor channel closed",
            )
            .into()),
        }
    }

    /// Read exact number of bytes from background thread.
    /// Guarantees Bit-Exact: either fills entire buffer or returns error.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut filled = 0;

        while filled < buf.len() {
            // Refill if buffer empty
            if self.buffer_pos >= self.buffer.len() {
                self.refill_buffer()?;
            }

            // Copy from internal buffer - single memcpy
            let available = self.buffer.len() - self.buffer_pos;
            let needed = buf.len() - filled;
            let to_copy = available.min(needed);

            buf[filled..filled + to_copy]
                .copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);

            self.buffer_pos += to_copy;
            filled += to_copy;
        }

        // Bit-Exact safety: verify complete fill
        debug_assert_eq!(filled, buf.len());
        Ok(())
    }

    /// Read blocks with background decompression.
    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        let len = (count as usize) * block_size;
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Skip blocks (consume without returning) - optimized to avoid allocation.
    pub fn skip_blocks(&mut self, count: u64, block_size: usize) -> Result<()> {
        let mut remaining = (count as usize) * block_size;

        // First, consume from current buffer
        let available = self.buffer.len() - self.buffer_pos;
        let to_skip = available.min(remaining);
        self.buffer_pos += to_skip;
        remaining -= to_skip;

        // Then skip whole chunks from channel
        while remaining >= CHANNEL_CHUNK_SIZE {
            self.refill_buffer()?;
            let to_skip = remaining.min(self.buffer.len());
            self.buffer_pos += to_skip;
            remaining -= to_skip;
        }

        // Handle remainder
        if remaining > 0 {
            self.refill_buffer()?;
            self.buffer_pos += remaining;
        }

        Ok(())
    }

    /// Get total bytes received so far.
    pub fn bytes_received(&self) -> usize {
        self.total_received
    }
    
    /// Print diagnostics report
    pub fn report_diagnostics(&self) {
        let decompressed = self.diag_bytes_decompressed.load(std::sync::atomic::Ordering::SeqCst);
        let finished = self.diag_thread_finished.load(std::sync::atomic::Ordering::SeqCst);
        
        log::info!("=== ParallelNewDataReader Diagnostics ===");
        log::info!("Thread status: {}", if finished { "completed" } else { "running" });
        log::info!("Decompressed: {} ({} MB)", decompressed, decompressed / 1_048_576);
        log::info!("Consumed: {} ({} MB)", self.total_received, self.total_received / 1_048_576);
        
        if decompressed > 0 {
            let ratio = (self.total_received as f64) / (decompressed as f64);
            log::info!("Consumption ratio: {:.2}%", ratio * 100.0);
        }
        
        if finished && decompressed == self.total_received as u64 {
            log::info!("OK: All decompressed data consumed");
        } else if finished && decompressed != self.total_received as u64 {
            log::warn!("MISMATCH: decompressed {} MB, consumed {} MB", 
                decompressed / 1_048_576, self.total_received / 1_048_576);
        } else {
            log::info!("Thread still running or not finished");
        }
    }
}

impl Drop for ParallelNewDataReader {
    fn drop(&mut self) {
        // Channel is automatically closed when sender is dropped
    }
}

// Legacy non-parallel reader for compatibility.
pub struct NewDataReader {
    reader: Box<dyn Read>,
}

impl NewDataReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        // ParallelNewDataReader is the default, this fallback shouldn't be used

        let (file, ext) = match File::open(path) {
            Ok(f) => {
                let ext = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("")
                    .to_lowercase();
                (f, ext)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let br_path = std::path::PathBuf::from(format!("{}.br", path.display()));
                let lzma_path = std::path::PathBuf::from(format!("{}.lzma", path.display()));
                let xz_path = std::path::PathBuf::from(format!("{}.xz", path.display()));

                if let Ok(f) = File::open(&br_path) {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        br_path.display()
                    );
                    (f, "br".to_string())
                } else if let Ok(f) = File::open(&lzma_path) {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        lzma_path.display()
                    );
                    (f, "lzma".to_string())
                } else if let Ok(f) = File::open(&xz_path) {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        xz_path.display()
                    );
                    (f, "xz".to_string())
                } else {
                    return Err(e.into());
                }
            }
            Err(e) => return Err(e.into()),
        };

        let reader: Box<dyn Read> = match ext.as_str() {
            "br" => {
                log::info!("using Brotli streaming decompressor for new data");
                Box::new(brotli::Decompressor::new(file, DECOMP_BUF_SIZE))
            }
            "lzma" => {
                log::info!("using LZMA streaming decompressor for new data");
                let stream = xz2::stream::Stream::new_lzma_decoder(u64::MAX)
                    .map_err(|e| anyhow::anyhow!("failed to create LZMA decoder: {}", e))?;
                Box::new(xz2::read::XzDecoder::new_stream(
                    std::io::BufReader::with_capacity(DECOMP_BUF_SIZE, file),
                    stream,
                ))
            }
            "xz" => {
                log::info!("using XZ streaming decompressor for new data");
                Box::new(xz2::read::XzDecoder::new(
                    std::io::BufReader::with_capacity(DECOMP_BUF_SIZE, file),
                ))
            }
            _ => Box::new(std::io::BufReader::with_capacity(DECOMP_BUF_SIZE, file)),
        };

        Ok(Self { reader })
    }

    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        let len = (count as usize) * block_size;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn skip_blocks(&mut self, count: u64, block_size: usize) -> Result<()> {
        let mut len = (count as usize) * block_size;
        let mut buf = vec![0u8; DECOMP_BUF_SIZE.min(len)];
        while len > 0 {
            let to_read = len.min(buf.len());
            self.reader.read_exact(&mut buf[..to_read])?;
            len -= to_read;
        }
        Ok(())
    }

    pub fn get_reader_mut(&mut self) -> &mut dyn Read {
        &mut *self.reader
    }
}

/// Patch data reader using memory mapping for zero-copy access.
pub struct PatchDataReader {
    mmap: Option<memmap2::Mmap>,
}

impl PatchDataReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let file = File::open(path)?;
        let meta = file.metadata()?;

        let mmap = if meta.len() > 0 {
            Some(unsafe { memmap2::Mmap::map(&file)? })
        } else {
            log::info!(
                "patch file {} is empty (0 bytes), skipping mmap",
                path.display()
            );
            None
        };

        Ok(Self { mmap })
    }

    pub fn read_patch(&self, offset: u64, len: u64) -> Result<&[u8]> {
        if len == 0 {
            return Ok(&[]);
        }
        if let Some(ref mmap) = self.mmap {
            let start = offset as usize;
            let end = start + (len as usize);
            ensure!(end <= mmap.len(), "patch read out of bounds");
            Ok(&mmap[start..end])
        } else {
            anyhow::bail!("attempted to read patch data but patch file is empty");
        }
    }
}

/// Command execution context.
pub struct CommandContext {
    pub version: u32,
    pub block_size: usize,
    pub target: BlockFile,
    pub source: Option<BlockFile>,
    pub stash: crate::core::blockimg::stash::StashManager,
    pub new_data: ParallelNewDataReader,
    pub patch_data: PatchDataReader,
    pub written_blocks: u64,
    pub progress: Box<dyn ProgressReporter>,
    pub blocks_advanced_this_cmd: u64,
    pub(crate) reuse_buffer: Vec<u8>,
}

impl CommandContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u32,
        block_size: usize,
        target: BlockFile,
        source: Option<BlockFile>,
        stash: crate::core::blockimg::stash::StashManager,
        new_data: ParallelNewDataReader,
        patch_data: PatchDataReader,
        progress: Box<dyn ProgressReporter>,
    ) -> Self {
        Self {
            version,
            block_size,
            target,
            source,
            stash,
            new_data,
            patch_data,
            written_blocks: 0,
            progress,
            blocks_advanced_this_cmd: 0,
            reuse_buffer: Vec::new(),
        }
    }

    pub fn get_reuse_buffer(&mut self, min_capacity: usize) -> &mut Vec<u8> {
        if self.reuse_buffer.capacity() < min_capacity {
            self.reuse_buffer.reserve(min_capacity - self.reuse_buffer.capacity());
        }
        unsafe {
            self.reuse_buffer.set_len(min_capacity);
        }
        &mut self.reuse_buffer
    }

    pub fn take_reuse_buffer(&mut self, size: usize) -> &mut [u8] {
        self.reuse_buffer.clear();
        if self.reuse_buffer.capacity() < size {
            self.reuse_buffer.reserve(size - self.reuse_buffer.capacity());
        }
        unsafe {
            self.reuse_buffer.set_len(size);
        }
        &mut self.reuse_buffer[..size]
    }

    /// Load source blocks with stash support — matches AOSP `LoadSourceBlocks`.
    ///
    /// Loads data from:
    /// 1. Source image ranges (if provided)
    /// 2. Stash refs (with fault tolerance - continues even if some stashes fail)
    ///
    /// The data is merged into a single buffer according to `buffer_map` (if provided)
    /// or concatenated in order.
    ///
    /// # Fault Tolerance
    ///
    /// Matches C++ behavior: if a stash fails to load (not found, hash mismatch),
    /// logs a warning and continues. The final verification step will catch
    /// any corruption caused by missing stash data.
    pub fn load_src_blocks(
        &mut self,
        ranges: &RangeSet,
        stash_refs: &[(String, RangeSet)],
        buffer_map: Option<&RangeSet>,
    ) -> Result<Vec<u8>> {
        // Calculate total size needed
        let total_blocks = ranges.blocks();
        let total_bytes = (total_blocks as usize) * self.block_size;
        let mut result = vec![0u8; total_bytes];

        // 1. Load data from source image if available
        // (When there's no source image, we expect all data to come from stashes)
        if !ranges.is_empty() {
            if let Some(ref src) = self.source {
                let src_data = src.read_ranges(ranges)?;
                // Copy source data to the beginning of result buffer
                let copy_len = src_data.len().min(total_bytes);
                result[..copy_len].copy_from_slice(&src_data[..copy_len]);
            }
        }

        // 2. Load data from stashes with fault tolerance
        // Matches C++ behavior: continue even if some stashes fail
        for (stash_id, stash_ranges) in stash_refs {
            match self.stash.try_load(stash_id) {
                Ok(Some(stash_data)) => {
                    // Copy stash data to the appropriate location in result buffer
                    // The stash_ranges defines where in the source buffer this data goes
                    self.copy_stash_to_buffer(&mut result, stash_ranges, &stash_data)?;
                }
                Ok(None) => {
                    // Non-fatal failure: stash not found or hash mismatch
                    // Log warning and continue - let final verification catch it
                    log::warn!(
                        "load_src_blocks: failed to load stash {} (not found or corrupted), \
                         continuing anyway - verification will catch if data is actually needed",
                        stash_id
                    );
                }
                Err(e) => {
                    // Fatal IO error
                    return Err(e).with_context(|| {
                        format!("load_src_blocks: fatal error loading stash {}", stash_id)
                    });
                }
            }
        }

        // 3. Apply buffer map if provided
        // The buffer_map rearranges data from source layout to target layout
        if let Some(map) = buffer_map {
            result = self.apply_buffer_map(&result, map)?;
        }

        Ok(result)
    }

    /// Copy stash data to the appropriate location in the result buffer.
    fn copy_stash_to_buffer(
        &self,
        buffer: &mut [u8],
        stash_ranges: &RangeSet,
        stash_data: &[u8],
    ) -> Result<()> {
        let block_size = self.block_size;
        let mut stash_offset = 0usize;

        for (start, end) in stash_ranges.iter() {
            let block_count = (end - start) as usize;
            let byte_len = block_count * block_size;

            // Calculate destination offset in buffer
            let dest_offset = (start as usize) * block_size;

            // Ensure we don't overflow
            if dest_offset + byte_len > buffer.len() {
                anyhow::bail!(
                    "copy_stash_to_buffer: range {}-{} exceeds buffer size {}",
                    start, end, buffer.len()
                );
            }
            if stash_offset + byte_len > stash_data.len() {
                anyhow::bail!(
                    "copy_stash_to_buffer: not enough stash data (need {} bytes from offset {}, have {})",
                    byte_len, stash_offset, stash_data.len()
                );
            }

            // Copy the data
            buffer[dest_offset..dest_offset + byte_len]
                .copy_from_slice(&stash_data[stash_offset..stash_offset + byte_len]);

            stash_offset += byte_len;
        }

        Ok(())
    }

    /// Apply buffer map to rearrange data.
    /// The buffer_map defines how to rearrange blocks from source layout to target layout.
    fn apply_buffer_map(&self, buffer: &[u8], map: &RangeSet) -> Result<Vec<u8>> {
        let block_size = self.block_size;
        let total_blocks = map.blocks();
        let total_bytes = (total_blocks as usize) * block_size;
        let mut result = vec![0u8; total_bytes];

        let mut src_offset = 0usize;
        let mut result_offset = 0usize;

        for (start, end) in map.iter() {
            let block_count = (end - start) as usize;
            let byte_len = block_count * block_size;

            // The map defines which source blocks go to which destination blocks
            // For now, we assume sequential mapping
            if src_offset + byte_len > buffer.len() {
                anyhow::bail!(
                    "apply_buffer_map: source range exceeds buffer size"
                );
            }
            if result_offset + byte_len > result.len() {
                anyhow::bail!(
                    "apply_buffer_map: destination range exceeds result size"
                );
            }

            result[result_offset..result_offset + byte_len]
                .copy_from_slice(&buffer[src_offset..src_offset + byte_len]);

            src_offset += byte_len;
            result_offset += byte_len;
        }

        Ok(result)
    }

    pub fn load_src_blocks_cow<'a>(
        &'a self,
        ranges: &RangeSet,
        stash_data: Option<&'a [u8]>,
    ) -> Result<Cow<'a, [u8]>> {
        if let Some(data) = stash_data {
            Ok(Cow::Borrowed(data))
        } else if let Some(ref src) = self.source {
            Ok(Cow::Owned(src.read_ranges(ranges)?))
        } else {
            anyhow::bail!("no source image or stash data available")
        }
    }
}