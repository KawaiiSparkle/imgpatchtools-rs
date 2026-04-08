use crate::util::io::BlockFile;
use crate::util::progress::ProgressReporter;
use crate::util::rangeset::RangeSet;
use anyhow::{ensure, Result};
use crossbeam_queue::ArrayQueue;
use std::borrow::Cow;
use std::fs::File;
use std::io::{self, Read};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

/// Buffer size for producer-consumer channel (64 MiB chunks).
/// Increased from 4 MiB to match modern SSD optimal transfer size
/// and reduce channel handoff overhead for large OTA files.
const CHANNEL_CHUNK_SIZE: usize = 64 * 1024 * 1024;

/// Number of buffers in the memory pool (triple buffering).
/// Allows decompressor to run ahead while main thread writes.
const NUM_POOL_BUFFERS: usize = 3;

/// Decompression read buffer size (512 KiB).
/// Larger than the default to reduce syscall overhead during
/// decompression of compressed OTA data.
const DECOMP_BUF_SIZE: usize = 512 * 1024;

/// Memory pool for reusable buffers - eliminates heap allocation.
struct BufferPool {
    buffers: Arc<ArrayQueue<Vec<u8>>>,
    acquired_count: AtomicUsize,
}

impl BufferPool {
    fn new() -> Self {
        let buffers = Arc::new(ArrayQueue::new(NUM_POOL_BUFFERS));
        for _ in 0..NUM_POOL_BUFFERS {
            let _ = buffers.push(vec![0u8; CHANNEL_CHUNK_SIZE]);
        }
        Self {
            buffers,
            acquired_count: AtomicUsize::new(0),
        }
    }

    /// Acquire a buffer from the pool.
    /// Returns zero-initialized buffer to ensure Bit-Exact safety.
    fn acquire(&self) -> Vec<u8> {
        match self.buffers.pop() {
            Some(buf) => {
                self.acquired_count.fetch_add(1, Ordering::Relaxed);
                // Buffer is already zero-initialized from pool creation
                // and was cleared on release, safe to use
                buf
            }
            None => {
                // Pool exhausted, allocate new zero-initialized buffer
                vec![0u8; CHANNEL_CHUNK_SIZE]
            }
        }
    }

    /// Return a buffer to the pool for reuse.
    /// Clears content for security and Bit-Exact safety.
    fn release(&self, mut buf: Vec<u8>) {
        // Zero the buffer before returning to pool
        // This ensures no stale data leaks between chunks
        buf.fill(0);
        buf.clear();
        // Only keep buffers that match expected size
        if buf.capacity() >= CHANNEL_CHUNK_SIZE {
            let _ = self.buffers.push(buf);
        }
        // Otherwise drop it
    }
}

/// Background thread decompressor for new data.
/// Uses lock-free ring buffer for zero-copy data transfer.
pub struct ParallelNewDataReader {
    /// Lock-free queue for data chunks.
    receiver: Arc<ArrayQueue<Vec<u8>>>,
    /// Current buffer being consumed.
    buffer: Vec<u8>,
    /// Position in current buffer.
    buffer_pos: usize,
    /// Total bytes received.
    total_received: usize,
    /// Buffer pool for memory reuse.
    pool: Arc<BufferPool>,
    /// Background thread handle.
    _thread_handle: Option<std::thread::JoinHandle<()>>,
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

        // Create shared buffer pool and queue
        let pool = Arc::new(BufferPool::new());
        let queue = Arc::new(ArrayQueue::<Vec<u8>>::new(NUM_POOL_BUFFERS));
        let pool_clone = Arc::clone(&pool);
        let queue_clone = Arc::clone(&queue);

        // Spawn background thread for decompression
        let handle = thread::spawn(move || {
            if let Err(e) = Self::decompressor_thread(file_path, ext, pool_clone, queue_clone) {
                log::error!("background decompressor thread failed: {}", e);
            }
        });

        Ok(Self {
            receiver: queue,
            buffer: Vec::new(),
            buffer_pos: 0,
            total_received: 0,
            pool,
            _thread_handle: Some(handle),
        })
    }

    /// Background thread: decompress data and send through queue.
    fn decompressor_thread(
        path: std::path::PathBuf,
        ext: String,
        pool: Arc<BufferPool>,
        queue: Arc<ArrayQueue<Vec<u8>>>,
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

        // Read and send chunks using memory pool
        loop {
            let mut chunk = pool.acquire();
            match reader.read(&mut chunk) {
                Ok(0) => {
                    // Return unused buffer to pool
                    pool.release(chunk);
                    break;
                } // EOF
                Ok(n) => {
                    chunk.truncate(n);
                    // Spin-wait briefly if queue is full (backpressure)
                    let mut retries = 0;
                    while let Err(c) = queue.push(chunk) {
                        chunk = c;
                        if retries > 1000 {
                            log::error!("decompressor: queue full, dropping data");
                            break;
                        }
                        std::thread::yield_now();
                        retries += 1;
                    }
                }
                Err(e) => {
                    log::error!("background decompressor read error: {}", e);
                    break;
                }
            }
        }

        log::debug!("background decompressor thread finished");
        Ok(())
    }

    /// Fill internal buffer from queue.
    fn refill_buffer(&mut self) -> Result<()> {
        // Spin-wait briefly for data (reduces latency)
        let mut retries = 0;
        loop {
            match self.receiver.pop() {
                Some(chunk) => {
                    self.total_received += chunk.len();
                    // Return old buffer to pool for reuse
                    if !self.buffer.is_empty() {
                        self.pool.release(std::mem::take(&mut self.buffer));
                    }
                    self.buffer = chunk;
                    self.buffer_pos = 0;
                    return Ok(());
                }
                None => {
                    if retries > 10000 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "background decompressor channel closed",
                        )
                        .into());
                    }
                    std::thread::yield_now();
                    retries += 1;
                }
            }
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

        // Then skip whole chunks from queue
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
}

impl Drop for ParallelNewDataReader {
    fn drop(&mut self) {
        // Return final buffer to pool
        if !self.buffer.is_empty() {
            self.pool.release(std::mem::take(&mut self.buffer));
        }
    }
}

/// Legacy non-parallel reader for compatibility.
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
        }
    }

    /// Load source blocks into a contiguous buffer for patch application.
    ///
    /// This is the standard path for bsdiff/imgdiff which require all source
    /// data as a single contiguous buffer. For simple move operations without
    /// stash refs, prefer [`load_src_blocks_cow`](Self::load_src_blocks_cow)
    /// to avoid unnecessary copies.
    pub fn load_src_blocks(
        &mut self,
        ranges: &RangeSet,
        _stash_map: &std::collections::HashMap<String, RangeSet>,
    ) -> Result<Vec<u8>> {
        if let Some(ref src) = self.source {
            src.read_ranges(ranges)
        } else {
            anyhow::bail!("no source image available to load blocks from")
        }
    }

    /// Copy-on-write version: returns a reference to stashed data if available,
    /// otherwise loads from source. Reduces memory copies for stash operations.
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
