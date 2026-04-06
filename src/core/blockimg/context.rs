use crate::util::io::BlockFile;
use crate::util::progress::ProgressReporter;
use crate::util::rangeset::RangeSet;
use anyhow::{ensure, Result};
use std::fs::File;
use std::io::{self, Read};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

/// Buffer size for producer-consumer channel (4 MiB chunks).
/// Increased from 1 MiB to reduce channel handoff overhead for large OTA files.
const CHANNEL_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// Decompression read buffer size (256 KiB).
/// Larger than the default 64 KiB to reduce syscall overhead during
/// decompression of compressed OTA data on SATA SSDs.
const DECOMP_BUF_SIZE: usize = 256 * 1024;

/// Background thread decompressor for new data.
/// Matches C++ pthread behavior: background thread decompresses while
/// main thread writes to disk.
pub struct ParallelNewDataReader {
    receiver: Receiver<Vec<u8>>,
    buffer: Vec<u8>,
    buffer_pos: usize,
    total_received: usize,
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

        // Create channel for producer-consumer
        let (sender, receiver) = mpsc::channel::<Vec<u8>>();

        // Spawn background thread for decompression
        thread::spawn(move || {
            if let Err(e) = Self::decompressor_thread(file_path, ext, sender) {
                log::error!("background decompressor thread failed: {}", e);
            }
        });

        Ok(Self {
            receiver,
            buffer: Vec::new(),
            buffer_pos: 0,
            total_received: 0,
        })
    }

    /// Background thread: decompress data and send through channel.
    fn decompressor_thread(
        path: std::path::PathBuf,
        ext: String,
        sender: Sender<Vec<u8>>,
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

        // Read and send chunks
        loop {
            let mut chunk = vec![0u8; CHANNEL_CHUNK_SIZE];
            match reader.read(&mut chunk) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    chunk.truncate(n);
                    if sender.send(chunk).is_err() {
                        // Receiver dropped, exit thread
                        break;
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
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut filled = 0;

        while filled < buf.len() {
            // Refill if buffer empty
            if self.buffer_pos >= self.buffer.len() {
                self.refill_buffer()?;
            }

            // Copy from internal buffer
            let available = self.buffer.len() - self.buffer_pos;
            let needed = buf.len() - filled;
            let to_copy = available.min(needed);

            buf[filled..filled + to_copy]
                .copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);

            self.buffer_pos += to_copy;
            filled += to_copy;
        }

        Ok(())
    }

    /// Read blocks with background decompression.
    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        let len = (count as usize) * block_size;
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// Skip blocks (consume without returning).
    pub fn skip_blocks(&mut self, count: u64, block_size: usize) -> Result<()> {
        let mut len = (count as usize) * block_size;
        let mut buf = vec![0u8; DECOMP_BUF_SIZE.min(len)];
        while len > 0 {
            let to_read = len.min(buf.len());
            self.read_exact(&mut buf[..to_read])?;
            len -= to_read;
        }
        Ok(())
    }

    /// Get total bytes received so far.
    pub fn bytes_received(&self) -> usize {
        self.total_received
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
        src_ranges: &Option<RangeSet>,
        src_buffer_map: &Option<RangeSet>,
        stash_refs: &[(String, RangeSet)],
        src_block_count: u64,
    ) -> Result<Vec<u8>> {
        let mut buffer = vec![0u8; (src_block_count as usize) * self.block_size];

        if let Some(rs) = src_ranges {
            let data = if let Some(ref s) = self.source {
                s.read_ranges(rs)?
            } else {
                self.target.read_ranges(rs)?
            };

            if let Some(map) = src_buffer_map {
                let mut data_off = 0usize;
                for (start, end) in map.iter() {
                    let len = ((end - start) as usize) * self.block_size;
                    let buf_start = (start as usize) * self.block_size;
                    let buf_end = buf_start + len;

                    ensure!(buf_end <= buffer.len(), "buffer_map out of bounds");
                    ensure!(
                        data_off + len <= data.len(),
                        "buffer_map needs more data than src_ranges provides"
                    );

                    buffer[buf_start..buf_end].copy_from_slice(&data[data_off..data_off + len]);
                    data_off += len;
                }
            } else {
                ensure!(
                    data.len() <= buffer.len(),
                    "direct source data exceeds buffer"
                );
                buffer[..data.len()].copy_from_slice(&data);
            }
        }

        // 方案6: Stream stash reads — only load the specific ranges needed
        // instead of reading the entire stash file into memory.
        for (stash_id, map_ranges) in stash_refs {
            let stash_data = self
                .stash
                .load_ranges(stash_id, map_ranges, self.block_size)?;

            let mut stash_off = 0usize;
            for (start, end) in map_ranges.iter() {
                let len = ((end - start) as usize) * self.block_size;
                let buf_start = (start as usize) * self.block_size;
                let buf_end = buf_start + len;

                ensure!(buf_end <= buffer.len(), "stash ref map exceeds buffer");
                ensure!(
                    stash_off + len <= stash_data.len(),
                    "stash data too small for map"
                );

                buffer[buf_start..buf_end].copy_from_slice(&stash_data[stash_off..stash_off + len]);
                stash_off += len;
            }
        }

        Ok(buffer)
    }

    /// Load source blocks with minimal allocation.
    ///
    /// For simple cases (no stash refs, no buffer map), this avoids
    /// copying through an intermediate buffer by reading source ranges
    /// directly. Falls back to `load_src_blocks` for complex cases.
    ///
    /// 方案2/8: Zero-copy optimization for simple move commands.
    pub fn load_src_blocks_simple(&mut self, src_ranges: &RangeSet) -> Result<Vec<u8>> {
        if let Some(ref s) = self.source {
            s.read_ranges(src_ranges)
        } else {
            self.target.read_ranges(src_ranges)
        }
    }
}
