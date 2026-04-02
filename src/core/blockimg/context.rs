use crate::util::io::BlockFile;
use crate::util::progress::ProgressReporter;
use crate::util::rangeset::RangeSet;
use anyhow::{ensure, Result};
use std::fs::File;
use std::io::Read;

pub struct NewDataReader {
    reader: Box<dyn Read>,
}

impl NewDataReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
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
                // Auto-fallback: check if a compressed version exists.
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

        // Attach appropriate streaming decompressor
        let reader: Box<dyn Read> = match ext.as_str() {
            "br" => {
                log::info!("using Brotli streaming decompressor for new data");
                Box::new(brotli::Decompressor::new(file, 65536))
            }
            "lzma" => {
                log::info!("using LZMA streaming decompressor for new data");
                // xz2 wraps liblzma; new_lzma_decoder handles raw .lzma format
                let stream = xz2::stream::Stream::new_lzma_decoder(u64::MAX)
                    .map_err(|e| anyhow::anyhow!("failed to create LZMA decoder: {}", e))?;
                Box::new(xz2::read::XzDecoder::new_stream(
                    std::io::BufReader::with_capacity(65536, file),
                    stream,
                ))
            }
            "xz" => {
                log::info!("using XZ streaming decompressor for new data");
                Box::new(xz2::read::XzDecoder::new(
                    std::io::BufReader::with_capacity(65536, file),
                ))
            }
            _ => Box::new(file),
        };

        Ok(Self { reader })
    }

    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        let len = (count as usize) * block_size;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// 用于断点续传时跳过已完成命令的数据
    pub fn skip_blocks(&mut self, count: u64, block_size: usize) -> Result<()> {
        let mut len = (count as usize) * block_size;
        let mut buf = vec![0u8; 65536.min(len)];
        while len > 0 {
            let to_read = len.min(buf.len());
            self.reader.read_exact(&mut buf[..to_read])?;
            len -= to_read;
        }
        Ok(())
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
    pub new_data: NewDataReader,
    pub patch_data: PatchDataReader,
    pub written_blocks: u64,
    pub progress: Box<dyn ProgressReporter>,
}

impl CommandContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u32,
        block_size: usize,
        target: BlockFile,
        source: Option<BlockFile>,
        stash: crate::core::blockimg::stash::StashManager,
        new_data: NewDataReader,
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
        }
    }

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
                ensure!(
                    data_off == data.len(),
                    "buffer_map did not consume all direct source data: consumed {}, data len {}",
                    data_off,
                    data.len()
                );
            } else {
                ensure!(
                    data.len() <= buffer.len(),
                    "direct source data exceeds buffer"
                );
                buffer[..data.len()].copy_from_slice(&data);
            }
        }

        for (stash_id, map_ranges) in stash_refs {
            let stash_data = self.stash.load(stash_id)?;

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
            ensure!(
                stash_off == stash_data.len(),
                "stash ref map did not consume full stash data: consumed {}, stash len {}",
                stash_off,
                stash_data.len()
            );
        }

        Ok(buffer)
    }
}
