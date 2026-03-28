use anyhow::{ensure, Result};
use std::io::Read;
use std::fs::File;
use crate::util::rangeset::RangeSet;
use crate::util::io::BlockFile;
use crate::util::progress::ProgressReporter;

pub struct NewDataReader {
    reader: Box<dyn Read>,
}

impl NewDataReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let file = File::open(path)?;
        let reader: Box<dyn Read> = if path.extension().map_or(false, |e| e == "br") {
            Box::new(brotli::Decompressor::new(file, 4096))
        } else {
            Box::new(file)
        };
        Ok(Self { reader })
    }

    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        let len = (count as usize) * block_size;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

pub struct PatchDataReader {
    mmap: memmap2::Mmap,
}

impl PatchDataReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self { mmap })
    }

    pub fn read_patch(&self, offset: u64, len: u64) -> Result<&[u8]> {
        let start = offset as usize;
        let end = start + (len as usize);
        ensure!(end <= self.mmap.len(), "patch read out of bounds");
        Ok(&self.mmap[start..end])
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

        // ---- 1) Fill from direct source ranges ----
        if let Some(rs) = src_ranges {
            let data = if let Some(ref s) = self.source {
                s.read_ranges(rs)?
            } else {
                self.target.read_ranges(rs)?
            };

            if let Some(map) = src_buffer_map {
                // Place `data` into `buffer` following map's block positions.
                let mut data_off = 0usize;
                for (start, end) in map.iter() {
                    let len = ((end - start) as usize) * self.block_size;
                    let buf_start = (start as usize) * self.block_size;
                    let buf_end = buf_start + len;

                    ensure!(buf_end <= buffer.len(), "buffer_map out of bounds");
                    ensure!(data_off + len <= data.len(), "buffer_map needs more data than src_ranges provides");

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
                // No map => AOSP behaviour is: fill from start.
                ensure!(data.len() <= buffer.len(), "direct source data exceeds buffer");
                buffer[..data.len()].copy_from_slice(&data);
            }
        }

        // ---- 2) Overlay stash refs (buffer positions) ----
        for (stash_id, map_ranges) in stash_refs {
            let stash_data = self.stash.load(stash_id)?;

            let mut stash_off = 0usize;
            for (start, end) in map_ranges.iter() {
                let len = ((end - start) as usize) * self.block_size;
                let buf_start = (start as usize) * self.block_size;
                let buf_end = buf_start + len;

                ensure!(buf_end <= buffer.len(), "stash ref map exceeds buffer");
                ensure!(stash_off + len <= stash_data.len(), "stash data too small for map");

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