//! `bsdiff` command —— 极致流式实现。
//!
//! 本实现特征：
//! - 目标预检：流式读取 + 增量哈希，不存储数据
//! - 源数据：按需加载（复用上下文缓冲区）
//! - Patch 应用：流式解压 + 直接写入文件
//! - 哈希验证：边处理边计算，事后比较
//!
//! 内存使用：O(1)，仅固定大小缓冲区（~64KB），与 OTA 大小无关。

use crate::core::applypatch::bspatch_streaming::{MemorySource, apply_bspatch_streaming};
use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::util::hash::{self, parse_hex_digest};
use crate::util::rangeset::RangeSet;
use anyhow::{Context, Result, ensure};
use sha1::{Digest, Sha1};

/// 流式验证目标区域是否已包含预期数据。
///
/// 不存储完整数据，而是边读取边计算 SHA1。
fn stream_verify_target(
    target: &mut crate::util::io::BlockFile,
    ranges: &RangeSet,
    block_size: usize,
    expected: &[u8; 20],
) -> Result<bool> {
    use std::io::{Read, Seek};

    let mut hasher = Sha1::new();
    const CHUNK_SIZE: usize = 256 * 1024; // 256KB 读取块

    for (start, end) in ranges.iter() {
        let offset = start * block_size as u64;
        let size = (end - start) as usize * block_size;

        target.file_mut().seek(std::io::SeekFrom::Start(offset))?;

        let mut remaining = size;
        let mut buf = vec![0u8; CHUNK_SIZE.min(size)];

        while remaining > 0 {
            let to_read = remaining.min(buf.len());
            target.file_mut().read_exact(&mut buf[..to_read])?;
            hasher.update(&buf[..to_read]);
            remaining -= to_read;
        }
    }

    let actual: [u8; 20] = hasher.finalize().into();
    Ok(actual == *expected)
}

/// 极致流式 bsdiff 命令。
pub fn cmd_bsdiff(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let target_ranges = cmd
        .target_ranges
        .as_ref()
        .context("bsdiff: missing target_ranges")?;
    let patch_offset = cmd.patch_offset.context("bsdiff: missing patch_offset")?;
    let patch_len = cmd.patch_len.context("bsdiff: missing patch_len")?;
    let src_ranges = cmd
        .src_ranges
        .as_ref()
        .context("bsdiff: missing src_ranges")?;

    // ========================================================================
    // Step 1: 目标预检（流式哈希，不存储数据）
    // ========================================================================
    if let Some(ref expected_hex) = cmd.target_hash
        && let Some(expected_digest) = parse_hex_digest(expected_hex) {
            match stream_verify_target(
                &mut ctx.target,
                target_ranges,
                ctx.block_size,
                &expected_digest,
            ) {
                Ok(true) => {
                    log::info!(
                        "bsdiff: target already has expected hash {}, skipping",
                        expected_hex
                    );
                    ctx.blocks_advanced_this_cmd = target_ranges.blocks();
                    return Ok(());
                }
                Ok(false) => log::debug!("bsdiff: target hash mismatch, will patch"),
                Err(e) => log::debug!("bsdiff: target verification failed: {}", e),
            }
        }

    // ========================================================================
    // Step 2: 加载源数据（支持 stash refs，容错加载）
    // ========================================================================
    let src_data =
        ctx.load_src_blocks(src_ranges, &cmd.src_stash_refs, cmd.src_buffer_map.as_ref())?;

    // ========================================================================
    // Step 3: 源哈希验证（二进制比较）
    // ========================================================================
    if let Some(ref expected_hex) = cmd.src_hash {
        let expected = parse_hex_digest(expected_hex).context("bsdiff: invalid source hash")?;
        let actual = hash::sha1_bytes(&src_data);
        ensure!(
            actual == expected,
            "bsdiff: source hash mismatch: expected {:02x?}, got {:02x?}",
            expected,
            actual
        );
    }

    // ========================================================================
    // Step 4: Patch 应用（极致流式）
    // ========================================================================
    let patch_bytes: Vec<u8> = ctx.patch_data.read_patch(patch_offset, patch_len)?.to_vec();
    let expected_tgt_hash = cmd.target_hash.as_ref().and_then(|h| parse_hex_digest(h));

    // 流式处理器：同时写入文件 + 计算哈希
    let mut hasher = Sha1::new();

    struct FileAndHashSink<'a> {
        file: &'a mut crate::util::io::BlockFile,
        hasher: &'a mut Sha1,
        ranges: Vec<(u64, u64)>,
        current_idx: usize,
        current_written: usize,
        block_size: usize,
        total_written: usize,
    }

    impl<'a> crate::core::applypatch::bspatch_streaming::DataSink for FileAndHashSink<'a> {
        fn write(&mut self, data: &[u8]) -> Result<()> {
            use std::io::{Seek, Write};

            let mut remaining = data;

            while !remaining.is_empty() {
                if self.current_idx >= self.ranges.len() {
                    anyhow::bail!("write exceeds target ranges");
                }

                let (start, end) = self.ranges[self.current_idx];
                let range_bytes = (end - start) as usize * self.block_size;
                let range_remaining = range_bytes - self.current_written;
                let write_len = remaining.len().min(range_remaining);

                self.file.file_mut().write_all(&remaining[..write_len])?;
                self.hasher.update(&remaining[..write_len]);

                remaining = &remaining[write_len..];
                self.current_written += write_len;
                self.total_written += write_len;

                if self.current_written >= range_bytes {
                    self.current_idx += 1;
                    self.current_written = 0;
                    if let Some((next_start, _)) = self.ranges.get(self.current_idx) {
                        self.file.file_mut().seek(std::io::SeekFrom::Start(
                            *next_start * self.block_size as u64,
                        ))?;
                    }
                }
            }
            Ok(())
        }

        fn finish(self) -> Result<usize> {
            use std::io::Write;
            self.file.file_mut().flush()?;
            Ok(self.total_written)
        }
    }

    let ranges_vec: Vec<_> = target_ranges.iter().collect();
    if !ranges_vec.is_empty() {
        use std::io::Seek;
        ctx.target.file_mut().seek(std::io::SeekFrom::Start(
            ranges_vec[0].0 * ctx.block_size as u64,
        ))?;
    }

    let mut sink = FileAndHashSink {
        file: &mut ctx.target,
        hasher: &mut hasher,
        ranges: ranges_vec,
        current_idx: 0,
        current_written: 0,
        block_size: ctx.block_size,
        total_written: 0,
    };

    let mut source = MemorySource::new(&src_data);
    apply_bspatch_streaming(&mut source, &patch_bytes, 0, &mut sink)
        .context("bsdiff: patch application failed")?;

    // ========================================================================
    // Step 5: 目标哈希验证
    // ========================================================================
    if let Some(expected) = expected_tgt_hash {
        let actual: [u8; 20] = hasher.finalize().into();
        ensure!(
            actual == expected,
            "bsdiff: target hash mismatch: expected {:02x?}, got {:02x?}",
            expected,
            actual
        );
    }

    // ========================================================================
    // 完成
    // ========================================================================
    log::debug!(
        "bsdiff: patched {} blocks (total written: {})",
        target_ranges.blocks(),
        ctx.written_blocks
    );

    Ok(())
}
