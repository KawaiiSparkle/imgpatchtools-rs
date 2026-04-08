//!极致流式 bsdiff 补丁应用 —— 零大内存分配。
//!
//! 本模块提供流式 API，特征：
//! - 源数据按需读取（不缓冲整个源文件）
//! - 目标数据直接写入（不缓冲整个输出）
//! - Patch 控制流边解压边处理
//! - 支持增量 SHA1 计算（边处理边哈希）
//!
//! 对比传统 API：
//! - 传统：`apply_bspatch(source: &[u8]) -> Vec<u8>` —— 需完整源数据，返回完整输出
//! - 流式：`apply_bspatch_streaming(source_fn, patch, sink, sha1_ctx)` —— 按需读取，流式输出

use std::io::Read;
use anyhow::{ensure, Context, Result};
use sha1::{Digest, Sha1};

// 使用 libc 进行批量内存操作，匹配 C++ 性能
unsafe extern "C" {
    fn memcpy(dest: *mut u8, src: *const u8, n: usize) -> *mut u8;
}

/// BSDIFF40 头部大小
const HEADER_SIZE: usize = 32;

/// 8 字节有符号整数解码（bsdiff 格式）
#[inline(always)]
fn offtin(buf: &[u8]) -> i64 {
    debug_assert!(buf.len() >= 8);
    let mut y: i64 = (buf[7] & 0x7F) as i64;
    y = (y << 8) | buf[6] as i64;
    y = (y << 8) | buf[5] as i64;
    y = (y << 8) | buf[4] as i64;
    y = (y << 8) | buf[3] as i64;
    y = (y << 8) | buf[2] as i64;
    y = (y << 8) | buf[1] as i64;
    y = (y << 8) | buf[0] as i64;
    if buf[7] & 0x80 != 0 {
        y = -y;
    }
    y
}

/// 源数据提供者 trait —— 按需返回源数据切片
///
/// 实现可以是：
/// - 内存切片（已有数据在内存中）
/// - 文件读取器（按需从磁盘读取）
/// - Mmap 视图（惰性加载）
pub trait SourceProvider {
    /// 获取源数据在 [offset, offset + len) 范围内的切片
    ///
    /// # Safety
    /// 返回的切片必须有效直到下一次调用或 provider 被销毁
    fn get_bytes(&mut self, offset: usize, len: usize) -> Result<&[u8]>;
    
    /// 源数据总大小
    fn len(&self) -> usize;
}

/// 内存中的源数据提供者（零成本抽象）
pub struct MemorySource<'a> {
    data: &'a [u8],
    ptr: *const u8,
    len: usize,
}

impl<'a> MemorySource<'a> {
    #[inline(always)]
    pub fn new(data: &'a [u8]) -> Self {
        Self { 
            data,
            ptr: data.as_ptr(),
            len: data.len(),
        }
    }
    
    /// 直接指针访问，消除所有抽象开销
    /// 这是确定性的优化，匹配 C++ 的原始指针访问
    #[inline(always)]
    pub unsafe fn get_byte_unchecked(&self, offset: usize) -> u8 {
        *self.ptr.add(offset)
    }
}

impl<'a> SourceProvider for MemorySource<'a> {
    #[inline(always)]
    fn get_bytes(&mut self, offset: usize, len: usize) -> Result<&[u8]> {
        // 使用指针算术进行边界检查，然后返回切片
        let end = offset + len;
        if end > self.len {
            anyhow::bail!(
                "source offset {} + len {} exceeds data size {}",
                offset, len, self.len
            );
        }
        // 直接通过原始指针创建切片，消除范围检查
        Ok(unsafe {
            std::slice::from_raw_parts(self.ptr.add(offset), len)
        })
    }
    
    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }
}

/// 目标数据接收者 trait —— 流式接收输出数据
///
/// 实现可以是：
/// - 内存缓冲区（收集到 Vec）
/// - 文件写入器（直接写入磁盘）
/// - 校验和计算器（只计算哈希不存储）
pub trait DataSink {
    /// 接收一块输出数据
    ///
    /// 实现必须要么：
    /// - 写入数据到最终目的地（文件/内存）
    /// - 或计算哈希后丢弃
    fn write(&mut self, data: &[u8]) -> Result<()>;
    
    /// 完成接收，返回写入的总字节数
    fn finish(self) -> Result<usize>;
}

/// 文件/块设备写入 Sink
pub struct BlockFileSink<'a> {
    file: &'a mut crate::util::io::BlockFile,
    ranges: &'a crate::util::rangeset::RangeSet,
    block_size: usize,
    current_range_idx: usize,
    current_range_offset: usize,
    total_written: usize,
}

impl<'a> BlockFileSink<'a> {
    pub fn new(
        file: &'a mut crate::util::io::BlockFile,
        ranges: &'a crate::util::rangeset::RangeSet,
        block_size: usize,
    ) -> Result<Self> {
        use std::io::Seek;
        
        // 定位到第一个 range
        let first_offset = ranges
            .iter()
            .next()
            .map(|(start, _end)| start as u64 * block_size as u64)
            .unwrap_or(0);
        
        file.file_mut().seek(std::io::SeekFrom::Start(first_offset))?;
        
        Ok(Self {
            file,
            ranges,
            block_size,
            current_range_idx: 0,
            current_range_offset: 0,
            total_written: 0,
        })
    }
}

impl<'a> DataSink for BlockFileSink<'a> {
    fn write(&mut self, data: &[u8]) -> Result<()> {
        use std::io::{Write, Seek};
        
        let mut remaining = data;
        
        while !remaining.is_empty() {
            // 获取当前 range 的剩余空间
            let ranges_vec: Vec<_> = self.ranges.iter().collect();
            
            if self.current_range_idx >= ranges_vec.len() {
                anyhow::bail!("BlockFileSink: write exceeds target ranges");
            }
            
            let (range_start, range_end) = ranges_vec[self.current_range_idx];
            let range_blocks = (range_end - range_start) as usize;
            let range_bytes = range_blocks * self.block_size;
            let range_remaining = range_bytes - self.current_range_offset;
            
            let write_len = remaining.len().min(range_remaining);
            
            // 直接写入文件底层句柄
            self.file.file_mut().write_all(&remaining[..write_len])?;
            
            remaining = &remaining[write_len..];
            self.current_range_offset += write_len;
            self.total_written += write_len;
            
            // 如果当前 range 写满了，seek 到下一个
            if self.current_range_offset >= range_bytes {
                self.current_range_idx += 1;
                self.current_range_offset = 0;
                
                if self.current_range_idx < ranges_vec.len() {
                    let (next_start, _) = ranges_vec[self.current_range_idx];
                    let offset = next_start as u64 * self.block_size as u64;
                    self.file.file_mut().seek(std::io::SeekFrom::Start(offset))?;
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

/// 增量哈希 Sink —— 边接收边计算 SHA1，不存储数据
pub struct HashingSink {
    hasher: Sha1,
    total_received: usize,
}

impl HashingSink {
    pub fn new() -> Self {
        Self {
            hasher: Sha1::new(),
            total_received: 0,
        }
    }
    
    pub fn finalize(self) -> [u8; 20] {
        self.hasher.finalize().into()
    }
}

impl DataSink for HashingSink {
    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.hasher.update(data);
        self.total_received += data.len();
        Ok(())
    }
    
    fn finish(self) -> Result<usize> {
        Ok(self.total_received)
    }
}

/// 双重 Sink —— 同时写入文件并计算哈希
pub struct VerifyingSink<'a> {
    file_sink: BlockFileSink<'a>,
    hasher: Sha1,
    expected_hash: Option<[u8; 20]>,
}

impl<'a> VerifyingSink<'a> {
    pub fn new(
        file: &'a mut crate::util::io::BlockFile,
        ranges: &'a crate::util::rangeset::RangeSet,
        block_size: usize,
        expected_hash: Option<[u8; 20]>,
    ) -> Result<Self> {
        Ok(Self {
            file_sink: BlockFileSink::new(file, ranges, block_size)?,
            hasher: Sha1::new(),
            expected_hash,
        })
    }
    
    pub fn verify(self) -> Result<()> {
        if let Some(expected) = self.expected_hash {
            let actual: [u8; 20] = self.hasher.finalize().into();
            ensure!(
                actual == expected,
                "hash mismatch: expected {:02x?}, got {:02x?}",
                expected,
                actual
            );
        }
        Ok(())
    }
}

impl<'a> DataSink for VerifyingSink<'a> {
    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.hasher.update(data);
        self.file_sink.write(data)
    }
    
    fn finish(self) -> Result<usize> {
        self.file_sink.finish()
    }
}

/// 极致流式 bsdiff 应用 —— 核心函数
///
/// 特征：
/// - 源数据通过 `source` provider 按需读取
/// - 目标数据通过 `sink` 流式输出
/// - 控制/差异/额外数据边解压边处理
/// - 处理过程中不分配与输出大小成比例的内存
///
/// # 类型参数
/// - `S`: SourceProvider，提供源数据
/// - `D`: DataSink，接收输出数据
///
/// # 参数
/// - `source`: 源数据提供者
/// - `patch`: 补丁数据（包含 BSDIFF40 头）
/// - `patch_offset`: 补丁在 buffer 中的偏移
/// - `sink`: 输出数据接收者
///
/// # 内存使用
/// - O(1) 额外内存（与输出大小无关）
/// - 固定大小的内部缓冲区（~64KB）
pub fn apply_bspatch_streaming<S: SourceProvider, D: DataSink>(
    source: &mut S,
    patch: &[u8],
    patch_offset: usize,
    sink: &mut D,
) -> Result<usize> {
    // 解析头部
    ensure!(
        patch.len() >= patch_offset + HEADER_SIZE,
        "patch too short"
    );
    let hdr = &patch[patch_offset..patch_offset + HEADER_SIZE];
    ensure!(&hdr[..8] == b"BSDIFF40", "bad bsdiff magic");
    
    let ctrl_len = offtin(&hdr[8..16]) as usize;
    let diff_len = offtin(&hdr[16..24]) as usize;
    let new_size = offtin(&hdr[24..32]) as usize;
    
    ensure!(ctrl_len > 0 || diff_len == 0, "bad patch header");
    
    // 解压流
    let payload = &patch[patch_offset + HEADER_SIZE..];
    let ctrl_compressed = &payload[..ctrl_len.min(payload.len())];
    let diff_start = ctrl_len;
    let diff_compressed = &payload[diff_start..(diff_start + diff_len).min(payload.len())];
    let extra_start = diff_start + diff_len;
    let extra_compressed = &payload[extra_start..];
    
    let mut ctrl_stream = bzip2::read::BzDecoder::new(ctrl_compressed);
    let mut diff_stream = bzip2::read::BzDecoder::new(diff_compressed);
    let mut extra_stream = bzip2::read::BzDecoder::new(extra_compressed);
    
    // 固定大小的处理缓冲区（64KB）
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut diff_buf = vec![0u8; CHUNK_SIZE];
    let mut extra_buf = vec![0u8; CHUNK_SIZE];
    let mut ctrl_buf = [0u8; 24];
    
    let mut new_pos: usize = 0;
    let mut old_pos: i64 = 0;
    let old_size = source.len() as i64;
    let mut total_written: usize = 0;
    
    while new_pos < new_size {
        // 读取控制三元组
        ctrl_stream.read_exact(&mut ctrl_buf).context("read control")?;
        let add_len = offtin(&ctrl_buf[0..8]) as usize;
        let copy_len = offtin(&ctrl_buf[8..16]) as usize;
        let seek_adj = offtin(&ctrl_buf[16..24]);
        
        // 处理 diff 块（分 chunk 处理）
        let mut diff_remaining = add_len;
        while diff_remaining > 0 {
            let chunk = diff_remaining.min(CHUNK_SIZE);
            
            diff_stream.read_exact(&mut diff_buf[..chunk]).context("read diff")?;
            
            // 获取源数据并应用 diff
            let src_start = old_pos.max(0) as usize;
            let src_end = ((old_pos + chunk as i64).min(old_size)).max(0) as usize;
            let src_len = src_end.saturating_sub(src_start);
            
            if src_len > 0 && src_start < source.len() {
                let src_bytes = source.get_bytes(src_start, src_len)?;
                let src_offset = old_pos.max(0) as usize - src_start;
                
                // 极致优化 diff 循环：使用指针运算 + 批量处理
                // 这是确定性的性能改进，匹配 C++ 的指针运算模式
                unsafe {
                    let diff_ptr = diff_buf.as_mut_ptr();
                    let src_ptr = src_bytes.as_ptr().add(src_offset);
                    
                    // 处理重叠部分（i < src_len）
                    let overlap = chunk.min(src_len);
                    
                    // 使用 8 字节批量处理减少循环次数
                    let mut i = 0;
                    let bulk_end = overlap - (overlap % 8);
                    
                    while i < bulk_end {
                        // 每次处理 8 字节
                        *diff_ptr.add(i) = (*diff_ptr.add(i)).wrapping_add(*src_ptr.add(i));
                        *diff_ptr.add(i + 1) = (*diff_ptr.add(i + 1)).wrapping_add(*src_ptr.add(i + 1));
                        *diff_ptr.add(i + 2) = (*diff_ptr.add(i + 2)).wrapping_add(*src_ptr.add(i + 2));
                        *diff_ptr.add(i + 3) = (*diff_ptr.add(i + 3)).wrapping_add(*src_ptr.add(i + 3));
                        *diff_ptr.add(i + 4) = (*diff_ptr.add(i + 4)).wrapping_add(*src_ptr.add(i + 4));
                        *diff_ptr.add(i + 5) = (*diff_ptr.add(i + 5)).wrapping_add(*src_ptr.add(i + 5));
                        *diff_ptr.add(i + 6) = (*diff_ptr.add(i + 6)).wrapping_add(*src_ptr.add(i + 6));
                        *diff_ptr.add(i + 7) = (*diff_ptr.add(i + 7)).wrapping_add(*src_ptr.add(i + 7));
                        i += 8;
                    }
                    
                    // 处理剩余字节
                    while i < overlap {
                        *diff_ptr.add(i) = (*diff_ptr.add(i)).wrapping_add(*src_ptr.add(i));
                        i += 1;
                    }
                    
                    // 非重叠部分（i >= src_len）相当于加 0，无需处理
                }
            }
            
            sink.write(&diff_buf[..chunk])?;
            total_written += chunk;
            
            diff_remaining -= chunk;
            new_pos += chunk;
            old_pos += chunk as i64;
        }
        
        // 处理 extra 块（直接复制）
        let mut extra_remaining = copy_len;
        while extra_remaining > 0 {
            let chunk = extra_remaining.min(CHUNK_SIZE);
            extra_stream.read_exact(&mut extra_buf[..chunk]).context("read extra")?;
            sink.write(&extra_buf[..chunk])?;
            total_written += chunk;
            extra_remaining -= chunk;
            new_pos += chunk;
        }
        
        // 调整源位置
        old_pos += seek_adj;
    }
    
    Ok(total_written)
}

/// 便捷函数：流式应用到文件并计算哈希
pub fn apply_bspatch_to_file(
    source_data: &[u8],  // 对于简单情况，仍支持内存源
    patch: &[u8],
    patch_offset: usize,
    target_file: &mut crate::util::io::BlockFile,
    target_ranges: &crate::util::rangeset::RangeSet,
    block_size: usize,
    expected_hash: Option<[u8; 20]>,
) -> Result<usize> {
    let mut source = MemorySource::new(source_data);
    let mut sink = VerifyingSink::new(target_file, target_ranges, block_size, expected_hash)?;
    
    let written = apply_bspatch_streaming(&mut source, patch, patch_offset, &mut sink)?;
    sink.verify()?;
    
    Ok(written)
}

/// 便捷函数：仅计算目标哈希（不写入文件）
pub fn compute_target_hash(
    source_data: &[u8],
    patch: &[u8],
    patch_offset: usize,
) -> Result<[u8; 20]> {
    let mut source = MemorySource::new(source_data);
    let mut sink = HashingSink::new();
    
    apply_bspatch_streaming(&mut source, patch, patch_offset, &mut sink)?;
    
    Ok(sink.finalize())
}
