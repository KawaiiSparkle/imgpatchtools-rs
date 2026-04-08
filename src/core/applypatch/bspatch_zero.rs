//! 零拷贝 bsdiff 补丁应用 —— SinkFn 风格 API + 直接解压到输出缓冲区
//!
//! 本模块特性：
//! - SinkFn 风格的流式输出 API（类似 AOSP bsdiff::bspatch）
//! - diff 数据直接解压到输出缓冲区，原地计算（零临时缓冲）
//! - extra 数据直接解压到输出缓冲区
//! - 支持增量 SHA1 计算
//!
//! 相比 bspatch_raw.rs 的改进：
//! - 零拷贝：diff/extra 直接解压到最终输出位置
//! - SinkFn API：调用者控制输出（文件写入、内存、网络等）
//! - 增量哈希：边处理边计算 SHA1，无需事后遍历

use anyhow::{ensure, bail, Result};
use bzip2_sys::*;
use sha1::{Digest, Sha1};

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

/// 零拷贝 bzip2 解压器 —— 直接解压到目标缓冲区
///
/// 封装 bz_stream，支持直接解压到任意内存位置
struct ZeroCopyBzDecoder {
    stream: bz_stream,
    input: *const u8,
    input_len: usize,
    input_pos: usize,
}

impl ZeroCopyBzDecoder {
    /// 从压缩数据创建解码器
    fn new(compressed: &[u8]) -> Result<Self> {
        let mut stream: bz_stream = unsafe { std::mem::zeroed() };
        
        let ret = unsafe {
            BZ2_bzDecompressInit(&mut stream, 0, 0)
        };
        
        ensure!(ret == BZ_OK as i32, "BZ2_bzDecompressInit failed: {}", ret);
        
        Ok(Self {
            stream,
            input: compressed.as_ptr(),
            input_len: compressed.len(),
            input_pos: 0,
        })
    }
    
    /// 解压指定数量的字节到目标缓冲区
    /// 
    /// 会一直解压直到填满整个 `output` 或流结束
    /// 返回实际解压的字节数（在成功时应等于 output.len()）
    fn decompress_exact(&mut self, output: &mut [u8]) -> Result<()> {
        let target_len = output.len();
        let mut total_produced = 0;
        
        while total_produced < target_len {
            // 设置输入
            let remaining_input = self.input_len - self.input_pos;
            self.stream.next_in = unsafe { self.input.add(self.input_pos) } as *mut _;
            self.stream.avail_in = remaining_input as u32;
            
            // 设置输出到目标位置
            self.stream.next_out = unsafe { output.as_mut_ptr().add(total_produced) } as *mut _;
            self.stream.avail_out = (target_len - total_produced) as u32;
            
            // 执行解压
            let ret = unsafe {
                BZ2_bzDecompress(&mut self.stream)
            };
            
            // 更新输入位置
            let consumed = remaining_input - self.stream.avail_in as usize;
            self.input_pos += consumed;
            
            // 计算本次输出
            let produced = (target_len - total_produced) - self.stream.avail_out as usize;
            total_produced += produced;
            
            match ret {
                n if n == BZ_OK as i32 => {
                    // 需要更多数据或输出空间
                    if produced == 0 && self.input_pos >= self.input_len {
                        bail!("Unexpected end of compressed stream");
                    }
                }
                n if n == BZ_STREAM_END as i32 => {
                    // 流正常结束
                    if total_produced < target_len {
                        bail!("Compressed stream ended early: got {}, expected {}", 
                              total_produced, target_len);
                    }
                    return Ok(());
                }
                n => bail!("BZ2_bzDecompress failed: {}", n),
            }
        }
        
        Ok(())
    }
    
    /// 完成解压（清理资源）
    fn finish(&mut self) -> Result<()> {
        // 尝试排空任何剩余输出
        let mut dummy = [0u8; 256];
        loop {
            self.stream.next_in = std::ptr::null_mut();
            self.stream.avail_in = 0;
            self.stream.next_out = dummy.as_mut_ptr() as *mut _;
            self.stream.avail_out = dummy.len() as u32;
            
            let ret = unsafe { BZ2_bzDecompress(&mut self.stream) };
            
            if ret == BZ_STREAM_END as i32 {
                return Ok(());
            }
            if ret != BZ_OK as i32 {
                bail!("BZ2_bzDecompress finish failed: {}", ret);
            }
            if self.stream.avail_out == dummy.len() as u32 {
                return Ok(());
            }
        }
    }
}

impl Drop for ZeroCopyBzDecoder {
    fn drop(&mut self) {
        unsafe {
            BZ2_bzDecompressEnd(&mut self.stream);
        }
    }
}

/// SinkFn 风格的 bsdiff 应用 —— 零拷贝实现
///
/// 参数：
/// - `source`: 源数据
/// - `patch`: 补丁数据
/// - `patch_offset`: 补丁起始偏移
/// - `sink`: 输出回调函数，接收 (data, ctx) -> Result<()>
/// - `ctx`: 传递给 sink 的上下文
/// - `sha1_ctx`: 可选的 SHA1 计算器（增量更新）
///
/// 示例：
/// ```rust,ignore
/// let mut output_file = File::create("output.bin")?;
/// let mut sha1 = Sha1::new();
/// 
/// apply_bspatch_zero_sink(
///     source,
///     patch,
///     0,
///     |data| output_file.write_all(data),
///     &mut (),
///     Some(&mut sha1),
/// )?;
/// ```
pub fn apply_bspatch_zero_sink<Sink, Ctx>(
    source: &[u8],
    patch: &[u8],
    patch_offset: usize,
    mut sink: Sink,
    _ctx: &mut Ctx,
    mut sha1_ctx: Option<&mut Sha1>,
) -> Result<()>
where
    Sink: FnMut(&[u8]) -> std::io::Result<()>,
{
    // 解析头部
    ensure!(
        patch.len() >= patch_offset + HEADER_SIZE,
        "patch too short: need {} bytes, have {}",
        patch_offset + HEADER_SIZE,
        patch.len()
    );
    
    let hdr = &patch[patch_offset..patch_offset + HEADER_SIZE];
    ensure!(&hdr[..8] == b"BSDIFF40", "bad bsdiff magic");
    
    let ctrl_len = offtin(&hdr[8..16]) as usize;
    let diff_len = offtin(&hdr[16..24]) as usize;
    let new_size = offtin(&hdr[24..32]) as usize;
    
    ensure!(ctrl_len > 0 || diff_len == 0, "bad patch header");
    
    // 设置三个压缩流的输入数据
    let payload = &patch[patch_offset + HEADER_SIZE..];
    let ctrl_compressed = &payload[..ctrl_len.min(payload.len())];
    let diff_start = ctrl_len;
    let diff_compressed = &payload[diff_start..(diff_start + diff_len).min(payload.len())];
    let extra_start = diff_start + diff_len;
    let extra_compressed = &payload[extra_start..];
    
    // 创建解码器
    let mut ctrl_dec = ZeroCopyBzDecoder::new(ctrl_compressed)?;
    let mut diff_dec = ZeroCopyBzDecoder::new(diff_compressed)?;
    let mut extra_dec = ZeroCopyBzDecoder::new(extra_compressed)?;
    
    // 预分配输出缓冲（一次性，复用）
    // 选择 64KB 或 new_size 的较小值
    const MAX_BUF_SIZE: usize = 64 * 1024;
    let buf_size = new_size.min(MAX_BUF_SIZE);
    let mut output_buf = vec![0u8; buf_size];
    
    let mut new_pos: usize = 0;
    let mut old_pos: i64 = 0;
    let old_size = source.len() as i64;
    let mut ctrl_buf = [0u8; 24];
    
    while new_pos < new_size {
        // 读取控制三元组（24 字节）
        ctrl_dec.decompress_exact(&mut ctrl_buf)?;
        
        let add_len = offtin(&ctrl_buf[0..8]) as usize;
        let copy_len = offtin(&ctrl_buf[8..16]) as usize;
        let seek_adj = offtin(&ctrl_buf[16..24]);
        
        // 处理 diff 块（零拷贝：直接解压到输出缓冲，原地计算）
        let mut diff_remaining = add_len;
        while diff_remaining > 0 {
            let chunk = diff_remaining.min(buf_size);
            let buf_slice = &mut output_buf[..chunk];
            
            // 直接解压到输出缓冲区
            diff_dec.decompress_exact(buf_slice)?;
            
            // 原地 diff 计算
            let src_start = old_pos.max(0) as usize;
            let src_end = ((old_pos + chunk as i64).min(old_size)).max(0) as usize;
            let src_len = src_end.saturating_sub(src_start);
            
            if src_len > 0 {
                unsafe {
                    let diff_ptr = buf_slice.as_mut_ptr();
                    let src_ptr = source.as_ptr().add(src_start);
                    let overlap = chunk.min(src_len);
                    
                    // 8字节批量处理
                    let bulk_end = overlap - (overlap % 8);
                    let mut i = 0;
                    while i < bulk_end {
                        *diff_ptr.add(i) = (*diff_ptr.add(i)).wrapping_add(*src_ptr.add(i));
                        *diff_ptr.add(i+1) = (*diff_ptr.add(i+1)).wrapping_add(*src_ptr.add(i+1));
                        *diff_ptr.add(i+2) = (*diff_ptr.add(i+2)).wrapping_add(*src_ptr.add(i+2));
                        *diff_ptr.add(i+3) = (*diff_ptr.add(i+3)).wrapping_add(*src_ptr.add(i+3));
                        *diff_ptr.add(i+4) = (*diff_ptr.add(i+4)).wrapping_add(*src_ptr.add(i+4));
                        *diff_ptr.add(i+5) = (*diff_ptr.add(i+5)).wrapping_add(*src_ptr.add(i+5));
                        *diff_ptr.add(i+6) = (*diff_ptr.add(i+6)).wrapping_add(*src_ptr.add(i+6));
                        *diff_ptr.add(i+7) = (*diff_ptr.add(i+7)).wrapping_add(*src_ptr.add(i+7));
                        i += 8;
                    }
                    // 剩余字节
                    while i < overlap {
                        *diff_ptr.add(i) = (*diff_ptr.add(i)).wrapping_add(*src_ptr.add(i));
                        i += 1;
                    }
                }
            }
            
            // 输出到 sink
            sink(buf_slice).map_err(|e| anyhow::anyhow!("sink error: {}", e))?;
            
            // 增量哈希
            if let Some(ref mut ctx) = sha1_ctx {
                ctx.update(buf_slice);
            }
            
            diff_remaining -= chunk;
            new_pos += chunk;
            old_pos += chunk as i64;
        }
        
        // 处理 extra 块（直接解压到输出缓冲）
        let mut extra_remaining = copy_len;
        while extra_remaining > 0 {
            let chunk = extra_remaining.min(buf_size);
            let buf_slice = &mut output_buf[..chunk];
            
            // 直接解压到输出缓冲区
            extra_dec.decompress_exact(buf_slice)?;
            
            // 输出到 sink
            sink(buf_slice).map_err(|e| anyhow::anyhow!("sink error: {}", e))?;
            
            // 增量哈希
            if let Some(ref mut ctx) = sha1_ctx {
                ctx.update(buf_slice);
            }
            
            extra_remaining -= chunk;
            new_pos += chunk;
        }
        
        // 调整源位置
        old_pos += seek_adj;
    }
    
    // 完成解压
    ctrl_dec.finish()?;
    diff_dec.finish()?;
    extra_dec.finish()?;
    
    Ok(())
}

/// 零拷贝 bsdiff 应用到内存 Vec（便捷函数）
///
/// 内部使用预分配 Vec 作为 sink，避免重复分配
pub fn apply_bspatch_zero_vec(
    source: &[u8],
    patch: &[u8],
    patch_offset: usize,
) -> Result<Vec<u8>> {
    // 解析头部获取输出大小
    ensure!(
        patch.len() >= patch_offset + HEADER_SIZE,
        "patch too short"
    );
    let hdr = &patch[patch_offset..patch_offset + HEADER_SIZE];
    ensure!(&hdr[..8] == b"BSDIFF40", "bad bsdiff magic");
    
    let new_size = offtin(&hdr[24..32]) as usize;
    let mut output = Vec::with_capacity(new_size);
    
    // 使用 unsafe 直接写入 Vec 内部（零拷贝）
    {
        let mut pos = 0;
        apply_bspatch_zero_sink(
            source,
            patch,
            patch_offset,
            |data: &[u8]| {
                let len = data.len();
                // 扩展 Vec 到足够大小
                if output.len() < pos + len {
                    output.resize(pos + len, 0);
                }
                // 直接复制数据
                output[pos..pos + len].copy_from_slice(data);
                pos += len;
                Ok(())
            },
            &mut (),
            None::<&mut Sha1>,
        )?;
    }
    
    Ok(output)
}

/// 零拷贝 bsdiff 应用到文件（便捷函数）
///
/// 同时计算 SHA1 哈希（增量）
pub fn apply_bspatch_zero_file(
    source: &[u8],
    patch: &[u8],
    patch_offset: usize,
    output_path: &std::path::Path,
) -> Result<[u8; 20]> {
    use std::io::Write;
    
    let mut file = std::fs::File::create(output_path)?;
    let mut sha1 = Sha1::new();
    
    apply_bspatch_zero_sink(
        source,
        patch,
        patch_offset,
        |data: &[u8]| file.write_all(data),
        &mut (),
        Some(&mut sha1),
    )?;
    
    let hash = sha1.finalize();
    Ok(hash.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_offtin() {
        let buf = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(offtin(&buf), 1);
        
        let buf2 = [0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(offtin(&buf2), 256);
    }
    
    #[test]
    fn test_offtin_negative() {
        let buf = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];
        assert_eq!(offtin(&buf), 0);  // -0 = 0
        
        let buf2 = [0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80];
        assert_eq!(offtin(&buf2), -1);
    }
}
