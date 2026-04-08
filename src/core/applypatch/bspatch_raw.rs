//! 原始 bzip2 FFI 版本的极致流式 bsdiff —— 完全跳过 Rust Read trait 封装。
//!
//! 本模块直接使用 bzip2-sys 的 FFI 函数：
//! - BZ2_bzDecompressInit
//! - BZ2_bzDecompress
//! - BZ2_bzDecompressEnd
//!
//! 相比 bspatch_streaming.rs 的改进：
//! - 零 Read trait 开销
//! - 直接内存到内存解压
//! - 更少的抽象层

use anyhow::{ensure, bail, Result};
use bzip2_sys::*;

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

/// 原始 bzip2 解压器 —— 直接 FFI，零 Rust 封装开销
struct RawBzDecoder {
    stream: bz_stream,
    input: *const u8,
    input_len: usize,
    input_pos: usize,
}

impl RawBzDecoder {
    /// 从压缩数据创建解码器
    fn new(compressed: &[u8]) -> Result<Self> {
        let mut stream: bz_stream = unsafe { std::mem::zeroed() };
        
        // 初始化解压
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
    
    /// 解压数据到输出缓冲区
    /// 
    /// 返回实际解压的字节数，0 表示流结束
    fn decompress(&mut self, output: &mut [u8]) -> Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        
        // 设置输入指针
        let remaining_input = self.input_len - self.input_pos;
        self.stream.next_in = unsafe { self.input.add(self.input_pos) } as *mut _;
        self.stream.avail_in = remaining_input as u32;
        
        // 设置输出指针 (bzip2 使用 *mut i8)
        self.stream.next_out = output.as_mut_ptr() as *mut _;
        self.stream.avail_out = output.len() as u32;
        
        // 执行解压
        let ret = unsafe {
            BZ2_bzDecompress(&mut self.stream)
        };
        
        // 更新输入位置
        let consumed = remaining_input - self.stream.avail_in as usize;
        self.input_pos += consumed;
        
        // 计算输出字节数
        let produced = output.len() - self.stream.avail_out as usize;
        
        match ret {
            n if n == BZ_OK as i32 => Ok(produced),
            n if n == BZ_STREAM_END as i32 => Ok(produced),
            n => bail!("BZ2_bzDecompress failed: {}", n),
        }
    }
    
    /// 确保所有输入都被消费
    fn finish(&mut self) -> Result<()> {
        // 尝试排空剩余数据
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
                // 没有更多输出了
                return Ok(());
            }
        }
    }
}

impl Drop for RawBzDecoder {
    fn drop(&mut self) {
        unsafe {
            BZ2_bzDecompressEnd(&mut self.stream);
        }
    }
}

/// 极致流式 bsdiff 应用 —— 原始 bzip2 FFI 版本
///
/// 完全跳过 Rust 的 Read trait，直接使用 bzip2-sys FFI。
pub fn apply_bspatch_raw(
    source: &[u8],
    patch: &[u8],
    patch_offset: usize,
    output: &mut [u8],
) -> Result<()> {
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
    
    ensure!(new_size == output.len(), 
        "output size mismatch: expected {}, got {}", new_size, output.len());
    ensure!(ctrl_len > 0 || diff_len == 0, "bad patch header");
    
    // 解压数据
    let payload = &patch[patch_offset + HEADER_SIZE..];
    let ctrl_compressed = &payload[..ctrl_len.min(payload.len())];
    let diff_start = ctrl_len;
    let diff_compressed = &payload[diff_start..(diff_start + diff_len).min(payload.len())];
    let extra_start = diff_start + diff_len;
    let extra_compressed = &payload[extra_start..];
    
    // 创建原始解码器
    let mut ctrl_dec = RawBzDecoder::new(ctrl_compressed)?;
    let mut diff_dec = RawBzDecoder::new(diff_compressed)?;
    let mut extra_dec = RawBzDecoder::new(extra_compressed)?;
    
    // 固定大小的缓冲区
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut diff_buf = vec![0u8; CHUNK_SIZE];
    let mut extra_buf = vec![0u8; CHUNK_SIZE];
    let mut ctrl_buf = [0u8; 24];
    
    let mut new_pos: usize = 0;
    let mut old_pos: i64 = 0;
    let old_size = source.len() as i64;
    
    while new_pos < new_size {
        // 读取控制三元组（必须完整读取 24 字节）
        let mut ctrl_read = 0;
        while ctrl_read < 24 {
            let n = ctrl_dec.decompress(&mut ctrl_buf[ctrl_read..])?;
            ensure!(n > 0 || ctrl_read == 24, "truncated control stream");
            ctrl_read += n;
        }
        
        let add_len = offtin(&ctrl_buf[0..8]) as usize;
        let copy_len = offtin(&ctrl_buf[8..16]) as usize;
        let seek_adj = offtin(&ctrl_buf[16..24]);
        
        // 处理 diff 块
        let mut diff_remaining = add_len;
        while diff_remaining > 0 {
            let chunk = diff_remaining.min(CHUNK_SIZE);
            
            // 解压 diff 数据
            let mut diff_decompressed = 0;
            while diff_decompressed < chunk {
                let n = diff_dec.decompress(&mut diff_buf[diff_decompressed..chunk])?;
                ensure!(n > 0, "truncated diff stream");
                diff_decompressed += n;
            }
            
            // 应用 diff（指针运算 + 批量处理）
            let src_start = old_pos.max(0) as usize;
            let src_end = ((old_pos + chunk as i64).min(old_size)).max(0) as usize;
            let src_len = src_end.saturating_sub(src_start);
            
            if src_len > 0 {
                unsafe {
                    let diff_ptr = diff_buf.as_mut_ptr();
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
            
            // 写入输出
            output[new_pos..new_pos + chunk].copy_from_slice(&diff_buf[..chunk]);
            
            diff_remaining -= chunk;
            new_pos += chunk;
            old_pos += chunk as i64;
        }
        
        // 处理 extra 块
        let mut extra_remaining = copy_len;
        while extra_remaining > 0 {
            let chunk = extra_remaining.min(CHUNK_SIZE);
            
            // 解压 extra 数据
            let mut extra_decompressed = 0;
            while extra_decompressed < chunk {
                let n = extra_dec.decompress(&mut extra_buf[extra_decompressed..chunk])?;
                ensure!(n > 0, "truncated extra stream");
                extra_decompressed += n;
            }
            
            // 直接复制
            output[new_pos..new_pos + chunk].copy_from_slice(&extra_buf[..chunk]);
            
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

/// 便捷函数：原始 FFI 版本应用到 Vec
pub fn apply_bspatch_raw_vec(source: &[u8], patch: &[u8], patch_offset: usize) -> Result<Vec<u8>> {
    // 解析头部获取输出大小
    ensure!(
        patch.len() >= patch_offset + HEADER_SIZE,
        "patch too short"
    );
    let hdr = &patch[patch_offset..patch_offset + HEADER_SIZE];
    ensure!(&hdr[..8] == b"BSDIFF40", "bad bsdiff magic");
    
    let new_size = offtin(&hdr[24..32]) as usize;
    let mut output = vec![0u8; new_size];
    
    apply_bspatch_raw(source, patch, patch_offset, &mut output)?;
    
    Ok(output)
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
}
