//! 多线程后台解压诊断和优化模块
//!
//! 本模块用于验证 ParallelNewDataReader 的多线程是否正常工作，
//! 并提供性能优化建议。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// 诊断信息收集器
pub struct ParallelDiagnostics {
    /// 后台线程读取的字节数
    pub bytes_decompressed: Arc<AtomicU64>,
    /// 主线程消费的字节数
    pub bytes_consumed: Arc<AtomicU64>,
    /// 队列满次数（后台线程等待）
    pub queue_full_count: Arc<AtomicU64>,
    /// 队列空次数（主线程等待）
    pub queue_empty_count: Arc<AtomicU64>,
    /// 后台线程是否仍在运行
    pub thread_alive: Arc<AtomicBool>,
    /// 开始时间
    pub start_time: Instant,
}

impl ParallelDiagnostics {
    pub fn new() -> Self {
        Self {
            bytes_decompressed: Arc::new(AtomicU64::new(0)),
            bytes_consumed: Arc::new(AtomicU64::new(0)),
            queue_full_count: Arc::new(AtomicU64::new(0)),
            queue_empty_count: Arc::new(AtomicU64::new(0)),
            thread_alive: Arc::new(AtomicBool::new(true)),
            start_time: Instant::now(),
        }
    }

    /// 打印诊断报告
    pub fn report(&self) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let decompressed = self.bytes_decompressed.load(Ordering::Relaxed);
        let consumed = self.bytes_consumed.load(Ordering::Relaxed);
        let full_count = self.queue_full_count.load(Ordering::Relaxed);
        let empty_count = self.queue_empty_count.load(Ordering::Relaxed);
        let alive = self.thread_alive.load(Ordering::Relaxed);

        log::info!("=== ParallelNewDataReader 诊断报告 ===");
        log::info!("运行时间: {:.2}s", elapsed);
        log::info!("后台线程状态: {}", if alive { "运行中" } else { "已结束" });
        log::info!("解压字节数: {} ({:.2} MB)", decompressed, decompressed as f64 / 1_048_576.0);
        log::info!("消费字节数: {} ({:.2} MB)", consumed, consumed as f64 / 1_048_576.0);
        log::info!("解压速度: {:.2} MB/s", decompressed as f64 / 1_048_576.0 / elapsed.max(0.001));
        log::info!("消费速度: {:.2} MB/s", consumed as f64 / 1_048_576.0 / elapsed.max(0.001));
        log::info!("队列满次数 (后台等待): {}", full_count);
        log::info!("队列空次数 (主线程等待): {}", empty_count);

        if full_count > empty_count * 10 {
            log::warn!("队列频繁满，建议: 增大 NUM_POOL_BUFFERS 或 CHANNEL_CHUNK_SIZE");
        } else if empty_count > full_count * 10 {
            log::warn!("队列频繁空，建议: 主线程 I/O 可能成为瓶颈");
        } else {
            log::info!("队列平衡良好，多线程工作正常");
        }
    }
}

/// 检查多线程是否正确启用的函数
/// 
/// 使用方法:
/// 1. 在 update.rs 中调用此函数创建诊断实例
/// 2. 将诊断实例的 Arc 传递给 ParallelNewDataReader
/// 3. 运行后调用 report() 查看结果
pub fn check_parallel_enabled() -> bool {
    let thread_count = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    log::info!("系统报告 {} 个逻辑核心", thread_count);

    if thread_count < 2 {
        log::warn!("系统只有 {} 个核心，多线程优化效果有限", thread_count);
        return false;
    }

    // 测试创建后台线程
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = thread::spawn(move || {
        tx.send(true).unwrap();
    });

    match rx.recv_timeout(Duration::from_secs(1)) {
        Ok(true) => {
            let _ = handle.join();
            log::info!("后台线程测试成功，多线程可用");
            true
        }
        Err(_) => {
            log::error!("后台线程测试失败，多线程可能不可用");
            false
        }
    }
}

/// 优化的队列大小建议
pub fn recommend_buffer_settings(file_size: u64) -> (usize, usize) {
    // 根据文件大小推荐缓冲区设置
    // 返回 (num_buffers, chunk_size)
    
    let (num_buffers, chunk_size_mb) = match file_size {
        0..=100_000_000 => (2, 16),      // < 100MB: 2 buffers, 16MB
        100_000_001..=1_000_000_000 => (3, 32), // 100MB-1GB: 3 buffers, 32MB
        1_000_000_001..=5_000_000_000 => (4, 64), // 1-5GB: 4 buffers, 64MB
        _ => (5, 64), // > 5GB: 5 buffers, 64MB
    };

    let chunk_size = chunk_size_mb * 1024 * 1024;
    log::info!(
        "推荐设置: {} 个缓冲区, 每个 {} MB (文件大小: {} MB)",
        num_buffers,
        chunk_size_mb,
        file_size / 1_048_576
    );

    (num_buffers, chunk_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_available() {
        let available = check_parallel_enabled();
        println!("多线程可用: {}", available);
    }

    #[test]
    fn test_recommend_settings() {
        let (buffers, chunk) = recommend_buffer_settings(2_000_000_000); // 2GB
        assert!(buffers >= 3);
        assert!(chunk >= 32 * 1024 * 1024);
    }
}
