//! 同步协作式 new data reader —— 完全复刻 C++ 版本的 pthread_cond_wait 模式
//!
//! 与 C++ 版本的关键对应：
//! - C++ `pthread_mutex_t` → Rust `Mutex`
//! - C++ `pthread_cond_t` → Rust `Condvar`
//! - C++ `writer` 指针 → Rust `WriterRequest`
//! - C++ `receive_new_data` → Rust `decompressor_thread` 回调

use anyhow::{Result, bail};
use std::io::Read;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

const BLOCKSIZE: usize = 4096;
const DECOMP_BUF_SIZE: usize = 512 * 1024;

/// 共享状态 —— 对应 C++ 的 NewThreadInfo
struct SharedState {
    /// 写入请求（主线程设置，后台线程消费）
    /// 类似 C++ 的 RangeSinkWriter* writer
    writer: Option<WriterRequest>,
    /// 后台线程是否可用
    receiver_available: bool,
    /// 错误标志
    has_error: bool,
}

/// 写入请求 —— 对应 C++ 的 RangeSinkWriter
struct WriterRequest {
    /// 目标缓冲区
    buf: Vec<u8>,
    /// 已写入字节数
    written: usize,
}

impl WriterRequest {
    fn new(size: usize) -> Self {
        Self {
            buf: vec![0u8; size],
            written: 0,
        }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.written
    }

    fn is_finished(&self) -> bool {
        self.written >= self.buf.len()
    }

    fn write(&mut self, data: &[u8]) -> usize {
        let to_write = data.len().min(self.remaining());
        self.buf[self.written..self.written + to_write].copy_from_slice(&data[..to_write]);
        self.written += to_write;
        to_write
    }
}

/// 同步协作式 new data reader
/// 完全复刻 C++ 版本的同步模式
pub struct SyncNewDataReader {
    /// 共享状态（Mutex + Condvar）
    shared: Arc<Mutex<SharedState>>,
    /// 条件变量（用于主线程和后台线程同步）
    cv: Arc<Condvar>,
    /// 后台线程句柄
    _handle: thread::JoinHandle<()>,
}

impl SyncNewDataReader {
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let path = path.to_path_buf();

        // 确定文件类型
        let (file_path, ext) = match std::fs::File::open(&path) {
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

                if std::fs::File::open(&br_path).is_ok() {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        br_path.display()
                    );
                    (br_path, "br".to_string())
                } else if std::fs::File::open(&lzma_path).is_ok() {
                    log::info!(
                        "auto-fallback to compressed new data: {}",
                        lzma_path.display()
                    );
                    (lzma_path, "lzma".to_string())
                } else if std::fs::File::open(&xz_path).is_ok() {
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
            "starting sync decompressor thread for: {}",
            file_path.display()
        );

        // 初始化共享状态
        let shared = Arc::new(Mutex::new(SharedState {
            writer: None,
            receiver_available: true,
            has_error: false,
        }));
        let cv = Arc::new(Condvar::new());

        let shared_clone = Arc::clone(&shared);
        let cv_clone = Arc::clone(&cv);

        // 启动后台线程
        let handle = thread::spawn(move || {
            if let Err(e) = Self::decompressor_thread(file_path, ext, shared_clone, cv_clone) {
                log::error!("decompressor thread failed: {}", e);
            }
        });

        Ok(Self {
            shared,
            cv,
            _handle: handle,
        })
    }

    /// 后台解压线程 —— 对应 C++ 的 unzip_new_data + receive_new_data
    fn decompressor_thread(
        path: std::path::PathBuf,
        ext: String,
        shared: Arc<Mutex<SharedState>>,
        cv: Arc<Condvar>,
    ) -> Result<()> {
        let file = std::fs::File::open(&path)?;

        let mut reader: Box<dyn Read + Send> = match ext.as_str() {
            "br" => {
                log::info!("using Brotli decompressor");
                Box::new(brotli::Decompressor::new(file, DECOMP_BUF_SIZE))
            }
            "lzma" | "xz" => {
                log::info!("using XZ/LZMA decompressor");
                Box::new(xz2::read::XzDecoder::new(file))
            }
            _ => {
                log::info!("using raw file reader");
                Box::new(file)
            }
        };

        // 解压缓冲区
        let mut decompress_buf = vec![0u8; DECOMP_BUF_SIZE];

        loop {
            // 读取解压数据
            let n = match reader.read(&mut decompress_buf) {
                Ok(0) => break, // EOF
                Ok(n) => n,
                Err(e) => {
                    log::error!("decompressor read error: {}", e);
                    let mut state = shared.lock().unwrap();
                    state.has_error = true;
                    state.receiver_available = false;
                    cv.notify_all();
                    return Err(e.into());
                }
            };

            let mut data = &decompress_buf[..n];

            // 处理数据（类似 C++ 的 receive_new_data）
            while !data.is_empty() {
                let mut state = shared.lock().unwrap();

                // 等待主线程设置 writer（对应 C++ 的 while (nti->writer == nullptr)）
                while state.writer.is_none() && state.receiver_available && !state.has_error {
                    state = cv.wait(state).unwrap();
                }

                if !state.receiver_available || state.has_error {
                    return Ok(());
                }

                // 获取 writer 的可变引用
                let written = if let Some(ref mut writer) = state.writer {
                    let to_write = data.len().min(writer.remaining());
                    writer.write(&data[..to_write]);
                    to_write
                } else {
                    0
                };

                let is_finished = state
                    .writer
                    .as_ref()
                    .map(|w| w.is_finished())
                    .unwrap_or(false);

                // 如果 writer 完成了，清空它并通知主线程
                if is_finished {
                    state.writer = None;
                    drop(state);
                    cv.notify_all();
                } else {
                    drop(state);
                }

                if written == 0 {
                    // 没有写入任何数据，可能出错了
                    break;
                }

                data = &data[written..];
            }
        }

        // 标记后台线程完成
        let mut state = shared.lock().unwrap();
        state.receiver_available = false;
        cv.notify_all();

        log::debug!("decompressor thread finished");
        Ok(())
    }

    /// 读取指定字节数 —— 对应 C++ 的写入流程
    /// 主线程调用：设置 writer，等待后台线程写入，获取结果
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        let writer = WriterRequest::new(buf.len());

        {
            let mut state = self.shared.lock().unwrap();

            if state.has_error {
                bail!("decompressor thread encountered an error");
            }

            if !state.receiver_available && state.writer.is_none() {
                bail!("decompressor channel closed");
            }

            // 设置 writer（对应 C++ 的 nti->writer = writer）
            state.writer = Some(writer);
        }

        // 通知后台线程有新 writer
        self.cv.notify_all();

        // 等待后台线程完成写入
        let result_buf = {
            let mut state = self.shared.lock().unwrap();

            loop {
                if state.has_error {
                    bail!("decompressor thread encountered an error");
                }

                // 检查 writer 是否完成（被后台线程清空）
                match state.writer {
                    None => {
                        // 后台线程完成了，但我们需要拿回 buffer
                        // 实际上 buffer 在 writer 里，我们需要特殊处理
                        break;
                    }
                    Some(ref w) if w.is_finished() => {
                        // 完成了，可以取回 buffer
                        break;
                    }
                    _ => {
                        // 还没完成，继续等待
                        state = self.cv.wait(state).unwrap();
                    }
                }
            }

            // 取回 writer
            std::mem::take(&mut state.writer)
        };

        // 复制数据到输出缓冲区
        if let Some(writer) = result_buf {
            buf.copy_from_slice(&writer.buf);
        } else {
            bail!("writer was taken unexpectedly");
        }

        // 通知后台线程可以继续了
        self.cv.notify_all();

        Ok(())
    }

    /// 读取 blocks
    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        let len = (count as usize) * block_size;
        let mut buf = vec![0u8; len];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }

    /// 跳过 blocks
    pub fn skip_blocks(&mut self, count: u64, block_size: usize) -> Result<()> {
        let mut remaining = (count as usize) * block_size;
        let mut skip_buf = vec![0u8; BLOCKSIZE.min(remaining)];

        while remaining > 0 {
            let chunk = remaining.min(skip_buf.len());
            self.read_exact(&mut skip_buf[..chunk])?;
            remaining -= chunk;
        }

        Ok(())
    }
}
