//! New data reader abstraction with memory-based auto-selection.
//!
//! Automatically selects implementation based on available system memory:
//! - Memory >= 2GB: Use channel-based (high throughput, buffered)
//! - Memory < 2GB: Use sync-based (C++ compatible, zero buffer)
//!
//! Usage:
//! ```rust
//! // Auto-select based on available memory
//! let reader = NewDataReader::open(path)?;
//!
//! // Or explicitly choose:
//! let reader = NewDataReader::with_sync(path)?;
//! ```
//!
//! Environment variable (overrides auto-detection):
//! - `IMGPATCHTOOLS_READER=sync` - Force sync-based implementation
//! - `IMGPATCHTOOLS_READER=channel` - Force channel-based implementation

use anyhow::Result;
use std::path::Path;

use super::sync_reader::SyncNewDataReader;

/// Channel-based reader memory requirement: 8 chunks * 64MB = 512MB
/// We add 1.5GB overhead for system/other usage, so threshold is 2GB
const CHANNEL_MEMORY_THRESHOLD_MB: u64 = 2048;

/// Get available system memory in MB
#[cfg(windows)]
fn get_available_memory_mb() -> Option<u64> {
    use std::mem;

    #[repr(C)]
    #[allow(non_snake_case)]
    struct MEMORYSTATUSEX {
        dwLength: u32,
        dwMemoryLoad: u32,
        ullTotalPhys: u64,
        ullAvailPhys: u64,
        ullTotalPageFile: u64,
        ullAvailPageFile: u64,
        ullTotalVirtual: u64,
        ullAvailVirtual: u64,
        ullAvailExtendedVirtual: u64,
    }

    unsafe extern "system" {
        fn GlobalMemoryStatusEx(lpBuffer: *mut MEMORYSTATUSEX) -> i32;
    }

    let mut stat = MEMORYSTATUSEX {
        dwLength: mem::size_of::<MEMORYSTATUSEX>() as u32,
        dwMemoryLoad: 0,
        ullTotalPhys: 0,
        ullAvailPhys: 0,
        ullTotalPageFile: 0,
        ullAvailPageFile: 0,
        ullTotalVirtual: 0,
        ullAvailVirtual: 0,
        ullAvailExtendedVirtual: 0,
    };

    unsafe {
        if GlobalMemoryStatusEx(&mut stat) != 0 {
            Some(stat.ullAvailPhys / 1024 / 1024)
        } else {
            None
        }
    }
}

#[cfg(unix)]
fn get_available_memory_mb() -> Option<u64> {
    // On Linux, read /proc/meminfo
    if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
        for line in content.lines() {
            if line.starts_with("MemAvailable:") {
                // Format: "MemAvailable:    12345678 kB"
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return Some(kb / 1024);
                    }
                }
            }
        }
    }
    None
}

/// Unified new data reader interface
pub struct NewDataReader {
    inner: ReaderImpl,
}

enum ReaderImpl {
    Sync(SyncNewDataReader),
    // Channel(ParallelNewDataReader), // TODO: add when available
}

impl NewDataReader {
    /// Open new data with smart auto-selection
    ///
    /// Selection priority:
    /// 1. Environment variable IMGPATCHTOOLS_READER (if set)
    /// 2. Available memory >= 2GB: channel-based (when implemented)
    /// 3. Available memory < 2GB: sync-based
    pub fn open(path: &Path) -> Result<Self> {
        // Check environment variable first
        if let Ok(reader_type) = std::env::var("IMGPATCHTOOLS_READER") {
            match reader_type.as_str() {
                "sync" => {
                    log::info!("Using sync reader (forced by environment)");
                    return Self::with_sync(path);
                }
                "channel" => {
                    log::info!("Channel reader requested but not implemented, using sync");
                    return Self::with_sync(path);
                }
                _ => log::warn!(
                    "Unknown IMGPATCHTOOLS_READER value: {}, using auto-detect",
                    reader_type
                ),
            }
        }

        // Auto-detect based on available memory
        match get_available_memory_mb() {
            Some(avail_mb) => {
                log::info!("Available memory: {} MB", avail_mb);
                if avail_mb >= CHANNEL_MEMORY_THRESHOLD_MB {
                    log::info!(
                        "Memory >= {} MB, channel reader would be optimal (using sync for now)",
                        CHANNEL_MEMORY_THRESHOLD_MB
                    );
                    // TODO: return channel version when implemented
                    Self::with_sync(path)
                } else {
                    log::info!(
                        "Memory < {} MB, using sync-based reader for low memory usage",
                        CHANNEL_MEMORY_THRESHOLD_MB
                    );
                    Self::with_sync(path)
                }
            }
            None => {
                log::warn!("Could not detect available memory, using sync-based reader");
                Self::with_sync(path)
            }
        }
    }

    /// Use sync-based implementation (C++ compatible, low memory)
    ///
    /// This matches the C++ implementation exactly:
    /// - No buffering
    /// - Direct handoff between threads
    /// - Mutex + Condvar synchronization
    pub fn with_sync(path: &Path) -> Result<Self> {
        Ok(Self {
            inner: ReaderImpl::Sync(SyncNewDataReader::open(path)?),
        })
    }

    /// Read exact number of bytes
    pub fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        match &mut self.inner {
            ReaderImpl::Sync(r) => r.read_exact(buf),
        }
    }

    /// Read blocks
    pub fn read_blocks(&mut self, count: u64, block_size: usize) -> Result<Vec<u8>> {
        match &mut self.inner {
            ReaderImpl::Sync(r) => r.read_blocks(count, block_size),
        }
    }

    /// Skip blocks
    pub fn skip_blocks(&mut self, count: u64, block_size: usize) -> Result<()> {
        match &mut self.inner {
            ReaderImpl::Sync(r) => r.skip_blocks(count, block_size),
        }
    }

    /// Print diagnostics
    pub fn report_diagnostics(&self) {
        match &self.inner {
            ReaderImpl::Sync(_) => {
                log::info!("Using sync-based new data reader (C++ compatible, low memory)");
            }
        }
    }
}
