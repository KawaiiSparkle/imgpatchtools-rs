//! Global command-status tracking for the watchdog idle indicator.
//!
//! The batch watchdog thread reads the current status to display what the
//! tool is doing during 30+ seconds of no terminal output. The blockimg
//! command loop updates this status before each command so users can see
//! which command type (move, bsdiff, imgdiff, etc.) is currently executing.

use std::sync::{Mutex, OnceLock};

static GLOBAL_STATUS: OnceLock<Mutex<String>> = OnceLock::new();

/// Set the global command status string.
///
/// Called from the blockimg command loop before each command, and from
/// the batch module for higher-level status (OTA extraction, copying, etc.).
pub fn set_global_status(msg: &str) {
    let m = GLOBAL_STATUS.get_or_init(|| Mutex::new(String::new()));
    if let Ok(mut s) = m.lock() {
        *s = msg.to_string();
    }
}

/// Get a copy of the current global status string.
///
/// Called from the watchdog thread to display in the dim idle line.
pub fn get_global_status() -> String {
    let m = GLOBAL_STATUS.get_or_init(|| Mutex::new(String::new()));
    m.lock().map(|s| s.clone()).unwrap_or_default()
}

/// Clear the global command status string.
///
/// Called after batch operations complete to stop the watchdog from
/// printing stale status (e.g. "[2848/2848] erase" after all partitions
/// are already done).
pub fn clear_global_status() {
    let m = GLOBAL_STATUS.get_or_init(|| Mutex::new(String::new()));
    if let Ok(mut s) = m.lock() {
        s.clear();
    }
}
