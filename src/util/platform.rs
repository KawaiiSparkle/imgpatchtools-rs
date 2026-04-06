//! Platform-specific file I/O optimizations.
//!
//! Currently provides Windows-specific APIs (方案5):
//! - `FSCTL_SET_SPARSE` / `FSCTL_SET_ZERO_DATA` for sparse zeroing
//!
//! On non-Windows platforms, all functions are no-ops or return errors.

/// Mark a file as sparse on Windows.
///
/// Uses `FSCTL_SET_SPARSE` (DeviceIoControl code 0x00120020).
/// Silently succeeds on non-Windows platforms.
#[cfg(target_os = "windows")]
pub fn set_sparse(file: &std::fs::File) -> Result<(), anyhow::Error> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::IO::DeviceIoControl;

    const FSCTL_SET_SPARSE: u32 = 0x0012_0020;

    unsafe {
        let handle = file.as_raw_handle() as HANDLE;
        let mut bytes_returned: u32 = 0;
        let result = DeviceIoControl(
            handle,
            FSCTL_SET_SPARSE,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            0,
            &mut bytes_returned,
            ptr::null_mut(),
        );
        if result == 0 {
            log::debug!("FSCTL_SET_SPARSE failed (non-fatal)");
        }
    }
    Ok(())
}

/// Zero a byte range in a file without writing actual zeroes (sparse de-allocation).
///
/// Uses `FSCTL_SET_ZERO_DATA` (DeviceIoControl code 0x0009_80C8).
/// The OS marks the range as zeroes without allocating disk blocks.
#[cfg(target_os = "windows")]
pub fn zero_data(file: &std::fs::File, offset: u64, length: u64) -> Result<(), anyhow::Error> {
    use std::os::windows::io::AsRawHandle;
    use std::ptr;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::System::IO::DeviceIoControl;

    const FSCTL_SET_ZERO_DATA: u32 = 0x0009_80C8;

    // LARGE_INTEGER is i64-aligned 64-bit value for DeviceIoControl
    #[repr(C)]
    struct FileZeroDataInformation {
        file_offset: i64,
        beyond_final_zero: i64,
    }

    unsafe {
        let handle = file.as_raw_handle() as HANDLE;
        let mut bytes_returned: u32 = 0;
        let info = FileZeroDataInformation {
            file_offset: offset as i64,
            beyond_final_zero: (offset + length) as i64,
        };
        let result = DeviceIoControl(
            handle,
            FSCTL_SET_ZERO_DATA,
            &info as *const _ as *mut _,
            std::mem::size_of::<FileZeroDataInformation>() as u32,
            ptr::null_mut(),
            0,
            &mut bytes_returned,
            ptr::null_mut(),
        );
        if result == 0 {
            return Err(anyhow::anyhow!("FSCTL_SET_ZERO_DATA failed"));
        }
    }
    Ok(())
}

/// Apply sequential scan hint (no-op on Windows, see set_sequential_hint below).
#[cfg(target_os = "windows")]
pub fn set_sequential_hint(_file: &std::fs::File) {
    // Windows provides read-ahead automatically for sequential patterns.
    // FILE_FLAG_SEQUENTIAL_SCAN is set at file creation time, not after.
    // Since we open files via OpenOptions, we can't easily add it post-hoc.
    // The OS read-ahead heuristics handle this adequately.
    log::debug!("Windows: sequential scan hint (OS read-ahead active)");
}

// ---------------------------------------------------------------------------
// Non-Windows stubs (no-ops)
// ---------------------------------------------------------------------------

#[cfg(not(target_os = "windows"))]
pub fn set_sparse(_file: &std::fs::File) -> Result<(), anyhow::Error> {
    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn zero_data(file: &std::fs::File, offset: u64, length: u64) -> Result<(), anyhow::Error> {
    use std::io::{Seek, SeekFrom, Write};

    let mut file = file;
    file.seek(SeekFrom::Start(offset))?;

    const ZERO_BUF_SIZE: usize = 64 * 1024; // 64KB buffer
    let zero_buf = vec![0u8; ZERO_BUF_SIZE];

    let mut remaining = length;
    while remaining > 0 {
        let to_write = std::cmp::min(remaining, ZERO_BUF_SIZE as u64) as usize;
        file.write_all(&zero_buf[..to_write])?;
        remaining -= to_write as u64;
    }

    Ok(())
}

#[cfg(not(target_os = "windows"))]
pub fn set_sequential_hint(_file: &std::fs::File) {
    // no-op on non-Windows
}
