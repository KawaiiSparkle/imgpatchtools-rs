//! Crash-resume support — port of AOSP `ParseLastCommandFile` /
//! `UpdateLastCommandIndex` from `blockimg.cpp`.
//!
//! # File format (AOSP `last_command_file`)
//!
//! ```text
//! <file_format_version>
//! <last_completed_command_index>
//! ```
//!
//! The format version is currently `1`. Only the first two lines are read;
//! extra content is ignored.

use std::fs;
use std::io::Write;
use std::path::Path;

use anyhow::{bail, Context, Result};

/// File format version written to the first line.
const RESUME_FILE_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// read_resume_index
// ---------------------------------------------------------------------------

/// Read the resume checkpoint from a `last_command` file.
///
/// Returns `Ok(None)` if the file does not exist or is empty (start from
/// the beginning). Returns `Ok(Some(idx))` with the **0-based index of the
/// last completed command**. The caller should resume execution at
/// `idx + 1`.
///
/// # File format
///
/// ```text
/// 1              ← format version (must be 1)
/// 42             ← last completed command index
/// ```
///
/// Extra lines beyond the first two are silently ignored (forward
/// compatibility).
pub fn read_resume_index(path: &Path) -> Result<Option<usize>> {
    if !path.exists() {
        log::info!("resume: no checkpoint file at {}", path.display());
        return Ok(None);
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("resume: failed to read {}", path.display()))?;
    let trimmed = content.trim();

    if trimmed.is_empty() {
        log::info!("resume: checkpoint file is empty");
        return Ok(None);
    }

    let mut lines = trimmed.lines();

    // Line 1: format version.
    let version_str = lines.next().context("resume: missing version line")?.trim();
    let version: u32 = version_str
        .parse()
        .with_context(|| format!("resume: bad version: {version_str:?}"))?;

    if version != RESUME_FILE_VERSION {
        bail!(
            "resume: unsupported file format version {} (expected {})",
            version,
            RESUME_FILE_VERSION
        );
    }

    // Line 2: last completed command index.
    let index_str = lines
        .next()
        .context("resume: missing command index line")?
        .trim();
    let index: usize = index_str
        .parse()
        .with_context(|| format!("resume: bad command index: {index_str:?}"))?;

    log::info!(
        "resume: loaded checkpoint from {}: last completed = {}",
        path.display(),
        index
    );

    Ok(Some(index))
}

// ---------------------------------------------------------------------------
// write_resume_index
// ---------------------------------------------------------------------------

/// Atomically write the resume checkpoint.
///
/// Creates (or replaces) the file at `path` with the current command
/// index. Uses a temporary file + rename for crash safety.
///
/// # Cross-platform
///
/// * **Unix**: `rename(2)` atomically replaces the target.
/// * **Windows**: `tempfile::NamedTempFile::persist` uses
///   `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`.
pub fn write_resume_index(path: &Path, index: usize) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));

    if !parent.exists() {
        fs::create_dir_all(parent)
            .with_context(|| format!("resume: failed to create directory {}", parent.display()))?;
    }

    let mut tmp =
        tempfile::NamedTempFile::new_in(parent).context("resume: failed to create temp file")?;

    writeln!(tmp, "{}", RESUME_FILE_VERSION).context("resume: failed to write version")?;
    writeln!(tmp, "{}", index).context("resume: failed to write index")?;

    tmp.flush().context("resume: failed to flush")?;
    tmp.as_file()
        .sync_all()
        .context("resume: failed to fsync")?;

    tmp.persist(path)
        .with_context(|| format!("resume: failed to persist to {}", path.display()))?;

    log::debug!("resume: wrote checkpoint {} → {}", index, path.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// clear_resume_file
// ---------------------------------------------------------------------------

/// Delete the resume checkpoint file.
///
/// If the file does not exist, this is a silent no-op.
pub fn clear_resume_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            log::info!("resume: cleared checkpoint {}", path.display());
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log::debug!("resume: {} already absent", path.display());
            Ok(())
        }
        Err(e) => Err(e).with_context(|| format!("resume: failed to delete {}", path.display())),
    }
}
