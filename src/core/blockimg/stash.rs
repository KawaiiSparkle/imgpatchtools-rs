//! Stash management — lazy-loading port of AOSP `blockimg.cpp` stash subsystem.
//!
//! Uses disk-first strategy: data stays on disk, only metadata in memory.
//! This matches C++ behavior and minimizes RAM usage.
//!
//! # AOSP stash contract
//!
//! | Function       | AOSP equivalent    | Behaviour                              |
//! |----------------|--------------------|----------------------------------------|
//! | [`new`]        | `CreateStash`      | Create work dir, clean up stale        |
//! | [`save`]       | `WriteStash`       | Write to disk only (no cache)          |
//! | [`load`]       | `LoadStash`        | Read from disk (no in-memory cache)    |
//! | [`load_ref`]   | (verify mode)      | Return reference without caching       |
//! | [`free`]       | `FreeStash`        | Delete file only                       |
//! | [`clear_all`]  | (cleanup path)     | Remove everything                      |

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};

use crate::util::hash;
use crate::util::rangeset::RangeSet;

// ---------------------------------------------------------------------------
// StashManager
// ---------------------------------------------------------------------------

/// Manages stash slots using lazy loading from disk.
///
/// Unlike the original Rust implementation that cached data in memory,
/// this version matches C++ behavior: data stays on disk, only metadata
/// (block count) is tracked in memory. This minimizes RAM usage for
/// large updates.
pub struct StashManager {
    /// Directory where stash files are stored.
    work_dir: PathBuf,
    /// Block size in bytes.
    block_size: usize,
    /// Metadata only: stash_id → block_count (data stays on disk).
    entries: HashMap<String, u64>,
    /// Running total of blocks currently stashed.
    current_blocks: u64,
    /// Maximum number of simultaneous stash entries allowed (from header).
    max_entries: u32,
    /// Maximum total blocks held in stash at once (from header).
    max_blocks: u32,
}

impl StashManager {
    /// Create a new `StashManager`, mirroring AOSP `CreateStash`.
    ///
    /// * Creates `work_dir` if it does not exist.
    /// * Enumerates any leftover stash files from a prior run (for resume
    ///   support) — they remain on disk and can be loaded on demand.
    /// * The in-memory cache starts empty; files are loaded lazily.
    ///
    /// # Arguments
    ///
    /// * `work_dir`    — directory for stash file Storage.
    /// * `block_size`  — block size in bytes (typically 4096).
    /// * `max_entries` — max simultaneous stash slots (from transfer-list
    ///   header; 0 means unlimited).
    /// * `max_blocks`  — max total stashed blocks (from transfer-list
    ///   header; 0 means unlimited).
    pub fn new(
        work_dir: &Path,
        block_size: usize,
        max_entries: u32,
        max_blocks: u32,
    ) -> Result<Self> {
        ensure!(block_size > 0, "block_size must be positive");

        if !work_dir.exists() {
            fs::create_dir_all(work_dir).with_context(|| {
                format!("failed to create stash directory {}", work_dir.display())
            })?;
        }

        log::info!(
            "stash: dir={}, max_entries={}, max_blocks={}",
            work_dir.display(),
            max_entries,
            max_blocks,
        );

        // Count leftover files from a prior run (for diagnostics).
        let leftover = count_stash_files(work_dir);
        if leftover > 0 {
            log::info!(
                "stash: found {} leftover file(s) from a prior run",
                leftover
            );
        }

        Ok(Self {
            work_dir: work_dir.to_path_buf(),
            block_size,
            entries: HashMap::new(),
            current_blocks: 0,
            max_entries,
            max_blocks,
        })
    }

    // ---- Accessors --------------------------------------------------------

    /// Path to the stash directory.
    #[inline]
    pub fn work_dir(&self) -> &Path {
        &self.work_dir
    }

    /// Block size in bytes.
    #[inline]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Number of entries currently tracked (metadata in memory).
    #[inline]
    pub fn cached_count(&self) -> usize {
        self.entries.len()
    }

    /// Total blocks currently stashed (tracked, not recomputed).
    #[inline]
    pub fn current_blocks(&self) -> u64 {
        self.current_blocks
    }

    /// Whether a stash slot is present in the in-memory cache **or** on
    /// disk.
    pub fn exists(&self, id: &str) -> bool {
        self.entries.contains_key(id) || self.stash_path(id).exists()
    }

    // ---- Core operations --------------------------------------------------

    /// Save data into a stash slot — AOSP `WriteStash`.
    ///
    /// 1. Verifies the SHA-1 of `data` matches `id`.
    /// 2. Writes `data` to the on-disk file `{work_dir}/{id}`.
    /// 3. Records metadata (block count) in memory.
    ///
    /// Data is NOT cached in memory - it stays on disk for lazy loading.
    ///
    /// # Errors
    ///
    /// Returns an error if the SHA-1 check fails or the file write fails.
    pub fn save(&mut self, id: &str, data: &[u8]) -> Result<()> {
        // If already tracked, skip (idempotent).
        if self.entries.contains_key(id) {
            log::debug!("stash save: {} already exists, skipping", id);
            return Ok(());
        }

        // Verify SHA-1: in AOSP the stash ID is the SHA-1 of the data.
        verify_stash_sha1(id, data)?;

        // Size sanity: must be a whole number of blocks.
        ensure!(
            data.len().is_multiple_of(self.block_size),
            "stash data length {} is not a multiple of block_size {}",
            data.len(),
            self.block_size
        );

        let blocks = (data.len() / self.block_size) as u64;

        // Check limits.
        if self.max_entries > 0 {
            ensure!(
                (self.entries.len() as u32) < self.max_entries,
                "stash: would exceed max_entries ({}) with save of {}",
                self.max_entries,
                id
            );
        }
        if self.max_blocks > 0 {
            ensure!(
                self.current_blocks + blocks <= self.max_blocks as u64,
                "stash: would exceed max_blocks ({}) with {} additional blocks",
                self.max_blocks,
                blocks
            );
        }

        // Write to disk only (no in-memory cache).
        write_stash_file(&self.stash_path(id), data)
            .with_context(|| format!("stash save: failed to write {id}"))?;

        // Record metadata only (block count, not data).
        self.entries.insert(id.to_string(), blocks);
        self.current_blocks += blocks;

        log::debug!(
            "stash save: {} ({} blocks, total stashed: {})",
            id,
            blocks,
            self.current_blocks
        );

        Ok(())
    }

    /// Load data from a stash slot — AOSP `LoadStash`.
    ///
    /// Reads directly from disk, verifies SHA-1, returns data.
    /// Data is NOT cached in memory - it is read fresh from disk each time
    /// to minimize RAM usage (matches C++ behavior).
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not exist, cannot be read, or
    /// its SHA-1 does not match.
    pub fn load(&mut self, id: &str) -> Result<Arc<[u8]>> {
        let path = self.stash_path(id);

        // Always read from disk (no in-memory cache).
        // NOTE: We don't require metadata to exist - file might exist from prior run
        let data = fs::read(&path)
            .with_context(|| format!("stash load: failed to read {}", path.display()))?;

        // Verify integrity.
        verify_stash_sha1(id, &data)
            .with_context(|| format!("stash load: integrity check failed for {id}"))?;

        // Update metadata if not present (for resume support)
        if !self.entries.contains_key(id) {
            let blocks = (data.len() / self.block_size) as u64;
            self.entries.insert(id.to_string(), blocks);
            self.current_blocks += blocks;
        }

        log::debug!("stash load: {} ({} bytes from disk)", id, data.len());

        // Return data without caching (Arc for API compatibility).
        Ok(Arc::from(data))
    }

    /// Load only specific byte ranges from a stash file (方案6).
    ///
    /// Instead of reading the entire stash file into memory, calculates the
    /// byte offset for each mapped range and reads only those portions.
    /// This can dramatically reduce memory usage when a stash file is large
    /// but only a subset of its blocks are referenced.
    ///
    /// The `map_ranges` defines which blocks are needed and their order.
    /// Stash data is stored sequentially, so we compute cumulative offsets.
    ///
    /// Returns a contiguous buffer containing only the requested data,
    /// in the same order as `map_ranges`.
    pub fn load_ranges(
        &mut self,
        id: &str,
        map_ranges: &RangeSet,
        block_size: usize,
    ) -> Result<Vec<u8>> {
        let path = self.stash_path(id);
        let mut file = std::fs::File::open(&path)
            .with_context(|| format!("stash load_ranges: failed to open {}", path.display()))?;

        // Calculate total bytes needed
        let total_blocks = map_ranges.blocks();
        let total_bytes = (total_blocks as usize) * block_size;
        let mut result = vec![0u8; total_bytes];
        let mut result_offset = 0usize;
        let mut file_offset: u64 = 0;

        for (start, end) in map_ranges.iter() {
            let len = ((end - start) as usize) * block_size;

            use std::io::{Read, Seek, SeekFrom};
            file.seek(SeekFrom::Start(file_offset))?;
            file.read_exact(&mut result[result_offset..result_offset + len])
                .with_context(|| {
                    format!(
                        "stash load_ranges: failed to read {} bytes at offset {}",
                        len, file_offset
                    )
                })?;

            result_offset += len;
            file_offset += len as u64;
        }

        log::debug!(
            "stash load_ranges: {} ({} bytes / {} blocks from disk, file offset ended at {})",
            id,
            total_bytes,
            total_blocks,
            file_offset
        );

        // Update metadata if not present (for resume support)
        if !self.entries.contains_key(id) {
            let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
            let blocks = file_len / block_size as u64;
            self.entries.insert(id.to_string(), blocks);
            self.current_blocks += blocks;
        }

        Ok(result)
    }

    /// Load data from a stash slot without cloning — returns an Arc.
    ///
    /// Identical to [`load`](Self::load) - reads from disk every time
    /// to minimize RAM usage.
    pub fn load_ref(&self, id: &str) -> Result<Arc<[u8]>> {
        // Always read from disk (no in-memory cache).
        let path = self.stash_path(id);
        let data = fs::read(&path)
            .with_context(|| format!("stash load_ref: failed to read {}", path.display()))?;

        // Verify integrity.
        verify_stash_sha1(id, &data)
            .with_context(|| format!("stash load_ref: integrity check failed for {id}"))?;

        let blocks = self.entries.get(id).copied().unwrap_or(0);
        log::debug!("stash load_ref: {} ({} blocks from disk)", id, blocks);

        Ok(Arc::from(data))
    }

    /// Release a stash slot — AOSP `FreeStash`.
    ///
    /// 1. Removes metadata from memory.
    /// 2. Deletes the on-disk file.
    ///
    /// If the slot does not exist, this is a silent no-op.
    pub fn free(&mut self, id: &str) -> Result<()> {
        // Remove metadata and update block count.
        if let Some(blocks) = self.entries.remove(id) {
            self.current_blocks = self.current_blocks.saturating_sub(blocks);
        }

        // Delete on-disk file (ignore "not found" — idempotent).
        let path = self.stash_path(id);
        match fs::remove_file(&path) {
            Ok(()) => {
                log::debug!("stash free: {} (deleted)", id);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                log::debug!("stash free: {} (file already absent)", id);
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("stash free: failed to delete {}", path.display()));
            }
        }

        Ok(())
    }

    /// Remove **all** stash entries — memory and disk.
    ///
    /// Called at the end of a successful update to clean up.
    pub fn clear_all(&mut self) -> Result<()> {
        self.entries.clear();
        self.current_blocks = 0;

        // Remove all files in the stash directory.
        let entries = fs::read_dir(&self.work_dir).with_context(|| {
            format!(
                "stash clear_all: failed to list {}",
                self.work_dir.display()
            )
        })?;

        let mut errors = Vec::new();
        for entry in entries {
            let entry = entry.context("stash clear_all: readdir error")?;
            let path = entry.path();
            if path.is_file() {
                if let Err(e) = fs::remove_file(&path) {
                    errors.push(format!("{}: {e}", path.display()));
                }
            }
        }

        if !errors.is_empty() {
            bail!(
                "stash clear_all: failed to remove {} file(s): {}",
                errors.len(),
                errors.join("; ")
            );
        }

        // Attempt to remove the directory itself (may fail if not empty on
        // some platforms — that's OK).
        let _ = fs::remove_dir(&self.work_dir);

        log::info!("stash: cleared all entries");
        Ok(())
    }

    // ---- Internal helpers -------------------------------------------------

    /// Compute the on-disk path for a stash entry.
    ///
    /// AOSP uses the stash ID directly as the filename:
    /// `{stash_base}/{id}`.
    fn stash_path(&self, id: &str) -> PathBuf {
        self.work_dir.join(id)
    }
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Verify that the SHA-1 of `data` matches the expected stash `id`.
///
/// In AOSP, the stash ID **is** the SHA-1 hex digest of the stashed
/// content. This check ensures data integrity both at save time (catching
/// bugs) and at load-from-disk time (catching corruption).
fn verify_stash_sha1(id: &str, data: &[u8]) -> Result<()> {
    let actual = hash::sha1_hex(data);
    if !actual.eq_ignore_ascii_case(id) {
        bail!(
            "stash SHA-1 mismatch: id={id}, actual={actual} ({} bytes)",
            data.len()
        );
    }
    Ok(())
}

/// Write stash data to disk atomically: write to `.tmp`, fsync, rename.
fn write_stash_file(path: &Path, data: &[u8]) -> Result<()> {
    use std::io::Write;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));

    let mut tmp =
        tempfile::NamedTempFile::new_in(parent).context("stash: failed to create temp file")?;

    tmp.write_all(data)
        .context("stash: failed to write temp file")?;
    tmp.flush().context("stash: failed to flush temp file")?;
    tmp.as_file()
        .sync_all()
        .context("stash: failed to sync temp file")?;

    tmp.persist(path)
        .with_context(|| format!("stash: failed to rename temp file to {}", path.display()))?;

    Ok(())
}

/// Count regular files in a directory (non-recursive).
fn count_stash_files(dir: &Path) -> usize {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .count()
        })
        .unwrap_or(0)
}
