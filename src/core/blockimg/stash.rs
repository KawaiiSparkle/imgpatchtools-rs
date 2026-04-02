//! Stash management — complete port of the stash subsystem in AOSP
//! `blockimg.cpp`.
//!
//! During a block-image update, the `move` / `bsdiff` / `imgdiff` commands
//! may need to read source blocks that will be overwritten before they are
//! consumed. The stash mechanism saves those blocks to a temporary location
//! (memory + on-disk file) so they can be retrieved later.
//!
//! # AOSP stash contract
//!
//! | Function       | AOSP equivalent    | Behaviour                          |
//! |----------------|--------------------|------------------------------------|
//! | [`new`]        | `CreateStash`      | Create work dir, clean up stale    |
//! | [`save`]       | `WriteStash`       | Write to cache + file              |
//! | [`load`]       | `LoadStash`        | Read from cache (or file fallback) |
//! | [`free`]       | `FreeStash`        | Remove from cache + delete file    |
//! | [`clear_all`]  | (cleanup path)     | Remove everything                  |
//!
//! # File naming
//!
//! In AOSP, stash IDs are the SHA-1 hex digest of the stashed data. Each
//! stash slot is persisted as `{work_dir}/{id}`. On load from disk, the
//! file's content is verified against the ID (= SHA-1).

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};

use crate::util::hash;

// ---------------------------------------------------------------------------
// StashManager
// ---------------------------------------------------------------------------

/// Manages stash slots for the block-image update engine.
///
/// Provides an in-memory cache backed by on-disk files for crash-safety.
/// The on-disk files enable interrupted updates to resume without
/// re-transferring data.
pub struct StashManager {
    /// Directory where stash files are stored.
    work_dir: PathBuf,
    /// Block size in bytes (used for size-sanity checks only; not for
    /// alignment, since stash data is always a whole number of blocks).
    block_size: usize,
    /// In-memory cache: `stash_id → data`.
    entries: HashMap<String, Vec<u8>>,
    /// Running total of blocks currently stashed (for progress / limits).
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
    /// * `work_dir`    — directory for stash file storage.
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

    /// Number of entries currently in the in-memory cache.
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
    /// 1. Verifies the SHA-1 of `data` matches `id` (stash IDs *are* the
    ///    SHA-1 of the stashed content in AOSP).
    /// 2. Writes `data` to the on-disk file `{work_dir}/{id}`.
    /// 3. Inserts `data` into the in-memory cache.
    ///
    /// If the slot already exists and the data matches, this is a no-op
    /// (idempotent for resume).
    ///
    /// # Errors
    ///
    /// Returns an error if the SHA-1 check fails or the file write fails.
    pub fn save(&mut self, id: &str, data: &[u8]) -> Result<()> {
        // If already cached with identical content, skip.
        if let Some(existing) = self.entries.get(id) {
            if existing.as_slice() == data {
                log::debug!("stash save: {} already cached, skipping", id);
                return Ok(());
            }
        }

        // Verify SHA-1: in AOSP the stash ID is the SHA-1 of the data.
        verify_stash_sha1(id, data)?;

        // Size sanity: must be a whole number of blocks.
        ensure!(
            data.len() % self.block_size == 0,
            "stash data length {} is not a multiple of block_size {}",
            data.len(),
            self.block_size
        );

        let blocks = (data.len() / self.block_size) as u64;

        // Check limits (AOSP checks available space; we check the
        // declared header limits).
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

        // Write to disk first (crash-safety: file must be durable before
        // we proceed).
        write_stash_file(&self.stash_path(id), data)
            .with_context(|| format!("stash save: failed to write {id}"))?;

        // Insert into cache.
        self.entries.insert(id.to_string(), data.to_vec());
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
    /// 1. If the data is in the in-memory cache, returns it directly (fast
    ///    path).
    /// 2. Otherwise, reads the on-disk file `{work_dir}/{id}`.
    /// 3. Verifies the SHA-1 of the loaded data matches `id`.
    /// 4. Inserts the loaded data into the in-memory cache for future use.
    ///
    /// # Errors
    ///
    /// Returns an error if the file does not exist, cannot be read, or
    /// its SHA-1 does not match.
    pub fn load(&mut self, id: &str) -> Result<Vec<u8>> {
        // Fast path: already cached.
        if let Some(data) = self.entries.get(id) {
            log::debug!("stash load: {} (cache hit)", id);
            return Ok(data.clone());
        }

        // Slow path: read from disk.
        let path = self.stash_path(id);
        let data = fs::read(&path)
            .with_context(|| format!("stash load: failed to read {}", path.display()))?;

        // Verify integrity.
        verify_stash_sha1(id, &data)
            .with_context(|| format!("stash load: integrity check failed for {id}"))?;

        // Populate cache.
        self.entries.insert(id.to_string(), data.clone());

        log::debug!(
            "stash load: {} (loaded from disk, {} bytes)",
            id,
            data.len()
        );

        Ok(data)
    }

    /// Load data from a stash slot without cloning — returns a reference.
    ///
    /// Identical to [`load`](Self::load) but avoids the allocation when
    /// the caller only needs a read reference (e.g. for the diff step).
    ///
    /// The data **must** already be in the cache (call [`load`] first if
    /// uncertain). Returns an error if the entry is not cached.
    pub fn load_ref(&self, id: &str) -> Result<&[u8]> {
        self.entries
            .get(id)
            .map(Vec::as_slice)
            .with_context(|| format!("stash load_ref: {id} not in cache"))
    }

    /// Release a stash slot — AOSP `FreeStash`.
    ///
    /// 1. Removes from the in-memory cache.
    /// 2. Deletes the on-disk file.
    ///
    /// If the slot does not exist, this is a silent no-op (matching AOSP
    /// behaviour on double-free during resume).
    pub fn free(&mut self, id: &str) -> Result<()> {
        // Remove from cache and update block count.
        if let Some(data) = self.entries.remove(id) {
            let blocks = (data.len() / self.block_size) as u64;
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const BS: usize = 4096;

    /// Build test data whose SHA-1 hex digest we know.
    /// Returns `(id, data)` where `id` = sha1_hex(data).
    fn make_stash_data(fill_byte: u8, num_blocks: usize) -> (String, Vec<u8>) {
        let data = vec![fill_byte; num_blocks * BS];
        let id = hash::sha1_hex(&data);
        (id, data)
    }

    /// Create a fresh StashManager in a temporary directory.
    fn make_manager() -> (tempfile::TempDir, StashManager) {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        let mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
        (dir, mgr)
    }

    fn make_manager_with_limits(
        max_entries: u32,
        max_blocks: u32,
    ) -> (tempfile::TempDir, StashManager) {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        let mgr = StashManager::new(&stash_dir, BS, max_entries, max_blocks).unwrap();
        (dir, mgr)
    }

    // ---- new --------------------------------------------------------------

    #[test]
    fn new_creates_directory() {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("deep").join("stash");
        assert!(!stash_dir.exists());

        let mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
        assert!(stash_dir.exists());
        assert_eq!(mgr.cached_count(), 0);
        assert_eq!(mgr.current_blocks(), 0);
    }

    #[test]
    fn new_existing_directory_ok() {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        fs::create_dir_all(&stash_dir).unwrap();

        let mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
        assert_eq!(mgr.cached_count(), 0);
    }

    #[test]
    fn new_zero_block_size_fails() {
        let dir = tempfile::tempdir().unwrap();
        assert!(StashManager::new(dir.path(), 0, 0, 0).is_err());
    }

    #[test]
    fn new_detects_leftover_files() {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        fs::create_dir_all(&stash_dir).unwrap();
        // Plant a fake leftover file.
        fs::write(stash_dir.join("abc123"), b"leftover").unwrap();

        let mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
        // The leftover is not in the cache but exists on disk.
        assert_eq!(mgr.cached_count(), 0);
        assert!(mgr.exists("abc123"));
    }

    // ---- save -------------------------------------------------------------

    #[test]
    fn save_basic() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xAA, 2);

        mgr.save(&id, &data).unwrap();

        assert!(mgr.exists(&id));
        assert_eq!(mgr.cached_count(), 1);
        assert_eq!(mgr.current_blocks(), 2);

        // File should exist on disk.
        assert!(mgr.stash_path(&id).exists());

        // File content should match.
        let on_disk = fs::read(mgr.stash_path(&id)).unwrap();
        assert_eq!(on_disk, data);
    }

    #[test]
    fn save_idempotent() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xBB, 3);

        mgr.save(&id, &data).unwrap();
        mgr.save(&id, &data).unwrap(); // should not error

        assert_eq!(mgr.cached_count(), 1);
        // Block count should NOT double.
        assert_eq!(mgr.current_blocks(), 3);
    }

    #[test]
    fn save_wrong_sha1_fails() {
        let (_dir, mut mgr) = make_manager();
        let data = vec![0xCC; 2 * BS];
        let wrong_id = "0000000000000000000000000000000000000000";

        assert!(mgr.save(wrong_id, &data).is_err());
        assert!(!mgr.exists(wrong_id));
    }

    #[test]
    fn save_non_block_aligned_fails() {
        let (_dir, mut mgr) = make_manager();
        let data = vec![0xDD; BS + 1]; // not a multiple of BS
        let id = hash::sha1_hex(&data);

        assert!(mgr.save(&id, &data).is_err());
    }

    #[test]
    fn save_multiple_entries() {
        let (_dir, mut mgr) = make_manager();
        let (id1, d1) = make_stash_data(0x11, 1);
        let (id2, d2) = make_stash_data(0x22, 2);
        let (id3, d3) = make_stash_data(0x33, 3);

        mgr.save(&id1, &d1).unwrap();
        mgr.save(&id2, &d2).unwrap();
        mgr.save(&id3, &d3).unwrap();

        assert_eq!(mgr.cached_count(), 3);
        assert_eq!(mgr.current_blocks(), 6);
    }

    // ---- save with limits -------------------------------------------------

    #[test]
    fn save_exceeds_max_entries_fails() {
        let (_dir, mut mgr) = make_manager_with_limits(2, 0);
        let (id1, d1) = make_stash_data(0x01, 1);
        let (id2, d2) = make_stash_data(0x02, 1);
        let (id3, d3) = make_stash_data(0x03, 1);

        mgr.save(&id1, &d1).unwrap();
        mgr.save(&id2, &d2).unwrap();
        assert!(mgr.save(&id3, &d3).is_err()); // 3rd entry exceeds limit of 2
    }

    #[test]
    fn save_exceeds_max_blocks_fails() {
        let (_dir, mut mgr) = make_manager_with_limits(0, 3);
        let (id1, d1) = make_stash_data(0x01, 2);
        let (id2, d2) = make_stash_data(0x02, 2);

        mgr.save(&id1, &d1).unwrap(); // 2 blocks, total = 2
        assert!(mgr.save(&id2, &d2).is_err()); // would be 4, limit is 3
    }

    // ---- load -------------------------------------------------------------

    #[test]
    fn load_from_cache() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xAA, 2);

        mgr.save(&id, &data).unwrap();
        let loaded = mgr.load(&id).unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn load_from_disk_after_cache_eviction() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xBB, 1);

        mgr.save(&id, &data).unwrap();

        // Simulate cache eviction by manually clearing (but NOT deleting
        // the file).
        let blocks = (mgr.entries.remove(&id).unwrap().len() / BS) as u64;
        mgr.current_blocks -= blocks;
        assert!(!mgr.entries.contains_key(&id));
        assert!(mgr.stash_path(&id).exists());

        // Load should fall back to disk.
        let loaded = mgr.load(&id).unwrap();
        assert_eq!(loaded, data);

        // Should now be in cache again.
        assert!(mgr.entries.contains_key(&id));
    }

    #[test]
    fn load_nonexistent_fails() {
        let (_dir, mut mgr) = make_manager();
        assert!(mgr.load("nonexistent").is_err());
    }

    #[test]
    fn load_corrupted_file_fails() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xCC, 1);

        mgr.save(&id, &data).unwrap();

        // Simulate cache eviction.
        mgr.entries.remove(&id);

        // Corrupt the on-disk file.
        let path = mgr.stash_path(&id);
        fs::write(&path, b"corrupted!").unwrap();

        // Load should fail SHA-1 verification.
        assert!(mgr.load(&id).is_err());
    }

    #[test]
    fn load_ref_cached() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xDD, 2);

        mgr.save(&id, &data).unwrap();
        let slice = mgr.load_ref(&id).unwrap();
        assert_eq!(slice, data.as_slice());
    }

    #[test]
    fn load_ref_not_cached_fails() {
        let (_dir, mgr) = make_manager();
        assert!(mgr.load_ref("missing").is_err());
    }

    // ---- free -------------------------------------------------------------

    #[test]
    fn free_basic() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xAA, 2);

        mgr.save(&id, &data).unwrap();
        assert!(mgr.exists(&id));
        assert_eq!(mgr.current_blocks(), 2);

        mgr.free(&id).unwrap();
        assert!(!mgr.exists(&id));
        assert_eq!(mgr.cached_count(), 0);
        assert_eq!(mgr.current_blocks(), 0);

        // File should be gone.
        assert!(!mgr.stash_path(&id).exists());
    }

    #[test]
    fn free_idempotent() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xBB, 1);

        mgr.save(&id, &data).unwrap();
        mgr.free(&id).unwrap();
        mgr.free(&id).unwrap(); // double-free should be OK (resume)
    }

    #[test]
    fn free_nonexistent_ok() {
        let (_dir, mut mgr) = make_manager();
        // Should not error — matching AOSP's silent no-op.
        mgr.free("nonexistent").unwrap();
    }

    #[test]
    fn free_updates_block_count() {
        let (_dir, mut mgr) = make_manager();
        let (id1, d1) = make_stash_data(0x11, 3);
        let (id2, d2) = make_stash_data(0x22, 5);

        mgr.save(&id1, &d1).unwrap();
        mgr.save(&id2, &d2).unwrap();
        assert_eq!(mgr.current_blocks(), 8);

        mgr.free(&id1).unwrap();
        assert_eq!(mgr.current_blocks(), 5);

        mgr.free(&id2).unwrap();
        assert_eq!(mgr.current_blocks(), 0);
    }

    // ---- exists -----------------------------------------------------------

    #[test]
    fn exists_cache_only() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xAA, 1);

        assert!(!mgr.exists(&id));
        mgr.save(&id, &data).unwrap();
        assert!(mgr.exists(&id));
    }

    #[test]
    fn exists_disk_only() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xBB, 1);

        mgr.save(&id, &data).unwrap();
        mgr.entries.remove(&id); // evict from cache

        // exists should still return true (checks disk).
        assert!(mgr.exists(&id));
    }

    #[test]
    fn exists_neither() {
        let (_dir, mgr) = make_manager();
        assert!(!mgr.exists("nope"));
    }

    // ---- clear_all --------------------------------------------------------

    #[test]
    fn clear_all_basic() {
        let (_dir, mut mgr) = make_manager();
        let (id1, d1) = make_stash_data(0x11, 1);
        let (id2, d2) = make_stash_data(0x22, 2);

        mgr.save(&id1, &d1).unwrap();
        mgr.save(&id2, &d2).unwrap();
        assert_eq!(mgr.cached_count(), 2);

        mgr.clear_all().unwrap();

        assert_eq!(mgr.cached_count(), 0);
        assert_eq!(mgr.current_blocks(), 0);
        assert!(!mgr.stash_path(&id1).exists());
        assert!(!mgr.stash_path(&id2).exists());
    }

    #[test]
    fn clear_all_empty() {
        let (_dir, mut mgr) = make_manager();
        mgr.clear_all().unwrap(); // no-op, should not error
    }

    #[test]
    fn clear_all_with_leftover_files() {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        fs::create_dir_all(&stash_dir).unwrap();
        fs::write(stash_dir.join("leftover1"), b"data").unwrap();
        fs::write(stash_dir.join("leftover2"), b"data").unwrap();

        let mut mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
        mgr.clear_all().unwrap();

        assert!(!stash_dir.join("leftover1").exists());
        assert!(!stash_dir.join("leftover2").exists());
    }

    // ---- Atomic write (crash safety) --------------------------------------

    #[test]
    fn save_file_is_durable() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xEE, 4);

        mgr.save(&id, &data).unwrap();

        // Drop the manager and re-read the file directly.
        drop(mgr);

        let on_disk = fs::read(_dir.path().join("stash").join(&id)).unwrap();
        assert_eq!(on_disk, data);
    }

    // ---- Save + free + re-save cycle (resume scenario) -------------------

    #[test]
    fn save_free_resave_cycle() {
        let (_dir, mut mgr) = make_manager();
        let (id, data) = make_stash_data(0xFF, 2);

        mgr.save(&id, &data).unwrap();
        assert_eq!(mgr.current_blocks(), 2);

        mgr.free(&id).unwrap();
        assert_eq!(mgr.current_blocks(), 0);

        // Re-save the same stash (e.g. on retry).
        mgr.save(&id, &data).unwrap();
        assert_eq!(mgr.current_blocks(), 2);
        assert_eq!(mgr.cached_count(), 1);
    }

    // ---- Load after manager recreation (resume scenario) -----------------

    #[test]
    fn resume_load_from_disk() {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        let (id, data) = make_stash_data(0xAB, 3);

        // First "session": save.
        {
            let mut mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
            mgr.save(&id, &data).unwrap();
        }

        // Second "session": new manager, load from disk.
        {
            let mut mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
            assert_eq!(mgr.cached_count(), 0);
            assert!(mgr.exists(&id));

            let loaded = mgr.load(&id).unwrap();
            assert_eq!(loaded, data);
            assert_eq!(mgr.cached_count(), 1);
        }
    }

    // ---- Concurrent saves to different slots ------------------------------

    #[test]
    fn multiple_independent_slots() {
        let (_dir, mut mgr) = make_manager();

        let slots: Vec<(String, Vec<u8>)> = (0u8..5)
            .map(|i| make_stash_data(i * 37 + 1, (i as usize) + 1))
            .collect();

        for (id, data) in &slots {
            mgr.save(id, data).unwrap();
        }
        assert_eq!(mgr.cached_count(), 5);

        for (id, data) in &slots {
            let loaded = mgr.load(id).unwrap();
            assert_eq!(&loaded, data);
        }

        let total_blocks: u64 = (1..=5).sum();
        assert_eq!(mgr.current_blocks(), total_blocks);

        mgr.clear_all().unwrap();
        assert_eq!(mgr.cached_count(), 0);
        assert_eq!(mgr.current_blocks(), 0);
    }

    // ---- verify_stash_sha1 ------------------------------------------------

    #[test]
    fn verify_sha1_correct() {
        let data = vec![0x42u8; 1024];
        let id = hash::sha1_hex(&data);
        verify_stash_sha1(&id, &data).unwrap();
    }

    #[test]
    fn verify_sha1_wrong() {
        let data = vec![0x42u8; 1024];
        assert!(verify_stash_sha1("bad_hash", &data).is_err());
    }

    #[test]
    fn verify_sha1_case_insensitive() {
        let data = vec![0x42u8; 1024];
        let id_upper = hash::sha1_hex(&data).to_ascii_uppercase();
        verify_stash_sha1(&id_upper, &data).unwrap();
    }

    // ---- work_dir accessor -----------------------------------------------

    #[test]
    fn work_dir_accessor() {
        let dir = tempfile::tempdir().unwrap();
        let stash_dir = dir.path().join("stash");
        let mgr = StashManager::new(&stash_dir, BS, 0, 0).unwrap();
        assert_eq!(mgr.work_dir(), stash_dir);
    }

    // ---- block_size accessor ---------------------------------------------

    #[test]
    fn block_size_accessor() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = StashManager::new(dir.path(), 512, 0, 0).unwrap();
        assert_eq!(mgr.block_size(), 512);
    }
}
