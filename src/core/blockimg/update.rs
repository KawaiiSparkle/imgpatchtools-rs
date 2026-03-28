//! Block-image update main function — complete port of AOSP
//! `PerformBlockImageUpdate` from `blockimg.cpp`.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use super::commands::{builtin_registry, execute_transfer_list};
use super::context::{CommandContext, NewDataReader, PatchDataReader};
use super::stash::StashManager;
use super::transfer_list::{parse_transfer_list, TransferList};
use crate::util::io::BlockFile;
use crate::util::progress::new_progress;
use crate::util::rangeset::RangeSet;

/// Default block size in bytes — matches AOSP `BLOCKSIZE` constant.
pub const BLOCK_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Execute a full block-image update.
#[allow(clippy::too_many_arguments)]
pub fn block_image_update(
    target_path: &Path,
    transfer_list_path: &Path,
    new_data_path: &Path,
    patch_data_path: &Path,
    source_path: Option<&Path>,
    stash_dir: &Path,
    verbose: bool,
    resume_file: Option<&Path>,
) -> Result<()> {
    // ---- 1. Parse transfer list ----
    let tl_content = fs::read_to_string(transfer_list_path).with_context(|| {
        format!(
            "failed to read transfer list {}",
            transfer_list_path.display()
        )
    })?;
    let tl = parse_transfer_list(&tl_content).context("failed to parse transfer list")?;

    // ---- 2. Print header info ----
    log_header_info(&tl);

    // ---- 3. Open / create target ----
    let mut target = open_or_create_target(target_path, &tl)?;

    // ---- 4. Open source (if provided) ----
    let source = open_source(source_path)?;

    // ---- 4.5 Pre-copy source into target (in-place semantics) ----
    //
    // AOSP uses the **same** fd for source and target — the partition is
    // updated in-place. In PC mode, if a separate source image was
    // provided, we must copy its content into the target *before* any
    // commands execute so that subsequent reads from target see the
    // original source data. This is the key to correct in-place update
    // semantics on PC.
    if let Some(ref src) = source {
        initialise_target_from_source(src, &mut target, &tl)?;
    }

    // ---- 5. Open new-data reader ----
    let new_data = NewDataReader::open(new_data_path).with_context(|| {
        format!("failed to open new-data {}", new_data_path.display())
    })?;

    // ---- 6. Open patch-data reader (mmap) ----
    let patch_data = PatchDataReader::open(patch_data_path).with_context(|| {
        format!("failed to open patch-data {}", patch_data_path.display())
    })?;

    // ---- 7. Initialise stash manager ----
    let stash = StashManager::new(
        stash_dir,
        BLOCK_SIZE,
        tl.header.stash_max_entries,
        tl.header.stash_max_blocks,
    )
    .context("failed to initialise stash manager")?;

    // ---- 8. Read resume checkpoint ----
    let resume_index = read_resume_index(resume_file)?;

    // ---- 9. Build context ----
    let progress = new_progress(verbose);
    let mut ctx = CommandContext::new(
        tl.version(),
        BLOCK_SIZE,
        target,
        source,
        stash,
        new_data,
        patch_data,
        progress,
    );

    // ---- 10. Execute all commands ----
    let registry = builtin_registry();
    execute_transfer_list(&mut ctx, &tl, &registry, resume_index)
        .context("transfer list execution failed")?;

    // ---- 11. Clean up stash ----
    ctx.stash.clear_all().context("failed to clean up stash")?;

    // ---- 12. Flush target ----
    ctx.target.flush().context("failed to flush target image")?;

    log::info!(
        "block_image_update complete: {} blocks written to {}",
        ctx.written_blocks,
        target_path.display()
    );

    Ok(())
}

/// Compute the SHA-1 of specific block ranges in a file.
pub fn range_sha1(
    file_path: &Path,
    ranges_str: &str,
    block_size: usize,
) -> Result<String> {
    let bf = BlockFile::open(file_path, block_size).with_context(|| {
        format!("failed to open {}", file_path.display())
    })?;
    let ranges = crate::util::rangeset::RangeSet::parse(ranges_str)
        .context("failed to parse ranges")?;
    let data = bf.read_ranges(&ranges).context("failed to read ranges")?;
    Ok(crate::util::hash::sha1_hex(&data))
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Log header information from the parsed transfer list.
fn log_header_info(tl: &TransferList) {
    log::info!("transfer list version: {}", tl.version());
    log::info!("total blocks: {}", tl.total_blocks());
    log::info!("commands: {}", tl.len());
    if tl.version() >= 2 {
        log::info!(
            "stash limits: {} entries, {} blocks",
            tl.header.stash_max_entries,
            tl.header.stash_max_blocks
        );
    }
}

/// Copy source image content into the target so that subsequent reads
/// from target see the original source data (in-place update semantics).
fn initialise_target_from_source(
    src: &BlockFile,
    target: &mut BlockFile,
    tl: &TransferList,
) -> Result<()> {
    // Determine how many blocks to copy — the minimum of source size and
    // target size (transfer list total_blocks).
    let src_blocks = src.total_blocks();
    let tgt_blocks = tl.total_blocks();
    let copy_blocks = src_blocks.min(tgt_blocks);

    if copy_blocks == 0 {
        return Ok(());
    }

    log::info!(
        "initialising target from source: copying {} blocks ({} bytes)",
        copy_blocks,
        copy_blocks as usize * BLOCK_SIZE
    );

    let ranges = RangeSet::from_range(0, copy_blocks);
    let data = src
        .read_ranges(&ranges)
        .context("initialise_target_from_source: failed to read source")?;
    target
        .write_ranges(&ranges, &data)
        .context("initialise_target_from_source: failed to write target")?;
    target
        .flush()
        .context("initialise_target_from_source: failed to flush target")?;

    Ok(())
}

/// Open an existing target file (read-write) or create a new one.
fn open_or_create_target(path: &Path, tl: &TransferList) -> Result<BlockFile> {
    if path.exists() {
        let meta = fs::metadata(path).with_context(|| {
            format!("failed to stat target {}", path.display())
        })?;

        if meta.len() > 0 {
            log::info!(
                "opening existing target: {} ({} bytes)",
                path.display(),
                meta.len()
            );
            return BlockFile::open(path, BLOCK_SIZE).with_context(|| {
                format!("failed to open target r/w {}", path.display())
            });
        }
    }

    log::info!(
        "creating target: {} ({} blocks, {} bytes)",
        path.display(),
        tl.total_blocks(),
        tl.total_blocks() as usize * BLOCK_SIZE,
    );

    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create parent dir {}", parent.display())
            })?;
        }
    }

    BlockFile::create(path, tl.total_blocks(), BLOCK_SIZE).with_context(|| {
        format!("failed to create target {}", path.display())
    })
}

/// Open an optional separate source file.
fn open_source(path: Option<&Path>) -> Result<Option<BlockFile>> {
    match path {
        Some(p) => {
            log::info!("opening source: {}", p.display());
            let bf = BlockFile::open(p, BLOCK_SIZE).with_context(|| {
                format!("failed to open source {}", p.display())
            })?;
            Ok(Some(bf))
        }
        None => {
            log::info!("no separate source — using target as source (incremental)");
            Ok(None)
        }
    }
}

/// Read the resume index from a `last_command` file.
fn read_resume_index(path: Option<&Path>) -> Result<Option<usize>> {
    let path = match path {
        Some(p) => p,
        None => return Ok(None),
    };

    if !path.exists() {
        log::info!("no resume file found at {}", path.display());
        return Ok(None);
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read resume file {}", path.display()))?;
    let trimmed = content.trim();

    if trimmed.is_empty() {
        log::info!("resume file is empty, starting from beginning");
        return Ok(None);
    }

    let last_completed: usize = trimmed
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .parse()
        .with_context(|| format!("bad resume index in {}: {:?}", path.display(), trimmed))?;

    let resume_at = last_completed + 1;
    log::info!(
        "resume file {}: last completed = {}, resuming at {}",
        path.display(),
        last_completed,
        resume_at
    );

    Ok(Some(resume_at))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::hash;

    const BS: usize = BLOCK_SIZE;

    fn write_file(dir: &tempfile::TempDir, name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        fs::write(&path, data).unwrap();
        path
    }

    fn make_tl_v1_new_only(total_blocks: u64) -> String {
        format!("1\n{total_blocks}\nnew 2,0,{total_blocks}\n")
    }

    fn make_tl_v3_zero_new(zero_blocks: u64, new_blocks: u64) -> String {
        let total = zero_blocks + new_blocks;
        format!(
            "3\n{total}\n0\n0\nzero 2,0,{zero_blocks}\nnew 2,{zero_blocks},{total}\n"
        )
    }

    #[test]
    fn update_new_only_v1() {
        let dir = tempfile::tempdir().unwrap();

        let num_blocks = 8u64;
        let new_data: Vec<u8> = (0..num_blocks)
            .flat_map(|blk| vec![(blk % 256) as u8; BS])
            .collect();

        let tl_path = write_file(&dir, "system.transfer.list", make_tl_v1_new_only(num_blocks).as_bytes());
        let nd_path = write_file(&dir, "system.new.dat", &new_data);
        let pd_path = write_file(&dir, "system.patch.dat", &[]);
        let target_path = dir.path().join("system.img");
        let stash_dir = dir.path().join("stash");

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            None,
        )
        .unwrap();

        let result = fs::read(&target_path).unwrap();
        assert_eq!(result, new_data);
    }

    #[test]
    fn update_zero_plus_new_v3() {
        let dir = tempfile::tempdir().unwrap();

        let zero_blocks = 4u64;
        let new_blocks = 6u64;
        let total = zero_blocks + new_blocks;

        let new_data = vec![0xAAu8; new_blocks as usize * BS];
        let tl = make_tl_v3_zero_new(zero_blocks, new_blocks);

        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let nd_path = write_file(&dir, "nd.dat", &new_data);
        let pd_path = write_file(&dir, "pd.dat", &[]);
        let target_path = dir.path().join("target.img");
        let stash_dir = dir.path().join("stash");

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            None,
        )
        .unwrap();

        let result = fs::read(&target_path).unwrap();
        assert_eq!(result.len(), total as usize * BS);
        assert!(result[..zero_blocks as usize * BS].iter().all(|&b| b == 0));
        assert!(result[zero_blocks as usize * BS..].iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn update_existing_target() {
        let dir = tempfile::tempdir().unwrap();

        let num_blocks = 4u64;
        let target_path = dir.path().join("target.img");
        fs::write(&target_path, vec![0xFFu8; num_blocks as usize * BS]).unwrap();

        let new_data = vec![0x11u8; num_blocks as usize * BS];
        let tl = make_tl_v1_new_only(num_blocks);

        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let nd_path = write_file(&dir, "nd.dat", &new_data);
        let pd_path = write_file(&dir, "pd.dat", &[]);
        let stash_dir = dir.path().join("stash");

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            None,
        )
        .unwrap();

        assert_eq!(fs::read(&target_path).unwrap(), new_data);
    }

    #[test]
    fn update_with_resume() {
        let dir = tempfile::tempdir().unwrap();

        let tl = "3\n8\n0\n0\nzero 2,0,4\nnew 2,4,8\n";
        let new_data = vec![0xBBu8; 4 * BS];

        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let nd_path = write_file(&dir, "nd.dat", &new_data);
        let pd_path = write_file(&dir, "pd.dat", &[]);
        let target_path = dir.path().join("target.img");
        let stash_dir = dir.path().join("stash");

        let resume_path = write_file(&dir, "last_command", b"0\n");

        fs::write(&target_path, vec![0xFFu8; 8 * BS]).unwrap();

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            Some(&resume_path),
        )
        .unwrap();

        let result = fs::read(&target_path).unwrap();
        assert!(result[..4 * BS].iter().all(|&b| b == 0xFF));
        assert!(result[4 * BS..].iter().all(|&b| b == 0xBB));
    }

    #[test]
    fn resume_index_none_when_no_file() {
        assert!(read_resume_index(None).unwrap().is_none());
    }

    #[test]
    fn resume_index_none_when_missing_file() {
        let path = std::path::PathBuf::from("/no/such/resume_file");
        assert!(read_resume_index(Some(&path)).unwrap().is_none());
    }

    #[test]
    fn resume_index_none_when_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(&dir, "resume", b"");
        assert!(read_resume_index(Some(&path)).unwrap().is_none());
    }

    #[test]
    fn resume_index_parses_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(&dir, "resume", b"5\n");
        assert_eq!(read_resume_index(Some(&path)).unwrap(), Some(6));
    }

    #[test]
    fn resume_index_with_extra_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(&dir, "resume", b"3\nextra data\n");
        assert_eq!(read_resume_index(Some(&path)).unwrap(), Some(4));
    }

    #[test]
    fn resume_index_bad_content_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_file(&dir, "resume", b"not_a_number\n");
        assert!(read_resume_index(Some(&path)).is_err());
    }

    #[test]
    fn range_sha1_basic() {
        let dir = tempfile::tempdir().unwrap();
        let data = vec![0xABu8; 4 * BS];
        let path = write_file(&dir, "img.bin", &data);

        let h = range_sha1(&path, "2,0,4", BS).unwrap();
        assert_eq!(h, hash::sha1_hex(&data));
    }

    #[test]
    fn range_sha1_subset() {
        let dir = tempfile::tempdir().unwrap();
        let data = vec![0xCDu8; 8 * BS];
        let path = write_file(&dir, "img.bin", &data);

        let h = range_sha1(&path, "2,2,6", BS).unwrap();
        assert_eq!(h, hash::sha1_hex(&data[2 * BS..6 * BS]));
    }

    #[test]
    fn stash_cleaned_after_update() {
        let dir = tempfile::tempdir().unwrap();

        let tl = make_tl_v1_new_only(4);
        let new_data = vec![0u8; 4 * BS];

        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let nd_path = write_file(&dir, "nd.dat", &new_data);
        let pd_path = write_file(&dir, "pd.dat", &[]);
        let target_path = dir.path().join("target.img");
        let stash_dir = dir.path().join("stash");

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            None,
        )
        .unwrap();

        if stash_dir.exists() {
            let entries: Vec<_> = fs::read_dir(&stash_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert!(entries.is_empty(), "stash dir should be empty");
        }
    }

    #[test]
    fn target_parent_dir_created() {
        let dir = tempfile::tempdir().unwrap();

        let tl = make_tl_v1_new_only(2);
        let new_data = vec![0u8; 2 * BS];

        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let nd_path = write_file(&dir, "nd.dat", &new_data);
        let pd_path = write_file(&dir, "pd.dat", &[]);
        let target_path = dir.path().join("sub").join("dir").join("target.img");
        let stash_dir = dir.path().join("stash");

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            None,
        )
        .unwrap();

        assert!(target_path.exists());
    }

    #[test]
    fn update_erase_only() {
        let dir = tempfile::tempdir().unwrap();

        let tl = "3\n8\n0\n0\nerase 2,0,8\n";
        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let nd_path = write_file(&dir, "nd.dat", &[]);
        let pd_path = write_file(&dir, "pd.dat", &[]);

        let target_path = dir.path().join("target.img");
        fs::write(&target_path, vec![0xFFu8; 8 * BS]).unwrap();

        let stash_dir = dir.path().join("stash");

        block_image_update(
            &target_path,
            &tl_path,
            &nd_path,
            &pd_path,
            None,
            &stash_dir,
            false,
            None,
        )
        .unwrap();

        let result = fs::read(&target_path).unwrap();
        assert!(result.iter().all(|&b| b == 0));
    }
}