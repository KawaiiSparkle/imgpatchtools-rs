//! Block-image update main function — complete port of AOSP
//! `PerformBlockImageUpdate` from `blockimg.cpp`.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use super::commands::{builtin_registry, execute_transfer_list};
use super::context::{CommandContext, ParallelNewDataReader, PatchDataReader};
use super::stash::StashManager;
use super::transfer_list::{TransferList, parse_transfer_list};
use crate::util::io::BlockFile;
use crate::util::progress::new_progress;
use crate::util::rangeset::RangeSet;

pub const BLOCK_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

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
    let tl_content = fs::read_to_string(transfer_list_path).with_context(|| {
        format!(
            "failed to read transfer list {}",
            transfer_list_path.display()
        )
    })?;
    let tl = parse_transfer_list(&tl_content).context("failed to parse transfer list")?;

    // For V4 transfer lists, the header's total_blocks may represent changed blocks
    // rather than the actual partition size. Calculate the actual total from commands.
    let actual_total_blocks = calculate_actual_total_blocks(&tl);
    let header_total_blocks = tl.total_blocks();
    
    // Use the larger of the two values to ensure the target is large enough
    let effective_total_blocks = actual_total_blocks.max(header_total_blocks);
    
    if tl.version() >= 4 && actual_total_blocks != header_total_blocks {
        log::info!(
            "V4 transfer list: header reports {} blocks, actual max is {} blocks, using {}",
            header_total_blocks,
            actual_total_blocks,
            effective_total_blocks
        );
    }
    
    log_header_info(&tl, effective_total_blocks);

    let mut target = open_or_create_target(target_path, effective_total_blocks, &tl)?;

    let source = open_source(source_path)?;

    if let Some(ref src) = source {
        initialise_target_from_source(src, &mut target, effective_total_blocks)?;
    }

    let new_data = ParallelNewDataReader::open(new_data_path)
        .with_context(|| format!("failed to open new-data {}", new_data_path.display()))?;

    let patch_data = PatchDataReader::open(patch_data_path)
        .with_context(|| format!("failed to open patch-data {}", patch_data_path.display()))?;

    let stash = StashManager::new(
        stash_dir,
        BLOCK_SIZE,
        tl.header.stash_max_entries,
        tl.header.stash_max_blocks,
    )
    .context("failed to initialise stash manager")?;

    let resume_index = read_resume_index(resume_file)?;

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

    let registry = builtin_registry();
    execute_transfer_list(&mut ctx, &tl, &registry, resume_index)
        .context("transfer list execution failed")?;

    ctx.stash.clear_all().context("failed to clean up stash")?;
    ctx.target.flush().context("failed to flush target image")?;

    // 报告多线程诊断信息
    ctx.new_data.report_diagnostics();

    log::info!(
        "block_image_update complete: {} blocks written to {}",
        ctx.written_blocks,
        target_path.display()
    );

    Ok(())
}

pub fn range_sha1(file_path: &Path, ranges_str: &str, block_size: usize) -> Result<String> {
    use memmap2::MmapOptions;
    use sha1::{Digest, Sha1};

    let ranges =
        crate::util::rangeset::RangeSet::parse(ranges_str).context("failed to parse ranges")?;

    // Open file and mmap for zero-copy access (eliminates read syscalls)
    let file = std::fs::File::open(file_path)
        .with_context(|| format!("failed to open {}", file_path.display()))?;

    let file_len = file.metadata()?.len();
    if file_len == 0 {
        // Empty file - return SHA1 of empty data
        let hasher = Sha1::new();
        let res = hasher.finalize();
        return Ok(res.iter().map(|b| format!("{:02x}", b)).collect());
    }

    // Memory map the entire file for direct access - much faster than read()
    let mmap = unsafe { MmapOptions::new().map(&file)? };

    // Sequential SHA1 computation (must be in order)
    let mut hasher = Sha1::new();
    for (start, end) in ranges.iter() {
        let start_byte = (start as usize) * block_size;
        let end_byte = ((end as usize) * block_size).min(file_len as usize);
        hasher.update(&mmap[start_byte..end_byte]);
    }

    let res = hasher.finalize();
    Ok(res.iter().map(|b| format!("{:02x}", b)).collect())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Calculate actual total blocks from transfer list commands.
/// For V4 transfer lists, the header's total_blocks may represent changed blocks
/// rather than the actual partition size. This function computes the true total
/// by finding the maximum block number referenced in any command.
fn calculate_actual_total_blocks(tl: &TransferList) -> u64 {
    let mut max_block: u64 = 0;

    for cmd in &tl.commands {
        // Check target ranges
        if let Some(ref ranges) = cmd.target_ranges {
            for (_, end) in ranges.iter() {
                if end > max_block {
                    max_block = end;
                }
            }
        }
        // Check source ranges
        if let Some(ref ranges) = cmd.src_ranges {
            for (_, end) in ranges.iter() {
                if end > max_block {
                    max_block = end;
                }
            }
        }
        // Check hash tree ranges
        if let Some(ref ranges) = cmd.hash_tree_ranges {
            for (_, end) in ranges.iter() {
                if end > max_block {
                    max_block = end;
                }
            }
        }
    }

    max_block
}

fn log_header_info(tl: &TransferList, effective_total_blocks: u64) {
    log::info!("transfer list version: {}", tl.version());
    log::info!("header total blocks: {}", tl.total_blocks());
    log::info!("effective total blocks: {}", effective_total_blocks);
    log::info!("commands: {}", tl.len());
    if tl.version() >= 2 {
        log::info!(
            "stash limits: {} entries, {} blocks",
            tl.header.stash_max_entries,
            tl.header.stash_max_blocks
        );
    }
}

fn initialise_target_from_source(
    src: &BlockFile,
    target: &mut BlockFile,
    total_blocks: u64,
) -> Result<()> {
    let src_blocks = src.total_blocks();
    let copy_blocks = src_blocks.min(total_blocks);

    if copy_blocks == 0 {
        return Ok(());
    }

    log::info!(
        "initialising target from source: copying {} blocks ({} bytes)",
        copy_blocks,
        copy_blocks as usize * BLOCK_SIZE
    );

    let ranges = RangeSet::from_range(0, copy_blocks);
    target
        .copy_ranges(&ranges, src)
        .context("initialise_target_from_source: failed to copy ranges")?;
    target
        .flush()
        .context("initialise_target_from_source: failed to flush target")?;

    Ok(())
}

fn open_or_create_target(path: &Path, total_blocks: u64, _tl: &TransferList) -> Result<BlockFile> {
    let expected_len = total_blocks * BLOCK_SIZE as u64;

    if path.exists() {
        let meta = fs::metadata(path)
            .with_context(|| format!("failed to stat target {}", path.display()))?;

        if meta.len() == expected_len {
            log::info!(
                "opening existing target: {} ({} bytes)",
                path.display(),
                meta.len()
            );
        } else if meta.len() < expected_len {
            // File is too small - extend it
            log::info!(
                "extending existing target from {} to {} bytes",
                meta.len(),
                expected_len
            );
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .with_context(|| format!("failed to open target for resize {}", path.display()))?;
            file.set_len(expected_len)
                .with_context(|| format!("failed to resize target to {}", expected_len))?;
        } else {
            // File is larger than expected - keep it as is
            log::info!(
                "opening existing target: {} ({} bytes, larger than expected {})",
                path.display(),
                meta.len(),
                expected_len
            );
        }
        return BlockFile::open(path, BLOCK_SIZE)
            .with_context(|| format!("failed to open target r/w {}", path.display()));
    }

    log::info!(
        "creating target: {} ({} blocks, {} bytes)",
        path.display(),
        total_blocks,
        expected_len,
    );

    if let Some(parent) = path.parent()
        && !parent.exists() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create parent dir {}", parent.display()))?;
        }

    BlockFile::create(path, total_blocks, BLOCK_SIZE)
        .with_context(|| format!("failed to create target {}", path.display()))
}

fn open_source(path: Option<&Path>) -> Result<Option<BlockFile>> {
    match path {
        Some(p) => {
            log::info!("opening source: {}", p.display());
            let bf = BlockFile::open(p, BLOCK_SIZE)
                .with_context(|| format!("failed to open source {}", p.display()))?;
            Ok(Some(bf))
        }
        None => {
            log::info!("no separate source — using target as source (incremental or full)");
            Ok(None)
        }
    }
}

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
