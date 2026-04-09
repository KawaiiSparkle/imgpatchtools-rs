//! `imgdiff` command — port of AOSP `PerformCommandDiff` for imgdiff patches.
//!
//! Applies imgdiff patches with pre-verification optimization:
//! - Checks target blocks first; skips if already correct (resumable OTA)
//! - Verifies source hash before patching

use crate::core::applypatch::imgpatch;
use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::util::hash;
use anyhow::{Context, Result, ensure};

pub fn cmd_imgdiff(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let target_ranges = cmd
        .target_ranges
        .as_ref()
        .context("imgdiff: missing target_ranges")?;
    let patch_offset = cmd.patch_offset.context("imgdiff: missing patch_offset")?;
    let patch_len = cmd.patch_len.context("imgdiff: missing patch_len")?;

    // Get source ranges - required for imgdiff
    let src_ranges = cmd
        .src_ranges
        .as_ref()
        .context("imgdiff: missing src_ranges")?;

    // Step 1: Check if target already has expected content (resumable OTA)
    if let Some(ref expected_tgt_hash) = cmd.target_hash {
        match ctx.target.read_ranges(target_ranges) {
            Ok(tgt_data) => {
                let actual_tgt_hash = hash::sha1_hex(&tgt_data);
                if actual_tgt_hash == *expected_tgt_hash {
                    log::info!(
                        "imgdiff: target already has expected hash {}, skipping",
                        expected_tgt_hash
                    );
                    ctx.blocks_advanced_this_cmd = target_ranges.blocks();
                    return Ok(());
                }
            }
            Err(e) => {
                log::debug!("imgdiff: target read failed: {}", e);
            }
        }
    }

    // Step 2: Load source blocks (支持 stash refs，容错加载)
    let src_data =
        ctx.load_src_blocks(src_ranges, &cmd.src_stash_refs, cmd.src_buffer_map.as_ref())?;

    // Step 3: Verify source hash
    if let Some(ref expected_src_hash) = cmd.src_hash {
        let actual_src_hash = hash::sha1_hex(&src_data);
        ensure!(
            actual_src_hash == *expected_src_hash,
            "imgdiff: source hash mismatch: expected {}, got {}",
            expected_src_hash,
            actual_src_hash
        );
    }

    // Step 4: Read patch and apply imgdiff
    let patch_bytes = ctx.patch_data.read_patch(patch_offset, patch_len)?;
    let target_data = imgpatch::apply_imgpatch(&src_data, patch_bytes)?;

    // Step 5: Verify target data before writing
    if let Some(ref expected_tgt_hash) = cmd.target_hash {
        let actual_tgt_hash = hash::sha1_hex(&target_data);
        ensure!(
            actual_tgt_hash == *expected_tgt_hash,
            "imgdiff: target hash mismatch before write: expected {}, got {}",
            expected_tgt_hash,
            actual_tgt_hash
        );
    }

    // Step 6: Write to target
    ctx.target.write_ranges(target_ranges, &target_data)?;

    ctx.written_blocks += target_ranges.blocks();
    ctx.blocks_advanced_this_cmd = target_ranges.blocks();

    log::debug!(
        "imgdiff: patched {} blocks (total written: {})",
        target_ranges.blocks(),
        ctx.written_blocks
    );

    Ok(())
}
