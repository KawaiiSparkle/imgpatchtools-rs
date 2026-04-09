//! `move` command — optimized block copy with minimal memory allocation.
//!
//! 方案3: Eliminates double buffering for move commands.
//! - Fast path: if no stash refs and target_hash == src_hash, use direct
//!   file copy (copy_ranges) + skip redundant target readback.
//! - General path: falls back to load_src_blocks for complex cases.
//!
//! 方案7: When target_hash equals src_hash (true for move commands in AOSP),
//! skip the target readback since we already verified src_hash.

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::util::hash;
use anyhow::{Context, Result, ensure};

pub fn cmd_move(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let target_ranges = cmd
        .target_ranges
        .as_ref()
        .context("move: missing target_ranges")?;

    let _nblk = cmd
        .src_block_count
        .unwrap_or_else(|| target_ranges.blocks());

    let src_ranges = cmd.src_ranges.as_ref();

    // 方案3 fast path: no stash refs, no buffer map → use direct file copy
    // This avoids allocating a large intermediate buffer.
    let is_simple =
        cmd.src_stash_refs.is_empty() && cmd.src_buffer_map.is_none() && src_ranges.is_some();

    // 方案7: if target_hash == src_hash, we can skip target readback
    // (move is a straight copy, so the hash should be identical)
    let tgt_equals_src = match (&cmd.target_hash, &cmd.src_hash) {
        (Some(t), Some(s)) => t.eq_ignore_ascii_case(s),
        _ => false,
    };

    if is_simple {
        // Fast path: direct copy without intermediate buffer
        let src_rs = src_ranges.unwrap();

        // Verify src_hash if present
        if let Some(ref expected_src_hash) = cmd.src_hash {
            let src_data = if let Some(ref s) = ctx.source {
                s.read_ranges(src_rs)?
            } else {
                ctx.target.read_ranges(src_rs)?
            };
            let actual_src_hash = hash::sha1_hex(&src_data);
            ensure!(
                actual_src_hash == *expected_src_hash,
                "move: source hash mismatch: expected {}, got {} ({} bytes)",
                expected_src_hash,
                actual_src_hash,
                src_data.len()
            );
            // For non-equal hashes, we need src_data for write anyway
            if !tgt_equals_src || cmd.target_hash.is_some() {
                // If tgt != src, we still need to write from buffer
                // (src_ranges might differ from target_ranges in block numbers)
                ctx.target.write_ranges(target_ranges, &src_data)?;

                if !tgt_equals_src {
                    // Must verify target hash since it differs from src
                    if let Some(ref expected_tgt_hash) = cmd.target_hash {
                        let actual_tgt_hash = hash::sha1_hex(&src_data);
                        ensure!(
                            actual_tgt_hash == *expected_tgt_hash,
                            "move: target hash mismatch: expected {}, got {}",
                            expected_tgt_hash,
                            actual_tgt_hash
                        );
                    }
                }
            } else {
                // tgt == src, no verification needed
                ctx.target.write_ranges(target_ranges, &src_data)?;
            }
        } else {
            // No src_hash: just copy directly (zero-allocation if source has mmap)
            if let Some(ref s) = ctx.source {
                ctx.target
                    .copy_ranges(target_ranges, s)
                    .context("move: failed to copy ranges from source")?;
            } else {
                // Self-copy: need to read first since source == target
                let src_data = ctx.target.read_ranges(src_rs)?;
                ctx.target.write_ranges(target_ranges, &src_data)?;
            }

            // Still verify target hash if present
            if let Some(ref expected_tgt_hash) = cmd.target_hash {
                let tgt_bytes = ctx.target.read_ranges(target_ranges)?;
                let actual_tgt_hash = hash::sha1_hex(&tgt_bytes);
                ensure!(
                    actual_tgt_hash == *expected_tgt_hash,
                    "move: target hash mismatch: expected {}, got {}",
                    expected_tgt_hash,
                    actual_tgt_hash
                );
            }
        }
    } else {
        // General path: complex case with stash refs or buffer map
        let src_ranges = cmd
            .src_ranges
            .as_ref()
            .context("move: missing src_ranges for complex path")?;
        let src_data =
            ctx.load_src_blocks(src_ranges, &cmd.src_stash_refs, cmd.src_buffer_map.as_ref())?;

        if let Some(ref expected_src_hash) = cmd.src_hash {
            let actual_src_hash = hash::sha1_hex(&src_data);
            ensure!(
                actual_src_hash == *expected_src_hash,
                "move: source hash mismatch: expected {}, got {} ({} bytes)",
                expected_src_hash,
                actual_src_hash,
                src_data.len()
            );
        }

        ctx.target.write_ranges(target_ranges, &src_data)?;

        if !tgt_equals_src {
            // Only readback when target hash differs from source hash
            if let Some(ref expected_tgt_hash) = cmd.target_hash {
                let tgt_bytes = ctx.target.read_ranges(target_ranges)?;
                let actual_tgt_hash = hash::sha1_hex(&tgt_bytes);
                ensure!(
                    actual_tgt_hash == *expected_tgt_hash,
                    "move: target hash mismatch: expected {}, got {}",
                    expected_tgt_hash,
                    actual_tgt_hash
                );
            }
        }
    }

    ctx.written_blocks += target_ranges.blocks();
    Ok(())
}
