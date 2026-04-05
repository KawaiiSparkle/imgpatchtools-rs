//! `new` command implementation — port of AOSP `PerformCommandNew` from
//! `blockimg.cpp`.
//!
//! Uses background thread decompression (ParallelNewDataReader) for
//! maximum throughput.

use anyhow::{Context, Result};

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;

// ---------------------------------------------------------------------------
// cmd_new with background thread decompression
// ---------------------------------------------------------------------------

pub fn cmd_new(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("new: missing target_ranges")?;

    let total_blocks = ranges.blocks();

    log::debug!("new: {} blocks (using background thread)", total_blocks);

    // Use parallel reader with callback-based write
    let progress = &mut ctx.progress;
    let advanced = &mut ctx.blocks_advanced_this_cmd;

    ctx.target
        .write_ranges_with_callback(
            ranges,
            |buf| ctx.new_data.read_exact(buf),
            |blocks| {
                progress.advance(blocks);
                *advanced += blocks;
            },
        )
        .context("new: failed to write to target")?;

    ctx.written_blocks += total_blocks;

    log::debug!(
        "new: wrote {} blocks (total written: {})",
        total_blocks,
        ctx.written_blocks
    );

    Ok(())
}
