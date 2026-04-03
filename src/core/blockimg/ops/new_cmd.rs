//! `new` command implementation — port of AOSP `PerformCommandNew` from
//! `blockimg.cpp`.

use anyhow::{Context, Result};

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;

// ---------------------------------------------------------------------------
// cmd_new
// ---------------------------------------------------------------------------

pub fn cmd_new(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("new: missing target_ranges")?;

    let total_blocks = ranges.blocks();

    log::debug!("new: {} blocks", total_blocks);

    let reader = ctx.new_data.get_reader_mut();
    let progress = &mut ctx.progress;
    let advanced = &mut ctx.blocks_advanced_this_cmd;

    ctx.target
        .write_ranges_from_reader(ranges, reader, |blocks| {
            progress.advance(blocks);
            *advanced += blocks;
        })
        .context("new: failed to write to target")?;

    ctx.written_blocks += total_blocks;

    log::debug!(
        "new: wrote {} blocks (total written: {})",
        total_blocks,
        ctx.written_blocks
    );

    Ok(())
}
