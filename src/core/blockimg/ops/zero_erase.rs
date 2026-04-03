//! `zero` and `erase` command implementations — port of AOSP
//! `PerformCommandZero` and `PerformCommandErase` from `blockimg.cpp`.

use anyhow::{Context, Result};

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;

// ---------------------------------------------------------------------------
// cmd_zero
// ---------------------------------------------------------------------------

pub fn cmd_zero(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("zero: missing target_ranges")?;

    log::debug!("zero: {} blocks", ranges.blocks());

    let progress = &mut ctx.progress;
    let advanced = &mut ctx.blocks_advanced_this_cmd;

    ctx.target
        .zero_ranges_with_progress(ranges, |blocks| {
            progress.advance(blocks);
            *advanced += blocks;
        })
        .context("zero: failed to zero target ranges")?;

    ctx.written_blocks += ranges.blocks();
    Ok(())
}

// ---------------------------------------------------------------------------
// cmd_erase
// ---------------------------------------------------------------------------

pub fn cmd_erase(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("erase: missing target_ranges")?;

    log::debug!("erase: {} blocks (zero-fill fallback)", ranges.blocks());

    let progress = &mut ctx.progress;
    let advanced = &mut ctx.blocks_advanced_this_cmd;

    ctx.target
        .zero_ranges_with_progress(ranges, |blocks| {
            progress.advance(blocks);
            *advanced += blocks;
        })
        .context("erase: failed to zero target ranges")?;

    ctx.written_blocks += ranges.blocks();
    Ok(())
}
