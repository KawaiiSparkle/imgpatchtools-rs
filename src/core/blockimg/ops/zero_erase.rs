//! `zero` and `erase` command implementations — port of AOSP
//! `PerformCommandZero` and `PerformCommandErase` from `blockimg.cpp`.
//!
//! | Command | AOSP behaviour                        | PC-file behaviour        |
//! |---------|---------------------------------------|--------------------------|
//! | `zero`  | Write zeroes to target ranges          | Same                     |
//! | `erase` | `BLKDISCARD` ioctl on block device     | Write zeroes (fallback)  |
//!
//! Both commands update `ctx.written_blocks` by the number of blocks in the
//! target range set.

use anyhow::{Context, Result};

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;

// ---------------------------------------------------------------------------
// cmd_zero
// ---------------------------------------------------------------------------

/// Write zeroes to all blocks in `cmd.target_ranges`.
///
/// Corresponds to AOSP `PerformCommandZero`.
///
/// After writing, `ctx.written_blocks` is incremented by the number of
/// blocks zeroed.
pub fn cmd_zero(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("zero: missing target_ranges")?;

    log::debug!("zero: {} blocks", ranges.blocks());

    ctx.target
        .zero_ranges(ranges)
        .context("zero: failed to zero target ranges")?;

    ctx.written_blocks += ranges.blocks();
    Ok(())
}

// ---------------------------------------------------------------------------
// cmd_erase
// ---------------------------------------------------------------------------

/// Erase (discard) all blocks in `cmd.target_ranges`.
///
/// On a real block device (AOSP updater), this issues `BLKDISCARD`.
/// In our PC-file implementation, erase degrades to zero-fill, matching
/// AOSP's fallback when the ioctl is unavailable.
///
/// After erasing, `ctx.written_blocks` is incremented.
pub fn cmd_erase(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("erase: missing target_ranges")?;

    log::debug!("erase: {} blocks (zero-fill fallback)", ranges.blocks());

    ctx.target
        .zero_ranges(ranges)
        .context("erase: failed to zero target ranges")?;

    ctx.written_blocks += ranges.blocks();
    Ok(())
}

