//! `new` command implementation — port of AOSP `PerformCommandNew` from
//! `blockimg.cpp`.
//!
//! The `new` command reads fresh data from the `.new.dat[.br]` stream and
//! writes it to the target image at the specified block ranges.
//!
//! # Streaming constraint
//!
//! The new-data stream is **strictly sequential** — it cannot be seeked or
//! rewound. Each `new` command consumes the next `N` bytes from the stream,
//! where `N = target_ranges.blocks() × block_size`. Commands must be
//! executed in the exact order they appear in the transfer list.

use anyhow::{Context, Result};

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;

// ---------------------------------------------------------------------------
// cmd_new
// ---------------------------------------------------------------------------

/// Write new data from the `.new.dat[.br]` stream to `cmd.target_ranges`.
///
/// # Algorithm (matches AOSP `PerformCommandNew` exactly)
///
/// 1. Extract `target_ranges` from the command.
/// 2. Compute `total_blocks = target_ranges.blocks()`.
/// 3. Read `total_blocks × block_size` bytes sequentially from
///    `ctx.new_data`.
/// 4. Write the data to `ctx.target` at the positions specified by
///    `target_ranges`.
/// 5. Increment `ctx.written_blocks` by `total_blocks`.
///
/// # Errors
///
/// Returns an error if:
/// * `target_ranges` is missing.
/// * The new-data stream is exhausted before all bytes are read.
/// * The target write fails.
pub fn cmd_new(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let ranges = cmd
        .target_ranges
        .as_ref()
        .context("new: missing target_ranges")?;

    let total_blocks = ranges.blocks();

    log::debug!("new: {} blocks", total_blocks);

    // Read fresh data from the sequential new-data stream.
    let data = ctx
        .new_data
        .read_blocks(total_blocks, ctx.block_size)
        .with_context(|| {
            format!(
                "new: failed to read {} blocks ({} bytes) from new-data stream",
                total_blocks,
                total_blocks as usize * ctx.block_size,
            )
        })?;

    // Write to target at the specified ranges.
    ctx.target
        .write_ranges(ranges, &data)
        .context("new: failed to write to target")?;

    ctx.written_blocks += total_blocks;

    log::debug!(
        "new: wrote {} blocks (total written: {})",
        total_blocks,
        ctx.written_blocks
    );

    Ok(())
}
