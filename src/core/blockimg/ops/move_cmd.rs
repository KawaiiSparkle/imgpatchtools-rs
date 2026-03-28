use anyhow::{ensure, Result, Context};
use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::util::hash;

pub fn cmd_move(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let target_ranges = cmd.target_ranges.as_ref()
        .context("move: missing target_ranges")?;
    
    let nblk = cmd.src_block_count.unwrap_or_else(|| target_ranges.blocks());

let src_data = ctx.load_src_blocks(
    &cmd.src_ranges,
    &cmd.src_buffer_map,
    &cmd.src_stash_refs,
    nblk
)?;
    if let Some(ref expected_src_hash) = cmd.src_hash {
        let actual_src_hash = hash::sha1_hex(&src_data);
        ensure!(
            actual_src_hash == *expected_src_hash,
            "move: source hash mismatch: expected {}, got {} ({} bytes)",
            expected_src_hash, actual_src_hash, src_data.len()
        );
    }

    ctx.target.write_ranges(target_ranges, &src_data)?;

    if let Some(ref expected_tgt_hash) = cmd.target_hash {
        let tgt_bytes = ctx.target.read_ranges(target_ranges)?;
        let actual_tgt_hash = hash::sha1_hex(&tgt_bytes);
        ensure!(
            actual_tgt_hash == *expected_tgt_hash,
            "move: target hash mismatch: expected {}, got {}",
            expected_tgt_hash, actual_tgt_hash
        );
    }

    ctx.written_blocks += target_ranges.blocks();
    Ok(())
}