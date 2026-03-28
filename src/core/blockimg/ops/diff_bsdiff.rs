use anyhow::{ensure, Result, Context};
use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::core::applypatch::bspatch;
use crate::util::hash;

pub fn cmd_bsdiff(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let target_ranges = cmd.target_ranges.as_ref().context("bsdiff: missing target_ranges")?;
    let patch_offset = cmd.patch_offset.context("bsdiff: missing patch_offset")?;
    let patch_len = cmd.patch_len.context("bsdiff: missing patch_len")?;
    let nblk = cmd.src_block_count.context("bsdiff: missing src_block_count")?;

let src_data = ctx.load_src_blocks(
    &cmd.src_ranges,
    &cmd.src_buffer_map,
    &cmd.src_stash_refs,
    nblk
)?;
    if let Some(ref expected_src_hash) = cmd.src_hash {
        let actual_src_hash = hash::sha1_hex(&src_data);
        ensure!(actual_src_hash == *expected_src_hash, "bsdiff: source hash mismatch");
    }

    let patch_bytes = ctx.patch_data.read_patch(patch_offset, patch_len)?;
    let target_data = bspatch::apply_bspatch_at(&src_data, patch_bytes, 0)?;

    ctx.target.write_ranges(target_ranges, &target_data)?;

    if let Some(ref expected_tgt_hash) = cmd.target_hash {
        let actual_tgt_hash = hash::sha1_hex(&target_data);
        ensure!(actual_tgt_hash == *expected_tgt_hash, "bsdiff: target hash mismatch");
    }

    ctx.written_blocks += target_ranges.blocks();
    Ok(())
}