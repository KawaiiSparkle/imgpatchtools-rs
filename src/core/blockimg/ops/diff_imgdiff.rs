use anyhow::{ensure, Result, Context};
use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::core::applypatch::imgpatch;
use crate::util::hash;

pub fn cmd_imgdiff(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let target_ranges = cmd.target_ranges.as_ref().context("imgdiff: missing target_ranges")?;
    let patch_offset = cmd.patch_offset.context("imgdiff: missing patch_offset")?;
    let patch_len = cmd.patch_len.context("imgdiff: missing patch_len")?;
    let nblk = cmd.src_block_count.context("imgdiff: missing src_block_count")?;

let src_data = ctx.load_src_blocks(
    &cmd.src_ranges,
    &cmd.src_buffer_map,
    &cmd.src_stash_refs,
    nblk
)?;
    if let Some(ref expected_src_hash) = cmd.src_hash {
        let actual_src_hash = hash::sha1_hex(&src_data);
        ensure!(actual_src_hash == *expected_src_hash, "imgdiff: source hash mismatch");
    }

    let patch_bytes = ctx.patch_data.read_patch(patch_offset, patch_len)?;
    let target_data = imgpatch::apply_imgpatch(&src_data, patch_bytes)?;

    ctx.target.write_ranges(target_ranges, &target_data)?;

    if let Some(ref expected_tgt_hash) = cmd.target_hash {
        let actual_tgt_hash = hash::sha1_hex(&target_data);
        ensure!(actual_tgt_hash == *expected_tgt_hash, "imgdiff: target hash mismatch");
    }

    ctx.written_blocks += target_ranges.blocks();
    Ok(())
}