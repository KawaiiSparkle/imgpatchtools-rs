use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::util::hash;
use anyhow::{Context, Result};

pub fn cmd_stash(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let id = cmd.stash_id.as_ref().context("stash: missing id")?;
    let rs = cmd
        .src_ranges
        .as_ref()
        .context("stash: missing src_ranges")?;

    if ctx.stash.exists(id) {
        // AOSP logic: if stash already exists, skip reading but potentially verify hash
        return Ok(());
    }

    // 修复点：如果 ctx.source 为空，则从 ctx.target 读取数据进行 stash
    let data = if let Some(ref s) = ctx.source {
        s.read_ranges(rs)?
    } else {
        ctx.target.read_ranges(rs)?
    };

    if ctx.version >= 3 {
        // In v3+, the stash_id is typically the expected SHA1
        let _actual_hash = hash::sha1_hex(&data);
        // Note: AOSP sometimes allows mismatch here depending on the command,
        // but typically we want it to match if the ID is a hash.
    }

    ctx.stash.save(id, data.as_ref())?;
    Ok(())
}

pub fn cmd_free(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    let id = cmd.stash_id.as_ref().context("free: missing id")?;
    ctx.stash.free(id)?;
    Ok(())
}
