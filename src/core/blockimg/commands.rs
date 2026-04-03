use anyhow::{bail, Context, Result};
use std::collections::HashMap;

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::ops;
use crate::core::blockimg::transfer_list::{TransferCommand, TransferList};
use crate::util::rangeset::RangeSet;

/// Signature of a transfer-list command handler.
pub type CommandFn = fn(&mut CommandContext, &TransferCommand) -> Result<()>;

/// Command registry (name -> function).
#[derive(Clone)]
pub struct CommandRegistry {
    map: HashMap<&'static str, CommandFn>,
}

impl CommandRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &'static str, f: CommandFn) {
        self.map.insert(name, f);
    }

    pub fn get(&self, name: &str) -> Option<CommandFn> {
        self.map.get(name).copied()
    }
}

/// Build the builtin command registry (matches AOSP blockimg command set).
pub fn builtin_registry() -> CommandRegistry {
    let mut r = CommandRegistry::new();

    r.register("zero", ops::cmd_zero as CommandFn);
    r.register("new", ops::cmd_new as CommandFn);
    r.register("erase", ops::cmd_erase as CommandFn);
    r.register("move", ops::cmd_move as CommandFn);
    r.register("bsdiff", ops::cmd_bsdiff as CommandFn);
    r.register("imgdiff", ops::cmd_imgdiff as CommandFn);
    r.register("stash", ops::cmd_stash as CommandFn);
    r.register("free", ops::cmd_free as CommandFn);

    r
}

pub fn execute_command(
    registry: &CommandRegistry,
    ctx: &mut CommandContext,
    cmd: &TransferCommand,
) -> Result<()> {
    let name = cmd.cmd_type.as_str();
    let f = registry
        .get(name)
        .with_context(|| format!("unsupported command: {name}"))?;
    f(ctx, cmd)
}

pub fn execute_transfer_list(
    ctx: &mut CommandContext,
    list: &TransferList,
    registry: &CommandRegistry,
    resume_index: Option<usize>,
) -> Result<()> {
    let total = list.commands.len();

    let start = resume_index.map(|i| i + 1).unwrap_or(0);
    if start > total {
        bail!(
            "resume index {} out of range (commands={})",
            resume_index.unwrap(),
            total
        );
    }

    let progress_total: u64 = list
        .commands
        .iter()
        .skip(start)
        .map(|c| c.target_ranges.as_ref().map_or(0, |r| r.blocks()))
        .sum();
    ctx.progress.set_total(progress_total);

    for (i, cmd) in list.commands.iter().enumerate().skip(start) {
        ctx.progress
            .set_stage(&format!("[{}/{}] {}", i + 1, total, cmd.cmd_type.as_str()));

        // Reset the intra-command progress tracker
        ctx.blocks_advanced_this_cmd = 0;

        execute_command(registry, ctx, cmd).with_context(|| {
            format!(
                "command {} (index {}) failed: \"{}\"",
                cmd.cmd_type.as_str(),
                i,
                cmd.raw_line
            )
        })?;

        // Safely complement any progress that wasn't updated during the command
        let processed = cmd
            .target_ranges
            .as_ref()
            .map_or(0, |r: &RangeSet| r.blocks());

        let remaining = processed.saturating_sub(ctx.blocks_advanced_this_cmd);
        if remaining > 0 {
            ctx.progress.advance(remaining);
        }
    }

    ctx.progress.finish();

    Ok(())
}
