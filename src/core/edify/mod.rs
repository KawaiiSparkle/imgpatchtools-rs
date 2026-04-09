//! Edify script engine — parser, function registry, and interpreter.

pub mod cli;
pub mod functions;
pub mod parser;

// Re-export commonly used functions and types
pub use parser::{RangeSha1Info, read_range_sha1_from_script, read_range_sha1_info_from_script};

pub fn run(script_path: &str, workdir: &str) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(script_path)
        .with_context(|| format!("failed to read {script_path}"))?;
    let registry = functions::builtin_registry();
    functions::run_script(&content, &registry, workdir)?;
    Ok(())
}

use anyhow::Context;
