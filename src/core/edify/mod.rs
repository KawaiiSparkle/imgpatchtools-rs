//! Edify script engine — parser, function registry, and interpreter.
//!
//! # Submodules
//!
//! | Module       | Contents                                         |
//! |-------------|--------------------------------------------------|
//! | [`parser`]   | Tokenizer + recursive-descent parser → AST       |
//! | [`functions`]| Built-in function registry + script runner        |
//! | [`cli`]      | CLI subcommand definition and dispatch            |

pub mod cli;
pub mod functions;
pub mod parser;

/// Execute an Edify script file.
///
/// Convenience entry point used by the top-level CLI dispatch.
pub fn run(script_path: &str, workdir: &str) -> anyhow::Result<()> {
    let content = std::fs::read_to_string(script_path)
        .with_context(|| format!("failed to read {script_path}"))?;
    let registry = functions::builtin_registry();
    functions::run_script(&content, &registry, workdir)?;
    Ok(())
}

use anyhow::Context;