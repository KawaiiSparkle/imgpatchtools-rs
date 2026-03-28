//! CLI front-end for the `edify` subcommand.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;

use super::functions::{builtin_registry, run_script};

/// Arguments for the `edify` subcommand.
#[derive(Args, Debug, Clone)]
pub struct EdifyArgs {
    /// Path to the Edify script file (e.g. updater-script).
    pub script: PathBuf,

    /// Working directory for relative paths in the script.
    #[arg(short, long, default_value = ".")]
    pub workdir: String,
}

/// Execute the `edify` subcommand.
pub fn run(args: &EdifyArgs, verbose: bool) -> Result<()> {
    let content = std::fs::read_to_string(&args.script).with_context(|| {
        format!("failed to read script {}", args.script.display())
    })?;

    if verbose {
        log::info!("edify: executing {}", args.script.display());
        log::info!("edify: workdir = {}", args.workdir);
    }

    let registry = builtin_registry();
    let result = run_script(&content, &registry, &args.workdir)?;

    if verbose {
        log::info!("edify: script result = {:?}", result.as_str());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_basic_script() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("test.edify");
        std::fs::write(&script_path, r#"ui_print("hello from edify")"#).unwrap();

        let args = EdifyArgs {
            script: script_path,
            workdir: dir.path().to_string_lossy().into(),
        };
        run(&args, false).unwrap();
    }

    #[test]
    fn cli_nonexistent_script_fails() {
        let args = EdifyArgs {
            script: PathBuf::from("/no/such/script.edify"),
            workdir: ".".into(),
        };
        assert!(run(&args, false).is_err());
    }

    #[test]
    fn cli_sequence_script() {
        let dir = tempfile::tempdir().unwrap();
        let script_path = dir.path().join("seq.edify");
        std::fs::write(&script_path, r#"
            ui_print("step 1");
            ui_print("step 2");
            ui_print("step 3")
        "#).unwrap();

        let args = EdifyArgs {
            script: script_path,
            workdir: dir.path().to_string_lossy().into(),
        };
        run(&args, true).unwrap();
    }
}