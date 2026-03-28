//! Patch application engine — corresponds to AOSP `applypatch/`.
//!
//! Supports bsdiff and imgdiff patch formats with full SHA-1 / SHA-256
//! target-hash verification to guarantee bit-exact output.
//!
//! # Submodules
//!
//! | Module            | Contents                                              |
//! |-------------------|-------------------------------------------------------|
//! | [`types`]         | Core data types: `PatchType`, `FileContents`, errors  |
//! | [`bspatch`]       | BSDIFF40 patch application                            |
//! | [`imgdiff_format`]| IMGDIFF2 format definitions and parser                |
//! | [`imgpatch`]      | IMGDIFF2 patch application                            |
//! | [`apply`]         | High-level apply/check logic with idempotency         |
//! | [`cli`]           | CLI argument parsing and subcommand dispatch           |

pub mod apply;
pub mod bspatch;
pub mod cli;
pub mod imgdiff_format;
pub mod imgpatch;
pub mod types;
pub mod zlib_raw;
/// Create an imgdiff-format patch from `source` to `target`.
///
/// This is a standalone entry point for the `imgdiff` subcommand; it does
/// not belong to the apply pipeline but is hosted here because it shares
/// format knowledge.
pub fn imgdiff(
    _source: &str,
    _target: &str,
    _output: &str,
    _chunk_size: Option<usize>,
) -> anyhow::Result<()> {
    anyhow::bail!("imgdiff: awaiting module implementation")
}