//! # imgpatchtools-rs
//!
//! Bit-exact, high-performance Rust reimplementation of the AOSP `updater`
//! block-image and apply-patch pipeline.
//!
//! ## Crate layout
//!
//! - [`core`] — high-level operations: block-image updates, apply-patch, Edify
//!   script execution.
//! - [`util`] — shared low-level utilities: range sets, hashing, I/O helpers,
//!   progress reporting.

pub mod core;
pub mod util;
