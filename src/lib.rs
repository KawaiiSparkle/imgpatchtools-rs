//! # imgpatchtools-rs
//!
//! Bit-exact, high-performance Rust reimplementation of the AOSP `updater`
//! block-image and apply-patch pipeline, focused on unpacking traditional
//! block-based Android OTA packages.
//!
//! ## Crate layout
//!
//! - [`core`] — high-level operations: block-image updates, apply-patch, OTA
//!   package reading, and sequential batch processing.
//! - [`util`] — shared low-level utilities: range sets, hashing, I/O helpers,
//!   progress reporting.

pub mod core;
pub mod util;
