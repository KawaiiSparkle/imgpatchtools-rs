//! Super partition image tools — Rust port of AOSP liblp + lpmake/lpdump/lpunpack.
//!
//! Supports LP metadata v10.0 (Android 10), v10.1 (Android 11), v10.2 (Android 12+).

pub mod builder;
pub mod cli;
pub mod lp_metadata;
pub mod op_list;
pub mod reader;
pub mod sparse;
pub mod writer;
