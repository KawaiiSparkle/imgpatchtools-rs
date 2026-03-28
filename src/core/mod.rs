//! Core operation modules — each mirrors a component of the AOSP `updater`.
//!
//! | Module         | AOSP equivalent        |
//! |----------------|------------------------|
//! | [`applypatch`] | `applypatch/`          |
//! | [`blockimg`]   | `blockimg.cpp`         |
//! | [`edify`]      | Edify script engine    |

pub mod applypatch;
pub mod blockimg;
pub mod edify;