//! Core operation modules — each mirrors a component of the AOSP `updater`.
//!
//! | Module         | AOSP equivalent        |
//! |----------------|------------------------|
//! | [`applypatch`] | `applypatch/`          |
//! | [`blockimg`]   | `blockimg.cpp`         |
//! | [`batch`]      | sequential OTA chain   |
//! | [`ota`]        | OTA package reading    |

pub mod applypatch;
pub mod batch;
pub mod blockimg;
pub mod ota;
