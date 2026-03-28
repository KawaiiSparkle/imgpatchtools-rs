//! Shared low-level utilities used across all core modules.
//!
//! | Module       | Purpose                                     |
//! |-------------|---------------------------------------------|
//! | [`rangeset`] | Block-range set arithmetic                  |
//! | [`hash`]     | SHA-1 / SHA-256 helpers                     |
//! | [`io`]       | High-performance file I/O (mmap, block r/w) |
//! | [`progress`] | Progress-bar wrapper around `indicatif`     |

pub mod hash;
pub mod io;
pub mod progress;
pub mod rangeset;