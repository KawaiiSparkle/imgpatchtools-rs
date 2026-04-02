//! Per-command operation implementations for the block-image engine.
//!
//! Each AOSP transfer-list command is implemented as a standalone public
//! function in a dedicated sub-module and re-exported here for the command
//! registry.
//!
//! | Module          | Commands               |
//! |-----------------|------------------------|
//! | [`zero_erase`]  | `zero`, `erase`        |
//! | [`new_cmd`]     | `new`                  |
//! | [`stash_free`]  | `stash`, `free`        |
//! | [`move_cmd`]    | `move`                 |
//! | [`diff_bsdiff`] | `bsdiff`               |
//! | [`diff_imgdiff`]| `imgdiff`              |

mod diff_bsdiff;
mod diff_imgdiff;
pub(crate) mod move_cmd;
mod new_cmd;
mod stash_free;
mod zero_erase;

// Re-export all command handlers for the registry.
pub use diff_bsdiff::cmd_bsdiff;
pub use diff_imgdiff::cmd_imgdiff;
pub use move_cmd::cmd_move;
pub use new_cmd::cmd_new;
pub use stash_free::{cmd_free, cmd_stash};
pub use zero_erase::{cmd_erase, cmd_zero};
