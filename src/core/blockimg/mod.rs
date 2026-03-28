//! Block-image update engine — corresponds to AOSP `blockimg.cpp`.
//!
//! # Submodules
//!
//! | Module            | Contents                                          |
//! |-------------------|---------------------------------------------------|
//! | [`transfer_list`] | Transfer-list parser (v1–v4)                      |
//! | [`stash`]         | Stash manager (save / load / free)                |
//! | [`context`]       | Command execution context (`CommandContext`)       |
//! | [`commands`]      | Command registry and execution loop               |
//! | [`ops`]           | Per-command operation implementations              |
//! | [`update`]        | Top-level `block_image_update` orchestrator        |
//! | [`verify`]        | Read-only verification functions                   |
//! | [`resume`]        | Crash-resume checkpoint read/write                 |
//! | [`cli`]           | CLI subcommand definitions and dispatch            |

pub mod cli;
pub mod commands;
pub mod context;
pub mod ops;
pub mod resume;
pub mod stash;
pub mod transfer_list;
pub mod update;
pub mod verify;

/// Execute a full block-image update (convenience wrapper).
pub fn run(
    transfer_list: &str,
    new_data: &str,
    patch_data: &str,
    target: &str,
) -> anyhow::Result<()> {
    update::block_image_update(
        std::path::Path::new(target),
        std::path::Path::new(transfer_list),
        std::path::Path::new(new_data),
        std::path::Path::new(patch_data),
        None,
        std::path::Path::new("/tmp/imgpatchtools-stash"),
        false,
        None,
    )
}