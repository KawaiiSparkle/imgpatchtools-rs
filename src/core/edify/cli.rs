//! CLI front-end for the `edify` subcommand — with interactive super.img prompt.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Args;

use super::functions::{builtin_registry, run_script_with_mode};
use crate::core::super_img::builder::{GroupInfo, *};
use crate::core::super_img::lp_metadata::*;
use crate::core::super_img::op_list::DynamicPartitionState;
use crate::core::super_img::writer::{self, SuperImageFormat};

#[derive(Args, Debug, Clone)]
pub struct EdifyArgs {
    /// Path to the Edify script file.
    pub script: PathBuf,

    /// Working directory.
    #[arg(short, long, default_value = ".")]
    pub workdir: String,

    /// Verify mode: execute all commands including block_image_verify and assertions.
    /// Without this flag, only apply_patch, block_image_update and abort are executed.
    #[arg(long)]
    pub verify: bool,
}

pub fn run(args: &EdifyArgs, verbose: bool) -> Result<()> {
    let content = std::fs::read_to_string(&args.script)
        .with_context(|| format!("read {}", args.script.display()))?;

    if verbose {
        log::info!("edify: executing {}", args.script.display());
        log::info!("edify: workdir = {}", args.workdir);
        if args.verify {
            log::info!("edify: verify mode enabled (all commands will execute)");
        } else {
            log::info!("edify: fast mode (only apply_patch and block_image_update will execute)");
        }
    }

    let registry = builtin_registry();
    let result = run_script_with_mode(&content, &registry, &args.workdir, args.verify, true)?;

    if verbose {
        log::info!("edify: result = {:?}", result.value.as_str());
    }

    // --- AUTO RECOVERY PATCHING ---
    auto_patch_recovery(&args.workdir);

    // If dynamic partitions were detected, ask user interactively.
    if let Some(ref dp) = result.dynamic_partitions {
        prompt_build_super(dp, &args.workdir)?;
    }

    Ok(())
}

pub(crate) fn auto_patch_recovery(workdir: &str) {
    let boot_path = Path::new(workdir).join("boot.img");
    let recovery_path = Path::new(workdir).join("recovery.img");
    let sys_img = Path::new(workdir).join("system.img");
    let vendor_img = Path::new(workdir).join("vendor.img");

    // Skip if recovery.img is already present or boot.img is missing
    if recovery_path.exists() || !boot_path.exists() {
        return;
    }

    let target_img = if sys_img.exists() {
        sys_img
    } else if vendor_img.exists() {
        vendor_img
    } else {
        return;
    };

    let script_path = Path::new(workdir).join("install-recovery.sh");
    let patch_path = Path::new(workdir).join("recovery-from-boot.p");

    log::info!(
        "Checking for recovery patch files in {}...",
        target_img.display()
    );

    // Extract files via 7z silently
    let out = std::process::Command::new("7z")
        .arg("e")
        .arg(&target_img)
        .arg("-r")
        .arg("install-recovery.sh")
        .arg("recovery-from-boot.p")
        .arg(format!("-o{}", workdir))
        .arg("-y")
        .output();

    if let Ok(out) = out {
        if !out.status.success() || !script_path.exists() || !patch_path.exists() {
            // Clean up partial extractions
            let _ = std::fs::remove_file(&script_path);
            let _ = std::fs::remove_file(&patch_path);
            return;
        }
    } else {
        log::warn!("auto_patch_recovery: failed to spawn 7z command.");
        return;
    }

    let script = match std::fs::read_to_string(&script_path) {
        Ok(s) => s,
        Err(_) => {
            let _ = std::fs::remove_file(&script_path);
            let _ = std::fs::remove_file(&patch_path);
            return;
        }
    };

    // Parse target_sha1 and target_size from install-recovery.sh
    // Standard format: applypatch <src> <tgt> <tgt_sha1> <tgt_size> <src_sha1>:<patch>
    let mut target_sha1 = String::new();
    let mut target_size = 0u64;
    let mut found = false;

    for line in script.lines() {
        let line = line.trim();
        if line.contains("applypatch") && !line.contains("-c") && !line.starts_with('#') {
            let tokens: Vec<&str> = line.split_whitespace().collect();
            for (i, tok) in tokens.iter().enumerate() {
                // Heuristic: target_sha1 is exactly 40 hex chars, target_size follows it
                if tok.len() == 40 && tok.chars().all(|c| c.is_ascii_hexdigit()) {
                    if i + 1 < tokens.len() {
                        if let Ok(sz) = tokens[i + 1].parse::<u64>() {
                            target_sha1 = tok.to_string();
                            target_size = sz;
                            found = true;
                            break;
                        }
                    }
                }
            }
        }
        if found {
            break;
        }
    }

    if found {
        println!("\n=== Auto-patching recovery.img ===");
        println!("  Found install-recovery.sh!");
        println!("  Source:      boot.img");
        println!("  Patch:       recovery-from-boot.p");
        println!("  Target SHA1: {}", target_sha1);
        println!("  Target Size: {}", target_size);
        print!("  Patching... ");
        let _ = io::stdout().flush();

        match crate::core::applypatch::apply::apply_patch(
            &boot_path,
            &recovery_path,
            &target_sha1,
            target_size,
            &patch_path,
        ) {
            Ok(_) => {
                println!("Done!");
                println!("  -> Successfully generated recovery.img!");
            }
            Err(e) => {
                println!("Failed!");
                println!("  -> Error: {}", e);
            }
        }
    }

    // Always clean up the extracted script and patch file
    let _ = std::fs::remove_file(&script_path);
    let _ = std::fs::remove_file(&patch_path);
}

fn prompt_build_super(dp: &DynamicPartitionState, workdir: &str) -> Result<()> {
    println!();
    println!("=== Dynamic partitions detected ===");
    println!("Build a super.img from the partition images? [y/N]");
    print!("> ");
    io::stdout().flush()?;

    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if !answer.trim().eq_ignore_ascii_case("y") {
        println!("Skipping super.img generation.");
        return Ok(());
    }

    // Ask Android version.
    println!();
    println!("Select Android version for LP metadata format:");
    println!("  10  → v10.0 (Android 10, basic)");
    println!("  11  → v10.1 (Android 11, adds ATTR_UPDATED)");
    println!("  12  → v10.2 (Android 12+, adds header flags)");
    print!("Android version [12]: ");
    io::stdout().flush()?;

    let mut ver_str = String::new();
    io::stdin().read_line(&mut ver_str)?;
    let ver_str = ver_str.trim();
    let version = if ver_str.is_empty() {
        LpVersion::V1_2
    } else {
        LpVersion::from_android_version(ver_str).unwrap_or_else(|| {
            println!(
                "Unknown version '{}', defaulting to v10.2 (Android 12+)",
                ver_str
            );
            LpVersion::V1_2
        })
    };

    // Ask metadata slots.
    println!();
    println!("Metadata slots (1 = non-A/B, 2 = A/B)");
    print!("Slots [2]: ");
    io::stdout().flush()?;

    let mut slots_str = String::new();
    io::stdin().read_line(&mut slots_str)?;
    let metadata_slots: u32 = slots_str.trim().parse().unwrap_or(2).max(1);

    // Ask device size.
    println!();
    println!("Super partition total size in bytes (0 = auto-calculate):");
    print!("Device size [0]: ");
    io::stdout().flush()?;

    let mut size_str = String::new();
    io::stdin().read_line(&mut size_str)?;
    let user_device_size: u64 = size_str.trim().parse().unwrap_or(0);

    // Ask output format.
    println!();
    println!("Output format:");
    println!("  1 → Sparse (recommended, smaller file, 7-Zip/fastboot compatible)");
    println!("  2 → Raw (full device_size, for dd/direct flash)");
    print!("Format [1]: ");
    io::stdout().flush()?;

    let mut fmt_str = String::new();
    io::stdin().read_line(&mut fmt_str)?;
    let format = match fmt_str.trim() {
        "2" | "raw" => SuperImageFormat::Raw,
        _ => SuperImageFormat::Sparse,
    };

    // Ask output name.
    print!("Output filename [super.img]: ");
    io::stdout().flush()?;
    let mut name_str = String::new();
    io::stdin().read_line(&mut name_str)?;
    let super_name = if name_str.trim().is_empty() {
        "super.img"
    } else {
        name_str.trim()
    };

    // Build.
    do_build_super(
        dp,
        workdir,
        version,
        metadata_slots,
        user_device_size,
        format,
        super_name,
    )?;
    Ok(())
}

fn do_build_super(
    dp: &DynamicPartitionState,
    workdir: &str,
    version: LpVersion,
    metadata_slots: u32,
    user_device_size: u64,
    format: SuperImageFormat,
    super_name: &str,
) -> Result<()> {
    let fmt_label = match format {
        SuperImageFormat::Sparse => "sparse",
        SuperImageFormat::Raw => "raw",
    };
    println!();
    println!(
        "Building super.img (LP {}, {})...",
        version.label(),
        fmt_label
    );

    let metadata_max_size: u32 = 65536;

    let groups: Vec<GroupInfo> = dp
        .groups
        .iter()
        .map(|g| GroupInfo {
            name: g.name.clone(),
            max_size: g.max_size,
        })
        .collect();

    let mut partitions = Vec::new();
    let mut images: Vec<(String, String)> = Vec::new();

    for p in &dp.partitions {
        let img_path = Path::new(workdir).join(format!("{}.img", p.name));

        let actual_size = if img_path.exists() {
            let flen = std::fs::metadata(&img_path)?.len();
            p.size.max(flen)
        } else {
            if p.size > 0 {
                log::warn!(
                    "partition '{}': image not found, will be zero-filled",
                    p.name
                );
            }
            p.size
        };

        partitions.push(PartitionInfo {
            name: p.name.clone(),
            group_name: p.group_name.clone(),
            attributes: LP_PARTITION_ATTR_READONLY,
            size: actual_size,
        });

        if img_path.exists() {
            images.push((p.name.clone(), img_path.to_string_lossy().into()));
        }
    }

    let mut config = SuperConfig {
        device_size: 0,
        metadata_max_size,
        metadata_slots,
        block_device_name: "super".into(),
        alignment: LP_DEFAULT_ALIGNMENT,
        alignment_offset: 0,
        logical_block_size: 4096,
        groups,
        partitions,
        version,
        header_flags: 0,
    };

    config.device_size = if user_device_size > 0 {
        user_device_size
    } else {
        let sz = auto_device_size(&config);
        println!(
            "Auto device_size: {} ({:.2} GB)",
            sz,
            sz as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        sz
    };

    let metadata = build_metadata(&config).context("build LP metadata")?;

    let out = Path::new(workdir).join(super_name);
    writer::write_super(&out, &metadata, &images, format)?;

    let file_size = std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0);

    println!();
    println!("=== super.img created ===");
    println!("  Path: {}", out.display());
    println!(
        "  Device size: {} ({:.2} GB)",
        config.device_size,
        config.device_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  File size: {} ({:.2} GB)",
        file_size,
        file_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!("  Format: LP {} ({})", version.label(), fmt_label);
    println!("  Slots: {}", metadata_slots);
    println!();
    println!("  Partitions:");
    for p in &metadata.partitions {
        let mut sec = 0u64;
        for i in 0..p.num_extents {
            sec += metadata.extents[(p.first_extent_index + i) as usize].num_sectors;
        }
        let sz = sec * LP_SECTOR_SIZE;
        if sz > 0 {
            println!(
                "    {:<24} {:>12} bytes ({:.1} MB)",
                p.name_str(),
                sz,
                sz as f64 / 1048576.0
            );
        }
    }

    Ok(())
}
