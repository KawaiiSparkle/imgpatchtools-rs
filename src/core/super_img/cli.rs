//! CLI front-end for `super`, `lpmake`, `lpdump`, and `lpunpack` subcommands.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use clap::Args;

use crate::core::super_img::builder::{
    GroupInfo, PartitionInfo, SuperConfig, auto_device_size, build_metadata,
};
use crate::core::super_img::lp_metadata::{
    LP_DEFAULT_ALIGNMENT, LP_PARTITION_ATTR_READONLY, LP_SECTOR_SIZE, LP_TARGET_TYPE_LINEAR,
    LpVersion, read_name,
};
use crate::core::super_img::op_list::{DynamicPartitionState, parse_op_list};
use crate::core::super_img::reader::read_metadata;
use crate::core::super_img::writer::{SuperImageFormat, write_super};

// ═══════════════════════════════════════════════════════════════════════════════
// Argument structs
// ═══════════════════════════════════════════════════════════════════════════════

/// Smart super.img builder — auto-detect partitions from workdir.
///
/// NOTE: Only RAW format is supported. Use img2simg to convert to sparse if needed.
#[derive(Args, Debug, Clone)]
pub struct SuperArgs {
    /// Working directory containing *.img partition files.
    #[arg(short, long, default_value = ".")]
    pub workdir: String,

    /// Output super.img path.
    #[arg(short, long, default_value = "super.img")]
    pub output: String,

    /// Path to dynamic_partitions_list file (simple partition name list).
    /// When specified, only partitions listed in this file will be included.
    #[arg(long)]
    pub dynamic_list: Option<PathBuf>,

    /// Path to dynamic_partitions_op_list file (OTA operations).
    #[arg(long)]
    pub op_list: Option<PathBuf>,

    /// Comma-separated partition names to include.
    #[arg(long)]
    pub partitions: Option<String>,

    /// Group definitions "name:max_size" (repeatable).
    #[arg(long, num_args = 1)]
    pub groups: Vec<String>,

    /// LP metadata version (format: "major.minor", e.g., "10.0", "10.1", "10.2").
    /// Default: "10.0" (safest for most devices).
    ///
    /// IMPORTANT: Use lpdump on your device's super partition to determine the correct version.
    /// - 10.0: Android 10 (Q)
    /// - 10.1: Android 11 (R)
    /// - 10.2: Android 12+ (S/T/U), enables Virtual A/B
    #[arg(long, default_value = "10.0")]
    pub lp_version: String,

    /// Number of metadata slots (1 = non-A/B, 2 = A/B).
    #[arg(long, default_value = "2")]
    pub slots: u32,

    /// Super partition device size in bytes (0 = auto-calculate).
    #[arg(long, default_value = "0")]
    pub device_size: u64,

    /// Metadata region size in bytes.
    #[arg(long, default_value = "65536")]
    pub metadata_size: u32,
}

/// Expert super.img builder — like AOSP lpmake.
///
/// NOTE: Only RAW format is supported. Output is always RAW.
/// Users should manually convert to sparse if needed: img2simg raw.img sparse.img
#[derive(Args, Debug, Clone)]
pub struct LpmakeArgs {
    /// Output super.img path.
    #[arg(short, long, default_value = "super.img")]
    pub output: String,

    /// Super partition device size in bytes (auto-calculate if omitted).
    #[arg(long)]
    pub device_size: Option<u64>,

    /// Metadata region size in bytes.
    #[arg(long, default_value = "65536")]
    pub metadata_size: u32,

    /// Number of metadata slots.
    #[arg(long, default_value = "2")]
    pub metadata_slots: u32,

    /// Block device name.
    #[arg(long, default_value = "super")]
    pub block_device_name: String,

    /// Partition alignment in bytes.
    #[arg(long, default_value = "1048576")]
    pub alignment: u32,

    /// Logical block size in bytes.
    #[arg(long, default_value = "4096")]
    pub logical_block_size: u32,

    /// LP metadata version (format: "major.minor", e.g., "10.0", "10.1", "10.2").
    /// Default: "10.0" (safest for most devices).
    ///
    /// IMPORTANT: Use lpdump on your device's super partition to determine the correct version.
    /// - 10.0: Android 10 (Q)
    /// - 10.1: Android 11 (R)
    /// - 10.2: Android 12+ (S/T/U), enables Virtual A/B
    #[arg(long, default_value = "10.0")]
    pub lp_version: String,

    /// Partition spec "name:group:size[:attr]" (repeatable).
    #[arg(long, num_args = 1)]
    pub partition: Vec<String>,

    /// Group spec "name:max_size" (repeatable).
    #[arg(long, num_args = 1)]
    pub group: Vec<String>,
}

/// Dump LP metadata from a super.img — like AOSP lpdump.
#[derive(Args, Debug, Clone)]
pub struct LpdumpArgs {
    /// Path to the super.img file.
    pub image: PathBuf,

    /// Metadata slot number.
    #[arg(long, default_value = "0")]
    pub slot: u32,
}

/// Extract partition images from a super.img — like AOSP lpunpack.
#[derive(Args, Debug, Clone)]
pub struct LpunpackArgs {
    /// Path to the super.img file.
    pub image: PathBuf,

    /// Output directory for extracted images.
    #[arg(short, long, default_value = ".")]
    pub output: String,

    /// Metadata slot number.
    #[arg(long, default_value = "0")]
    pub slot: u32,

    /// Comma-separated partition names to extract (all if omitted).
    #[arg(long)]
    pub partitions: Option<String>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// `super` command
// ═══════════════════════════════════════════════════════════════════════════════

pub fn run(args: &SuperArgs) -> Result<()> {
    let version = parse_lp_version(&args.lp_version);
    let metadata_slots = args.slots.max(1);
    // Output is always RAW format
    let format = SuperImageFormat::Raw;

    // Resolve partition and group lists.
    let dp = resolve_partitions(args)?;

    // Collect image paths for each partition.
    let mut partitions = Vec::new();
    let mut images: Vec<(String, String)> = Vec::new();

    for p in &dp.partitions {
        let img_path = Path::new(&args.workdir).join(format!("{}.img", p.name));
        let actual_size = if img_path.exists() {
            let file_len = fs::metadata(&img_path)
                .context("stat partition image")?
                .len();
            p.size.max(file_len)
        } else if p.size > 0 {
            log::warn!(
                "partition '{}': image not found, will be zero-filled",
                p.name
            );
            p.size
        } else {
            continue;
        };

        partitions.push(PartitionInfo {
            name: p.name.clone(),
            group_name: p.group_name.clone(),
            attributes: LP_PARTITION_ATTR_READONLY,
            size: actual_size,
        });

        if img_path.exists() {
            images.push((p.name.clone(), img_path.to_string_lossy().into_owned()));
        }
    }

    // Build groups list from the op_list or args.
    let groups: Vec<GroupInfo> = dp
        .groups
        .iter()
        .map(|g| GroupInfo {
            name: g.name.clone(),
            max_size: g.max_size,
        })
        .collect();

    // Also add any groups specified via --groups CLI arg.
    let mut extra_groups: Vec<GroupInfo> = Vec::new();
    for g_spec in &args.groups {
        let parts: Vec<&str> = g_spec.splitn(2, ':').collect();
        ensure!(
            parts.len() == 2,
            "invalid group spec '{}', expected name:max_size",
            g_spec
        );
        let name = parts[0].to_string();
        let max_size: u64 = parts[1].parse().context("parse group max_size")?;
        extra_groups.push(GroupInfo { name, max_size });
    }

    let mut all_groups = groups;
    all_groups.extend(extra_groups);

    // Build config and metadata.
    let mut config = SuperConfig {
        device_size: 0,
        metadata_max_size: args.metadata_size,
        metadata_slots,
        block_device_name: "super".into(),
        alignment: LP_DEFAULT_ALIGNMENT,
        alignment_offset: 0,
        logical_block_size: 4096,
        groups: all_groups,
        partitions,
        version,
        header_flags: 0,
    };

    config.device_size = if args.device_size > 0 {
        args.device_size
    } else {
        let sz = auto_device_size(&config);
        log::info!(
            "auto device_size: {} ({:.2} GB)",
            sz,
            sz as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        sz
    };

    let metadata = build_metadata(&config).context("build LP metadata")?;
    let out = Path::new(&args.output);
    write_super(out, &metadata, &images, format)?;

    // Print summary.
    let file_size = fs::metadata(out).map(|m| m.len()).unwrap_or(0);
    let fmt_label = match format {
        SuperImageFormat::Sparse => "sparse",
        SuperImageFormat::Raw => "raw",
    };

    println!();
    println!("=== super.img created ===");
    println!("  Path:         {}", out.display());
    println!(
        "  Device size:  {} ({:.2} GB)",
        config.device_size,
        config.device_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  File size:    {} ({:.2} MB)",
        file_size,
        file_size as f64 / 1048576.0
    );
    println!("  Format:       LP {} ({})", version.label(), fmt_label);
    println!("  Slots:        {}", metadata_slots);
    println!("  Partitions:   {}", metadata.partitions.len());
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

/// Resolve partition/group lists from op_list, --partitions, or workdir scan.
fn resolve_partitions(args: &SuperArgs) -> Result<DynamicPartitionState> {
    if let Some(ref op_list_path) = args.op_list {
        let content = fs::read_to_string(op_list_path)
            .with_context(|| format!("read op_list {}", op_list_path.display()))?;
        return parse_op_list(&content).context("parse op_list");
    }

    // dynamic_partitions_list — simple partition name list (one per line)
    if let Some(ref dp_list_path) = args.dynamic_list {
        let content = fs::read_to_string(dp_list_path)
            .with_context(|| format!("read dynamic_list {}", dp_list_path.display()))?;
        let names: Vec<&str> = content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        let mut dp = DynamicPartitionState::default();
        for name in names {
            let img_path = Path::new(&args.workdir).join(format!("{}.img", name));
            let size = if img_path.exists() {
                fs::metadata(&img_path)
                    .with_context(|| format!("stat {}", img_path.display()))?
                    .len()
            } else {
                0
            };
            dp.partitions
                .push(crate::core::super_img::op_list::PartitionState {
                    name: name.to_string(),
                    group_name: "default".to_string(),
                    size,
                });
        }
        ensure!(
            !dp.partitions.is_empty(),
            "no partitions found in dynamic_list"
        );
        return Ok(dp);
    }

    if let Some(ref part_list) = args.partitions {
        let names: Vec<&str> = part_list
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .collect();
        let mut dp = DynamicPartitionState::default();
        for name in names {
            let img_path = Path::new(&args.workdir).join(format!("{}.img", name));
            let size = if img_path.exists() {
                fs::metadata(&img_path)
                    .with_context(|| format!("stat {}", img_path.display()))?
                    .len()
            } else {
                0
            };
            dp.partitions
                .push(crate::core::super_img::op_list::PartitionState {
                    name: name.to_string(),
                    group_name: "default".to_string(),
                    size,
                });
        }
        return Ok(dp);
    }

    // Auto-detect dynamic_partitions_op_list or dynamic_partitions_list in workdir.
    let dir = Path::new(&args.workdir);
    let op_list_auto = dir.join("dynamic_partitions_op_list");
    if op_list_auto.exists() {
        log::info!("auto-detected {} in workdir", op_list_auto.display());
        let content = fs::read_to_string(&op_list_auto)
            .with_context(|| format!("read {}", op_list_auto.display()))?;
        return parse_op_list(&content).context("parse auto-detected op_list");
    }
    let dp_list_auto = dir.join("dynamic_partitions_list");
    if dp_list_auto.exists() {
        log::info!("auto-detected {} in workdir", dp_list_auto.display());
        let content = fs::read_to_string(&dp_list_auto)
            .with_context(|| format!("read {}", dp_list_auto.display()))?;
        let names: Vec<&str> = content
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        let mut dp = DynamicPartitionState::default();
        for name in names {
            let img_path = dir.join(format!("{}.img", name));
            let size = if img_path.exists() {
                fs::metadata(&img_path)
                    .with_context(|| format!("stat {}", img_path.display()))?
                    .len()
            } else {
                0
            };
            dp.partitions
                .push(crate::core::super_img::op_list::PartitionState {
                    name: name.to_string(),
                    group_name: "default".to_string(),
                    size,
                });
        }
        ensure!(
            !dp.partitions.is_empty(),
            "no partitions found in auto-detected dynamic_partitions_list"
        );
        return Ok(dp);
    }

    // Auto-scan workdir for *.img files.
    scan_workdir(&args.workdir)
}

/// Scan a directory for *.img files and build a partition list.
fn scan_workdir(workdir: &str) -> Result<DynamicPartitionState> {
    let dir = Path::new(workdir);
    ensure!(dir.is_dir(), "workdir '{}' is not a directory", workdir);

    let mut dp = DynamicPartitionState::default();
    let mut entries: Vec<String> = Vec::new();

    for entry in fs::read_dir(dir).context("read workdir")? {
        let entry = entry.context("read dir entry")?;
        let path = entry.path();
        if path.is_file()
            && let Some(stem) = path.file_stem()
                && let Some(ext) = path.extension()
                    && ext == std::ffi::OsStr::new("img") {
                        entries.push(stem.to_string_lossy().into_owned());
                    }
    }
    entries.sort();

    for name in entries {
        let size = fs::metadata(dir.join(format!("{}.img", &name)))
            .map(|m| m.len())
            .unwrap_or(0);
        dp.partitions
            .push(crate::core::super_img::op_list::PartitionState {
                name,
                group_name: "default".to_string(),
                size,
            });
    }

    ensure!(
        !dp.partitions.is_empty(),
        "no *.img files found in '{}'",
        workdir
    );
    log::info!(
        "auto-detected {} partitions in '{}'",
        dp.partitions.len(),
        workdir
    );
    Ok(dp)
}

// ═══════════════════════════════════════════════════════════════════════════════
// `lpmake` command
// ═══════════════════════════════════════════════════════════════════════════════

pub fn run_lpmake(args: &LpmakeArgs) -> Result<()> {
    ensure!(
        !args.partition.is_empty(),
        "at least one --partition spec is required (name:group:size[:attr])"
    );

    let version = parse_lp_version(&args.lp_version);
    // Output is always RAW format
    let format = SuperImageFormat::Raw;
    let metadata_slots = args.metadata_slots.max(1);

    // Parse groups: "name:max_size"
    let mut groups: Vec<GroupInfo> = Vec::new();
    for g_spec in &args.group {
        let parts: Vec<&str> = g_spec.splitn(2, ':').collect();
        ensure!(
            parts.len() == 2,
            "invalid group spec '{}', expected name:max_size",
            g_spec
        );
        let name = parts[0].to_string();
        let max_size: u64 = parts[1].parse().context("parse group max_size")?;
        groups.push(GroupInfo { name, max_size });
    }

    // Parse partitions: "name:group:size[:attr]"
    let mut partitions = Vec::new();
    for p_spec in &args.partition {
        let parts: Vec<&str> = p_spec.split(':').collect();
        ensure!(
            parts.len() >= 3,
            "invalid partition spec '{}', expected name:group:size[:attr]",
            p_spec
        );
        let name = parts[0].to_string();
        let group_name = parts[1].to_string();
        let size: u64 = parts[2].parse().context("parse partition size")?;
        let attributes: u32 = if parts.len() > 3 {
            parts[3]
                .parse::<u32>()
                .context("parse partition attributes")?
        } else {
            LP_PARTITION_ATTR_READONLY
        };
        partitions.push(PartitionInfo {
            name,
            group_name,
            attributes,
            size,
        });
    }

    // Auto-calculate device_size if not provided.
    let mut config = SuperConfig {
        device_size: args.device_size.unwrap_or(0),
        metadata_max_size: args.metadata_size,
        metadata_slots,
        block_device_name: args.block_device_name.clone(),
        alignment: args.alignment,
        alignment_offset: 0,
        logical_block_size: args.logical_block_size,
        groups,
        partitions,
        version,
        header_flags: 0,
    };

    if config.device_size == 0 {
        config.device_size = auto_device_size(&config);
        log::info!(
            "auto device_size: {} ({:.2} GB)",
            config.device_size,
            config.device_size as f64 / (1024.0 * 1024.0 * 1024.0)
        );
    }

    let metadata = build_metadata(&config).context("build LP metadata")?;

    // lpmake is metadata-only — no image data.
    let images: Vec<(String, String)> = Vec::new();
    let out = Path::new(&args.output);
    write_super(out, &metadata, &images, format)?;

    let file_size = fs::metadata(out).map(|m| m.len()).unwrap_or(0);
    let fmt_label = match format {
        SuperImageFormat::Sparse => "sparse",
        SuperImageFormat::Raw => "raw",
    };

    println!();
    println!("=== super.img created (lpmake) ===");
    println!("  Path:         {}", out.display());
    println!(
        "  Device size:  {} ({:.2} GB)",
        config.device_size,
        config.device_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );
    println!(
        "  File size:    {} ({:.2} MB)",
        file_size,
        file_size as f64 / 1048576.0
    );
    println!("  Format:       LP {} ({})", version.label(), fmt_label);
    println!("  Slots:        {}", metadata_slots);
    println!("  Partitions:   {}", metadata.partitions.len());
    for p in &metadata.partitions {
        let mut sec = 0u64;
        for i in 0..p.num_extents {
            sec += metadata.extents[(p.first_extent_index + i) as usize].num_sectors;
        }
        let sz = sec * LP_SECTOR_SIZE;
        println!(
            "    {:<24} {:>12} bytes ({:.1} MB)",
            p.name_str(),
            sz,
            sz as f64 / 1048576.0
        );
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// `lpdump` command
// ═══════════════════════════════════════════════════════════════════════════════

pub fn run_lpdump(args: &LpdumpArgs) -> Result<()> {
    let metadata = read_metadata(&args.image, args.slot)
        .with_context(|| format!("read metadata from {}", args.image.display()))?;

    let geo = &metadata.geometry;
    let hdr = &metadata.header;
    let version_label = format!("v{}.{}", hdr.major_version, hdr.minor_version);

    println!();
    println!("=== LP Metadata Dump ===");
    println!("  File:              {}", args.image.display());
    println!("  Slot:              {}", args.slot);
    println!("  Version:           {}", version_label);
    println!("  Header size:       {} bytes", hdr.header_size);
    println!("  Tables size:       {} bytes", hdr.tables_size);
    println!();

    // Geometry
    println!("Geometry:");
    println!("  Metadata max size: {}", geo.metadata_max_size);
    println!("  Metadata slots:    {}", geo.metadata_slot_count);
    println!("  Logical block size: {}", geo.logical_block_size);
    println!();

    // Block devices
    println!("Block devices ({}):", metadata.block_devices.len());
    for (i, bd) in metadata.block_devices.iter().enumerate() {
        let name = read_name(&bd.partition_name);
        println!("  [{}] {}", i, name);
        println!(
            "       Size:                {} ({:.2} GB)",
            bd.size,
            bd.size as f64 / (1024.0 * 1024.0 * 1024.0)
        );
        println!("       Alignment:           {}", bd.alignment);
        println!("       Alignment offset:    {}", bd.alignment_offset);
        println!("       First logical sector: {}", bd.first_logical_sector);
        println!("       Flags:               0x{:08x}", bd.flags);
    }
    println!();

    // Groups
    println!("Partition groups ({}):", metadata.groups.len());
    for (i, g) in metadata.groups.iter().enumerate() {
        let name = g.name_str();
        println!("  [{}] {:<24} max_size={}", i, name, g.maximum_size);
    }
    println!();

    // Partitions
    println!("Partitions ({}):", metadata.partitions.len());
    for (i, p) in metadata.partitions.iter().enumerate() {
        let name = p.name_str();
        let group_name = if (p.group_index as usize) < metadata.groups.len() {
            metadata.groups[p.group_index as usize].name_str()
        } else {
            format!("<unknown:{}>", p.group_index)
        };

        let mut total_sectors = 0u64;
        let mut linear_extents = 0usize;
        let mut zero_extents = 0usize;
        for j in 0..p.num_extents {
            let ext = &metadata.extents[(p.first_extent_index + j) as usize];
            total_sectors += ext.num_sectors;
            if ext.target_type == LP_TARGET_TYPE_LINEAR {
                linear_extents += 1;
            } else {
                zero_extents += 1;
            }
        }
        let size_bytes = total_sectors * LP_SECTOR_SIZE;

        println!("  [{}] {}", i, name);
        println!("       Group:      {}", group_name);
        println!("       Attributes: 0x{:08x}", p.attributes);
        println!(
            "       Size:       {} bytes ({:.1} MB)",
            size_bytes,
            size_bytes as f64 / 1048576.0
        );
        println!(
            "       Extents:    {} (linear={}, zero={})",
            p.num_extents, linear_extents, zero_extents
        );

        // Show extent details
        for j in 0..p.num_extents {
            let ext = &metadata.extents[(p.first_extent_index + j) as usize];
            if ext.target_type == LP_TARGET_TYPE_LINEAR {
                let offset = ext.target_data * LP_SECTOR_SIZE;
                let len = ext.num_sectors * LP_SECTOR_SIZE;
                println!(
                    "         [{}] LINEAR  sector={} offset={} len={}",
                    j, ext.target_data, offset, len
                );
            } else {
                let len = ext.num_sectors * LP_SECTOR_SIZE;
                println!("         [{}] ZERO    len={}", j, len);
            }
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// `lpunpack` command
// ═══════════════════════════════════════════════════════════════════════════════

pub fn run_lpunpack(args: &LpunpackArgs) -> Result<()> {
    let metadata = read_metadata(&args.image, args.slot)
        .with_context(|| format!("read metadata from {}", args.image.display()))?;

    // Build filter set from --partitions if provided.
    let filter: Option<std::collections::HashSet<String>> = args.partitions.as_ref().map(|s| {
        s.split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect()
    });

    let out_dir = Path::new(&args.output);
    if !out_dir.exists() {
        fs::create_dir_all(out_dir)
            .with_context(|| format!("create output directory {}", out_dir.display()))?;
    }

    // Read the raw file data.
    let file_data =
        fs::read(&args.image).with_context(|| format!("read {}", args.image.display()))?;

    let mut extracted = Vec::new();
    let mut skipped = Vec::new();

    for p in metadata.partitions.iter() {
        let name = p.name_str();

        // Apply partition filter.
        if let Some(ref filt) = filter
            && !filt.contains(&name) {
                continue;
            }

        if p.num_extents == 0 {
            log::warn!("skipping '{}': no extents", name);
            skipped.push(name);
            continue;
        }

        // Gather all LINEAR extents and compute total data size.
        let mut regions: Vec<(u64, u64)> = Vec::new(); // (offset, length)
        let mut total_size: u64 = 0;

        for j in 0..p.num_extents {
            let ext = &metadata.extents[(p.first_extent_index + j) as usize];
            if ext.target_type == LP_TARGET_TYPE_LINEAR {
                let offset = ext.target_data * LP_SECTOR_SIZE;
                let len = ext.num_sectors * LP_SECTOR_SIZE;
                regions.push((offset, len));
                total_size += len;
            }
        }

        if regions.is_empty() || total_size == 0 {
            log::info!("skipping '{}': no linear data", name);
            skipped.push(name.clone());
            continue;
        }

        // Extract data from file into a buffer.
        let mut buf = Vec::with_capacity(total_size as usize);
        for &(offset, len) in &regions {
            let end = (offset + len) as usize;
            if end > file_data.len() {
                log::warn!(
                    "partition '{}': extent at {} + {} exceeds file size {}, truncating",
                    name,
                    offset,
                    len,
                    file_data.len()
                );
                let available = (file_data.len() as u64).saturating_sub(offset) as usize;
                buf.extend_from_slice(&file_data[offset as usize..offset as usize + available]);
                // Pad remainder with zeros.
                buf.resize(total_size as usize, 0);
                break;
            }
            buf.extend_from_slice(&file_data[offset as usize..end]);
        }

        // Write to output file.
        let out_path = out_dir.join(format!("{}.img", &name));
        fs::write(&out_path, &buf).with_context(|| format!("write {}", out_path.display()))?;

        log::info!(
            "extracted '{}': {} bytes -> {}",
            name,
            buf.len(),
            out_path.display()
        );
        extracted.push((name, buf.len()));
    }

    // Print summary.
    println!();
    println!("=== lpunpack summary ===");
    println!("  Source: {}", args.image.display());
    println!("  Output: {}", out_dir.display());
    println!("  Slot:   {}", args.slot);
    println!();
    println!("  Extracted ({}):", extracted.len());
    for (name, size) in &extracted {
        println!(
            "    {:<24} {:>12} bytes ({:.1} MB)",
            name,
            *size,
            *size as f64 / 1048576.0
        );
    }
    if !skipped.is_empty() {
        println!();
        println!("  Skipped ({}):", skipped.len());
        for name in &skipped {
            println!("    {}", name);
        }
    }

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════════
// Shared helpers
// ═══════════════════════════════════════════════════════════════════════════════

/// Parse LP version string (e.g., "10.0", "10.1", "10.2") into LpVersion.
fn parse_lp_version(ver: &str) -> LpVersion {
    let parts: Vec<&str> = ver.split('.').collect();
    if parts.len() == 2
        && let (Ok(major), Ok(minor)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
            match (major, minor) {
                (10, 0) => return LpVersion::V1_0,
                (10, 1) => return LpVersion::V1_1,
                (10, 2) => return LpVersion::V1_2,
                _ => {}
            }
        }
    log::warn!(
        "invalid LP version '{}', defaulting to v10.0 (safest for most devices)",
        ver
    );
    LpVersion::V1_0
}
