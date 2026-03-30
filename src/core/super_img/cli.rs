//! CLI subcommands: lpmake, lpdump, lpunpack.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Args;

use super::builder::*;
use super::lp_metadata::*;
use super::reader;
use super::writer::{self, SuperImageFormat};

// ---------------------------------------------------------------------------
// lpmake
// ---------------------------------------------------------------------------

#[derive(Args, Debug, Clone)]
pub struct LpmakeArgs {
    /// Auto-scan directory for .img files and dynamic_partitions_op_list to build super.img.
    #[arg(long)]
    pub auto: Option<String>,

    /// Output format: "sparse" (default, highly recommended) or "raw".
    #[arg(long, default_value = "sparse")]
    pub format: String,

    /// Total super partition size (0 = auto-calculate).
    #[arg(short = 'd', long, default_value = "0")]
    pub device_size: u64,

    /// Maximum metadata size.
    #[arg(short = 'm', long, default_value = "65536")]
    pub metadata_size: u32,

    /// Number of metadata slots (1 = non-A/B, 2 = A/B).
    #[arg(short = 's', long, default_value = "2")]
    pub metadata_slots: u32,

    /// Output file.
    #[arg(short = 'o', long, default_value = "super.img")]
    pub output: String,

    /// Partition: "name:group:attrs:size" (attrs: none/readonly).
    #[arg(short = 'p', long = "partition")]
    pub partitions: Vec<String>,

    /// Group: "name:max_size".
    #[arg(short = 'g', long = "group")]
    pub groups: Vec<String>,

    /// Image: "name=path".
    #[arg(short = 'i', long = "image")]
    pub images: Vec<String>,

    /// Android version for LP format (10, 11, 12).
    #[arg(short = 'a', long, default_value = "12")]
    pub android_version: String,

    /// Alignment in bytes.
    #[arg(long, default_value = "1048576")]
    pub alignment: u32,

    /// Block device name.
    #[arg(long, default_value = "super")]
    pub block_device: String,

    /// Set Virtual A/B flag (v1.2 only).
    #[arg(long)]
    pub virtual_ab: bool,
}

pub fn run_lpmake(args: &LpmakeArgs) -> Result<()> {
    let version = LpVersion::from_android_version(&args.android_version)
        .ok_or_else(|| anyhow::anyhow!("unknown android version '{}', use 10/11/12", args.android_version))?;

    let format = match args.format.to_lowercase().as_str() {
        "raw" | "2" => SuperImageFormat::Raw,
        _ => SuperImageFormat::Sparse,
    };

    let mut groups = Vec::new();
    let mut partitions = Vec::new();
    let mut images: Vec<(String, String)> = Vec::new();

    // 智能 Auto 模式：自动扫描与装配
    if let Some(ref workdir) = args.auto {
        log::info!("lpmake: Auto-scanning workspace '{}'...", workdir);
        let op_list_path = Path::new(workdir).join("dynamic_partitions_op_list");
        
        if op_list_path.exists() {
            log::info!("lpmake: Found dynamic_partitions_op_list, parsing...");
            let content = std::fs::read_to_string(&op_list_path)?;
            let state = super::op_list::parse_op_list(&content)?;
            
            for g in state.groups {
                groups.push(GroupInfo { name: g.name, max_size: g.max_size });
            }
            for p in state.partitions {
                let img_path = Path::new(workdir).join(format!("{}.img", p.name));
                let mut actual_size = p.size;
                
                if img_path.exists() {
                    let flen = std::fs::metadata(&img_path)?.len();
                    actual_size = actual_size.max(flen);
                    images.push((p.name.clone(), img_path.to_string_lossy().into()));
                } else if actual_size > 0 {
                    log::warn!("partition '{}': image not found, will be zero-filled", p.name);
                }
                
                partitions.push(PartitionInfo {
                    name: p.name,
                    group_name: p.group_name,
                    attributes: LP_PARTITION_ATTR_READONLY,
                    size: actual_size,
                });
            }
        } else {
            // 极致兜底模式：连 op_list 都没有，直接扫描所有的 .img 文件！
            log::warn!("lpmake: No op_list found! Falling back to raw .img file scan...");
            groups.push(GroupInfo { name: "default".into(), max_size: 10_737_418_240 }); // 默认给 10GB Group
            
            // 排除掉不属于动态分区范畴的基础镜像
            let exclude = ["boot.img", "recovery.img", "super.img", "userdata.img", "vbmeta.img", "dtbo.img", "persist.img"];
            
            for entry in std::fs::read_dir(workdir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("img") {
                    let filename = path.file_name().unwrap().to_string_lossy().to_string();
                    if exclude.contains(&filename.as_str()) { continue; }
                    
                    let name = path.file_stem().unwrap().to_string_lossy().to_string();
                    let size = std::fs::metadata(&path)?.len();
                    
                    log::info!("Detected partition '{}' ({} bytes)", name, size);
                    partitions.push(PartitionInfo {
                        name: name.clone(),
                        group_name: "default".into(),
                        attributes: LP_PARTITION_ATTR_READONLY,
                        size,
                    });
                    images.push((name, path.to_string_lossy().into()));
                }
            }
        }
    } else {
        // 专家手动模式
        for g in &args.groups {
            let parts: Vec<&str> = g.splitn(2, ':').collect();
            if parts.len() < 2 { bail!("bad group '{}'", g); }
            groups.push(GroupInfo {
                name: parts[0].into(),
                max_size: parts[1].parse().context("bad max_size")?,
            });
        }

        for p in &args.partitions {
            let parts: Vec<&str> = p.splitn(4, ':').collect();
            if parts.len() < 4 { bail!("bad partition '{}': need name:group:attrs:size", p); }
            let attrs = match parts[2] {
                "none" => LP_PARTITION_ATTR_NONE,
                "readonly" => LP_PARTITION_ATTR_READONLY,
                v => v.parse().unwrap_or(0),
            };
            partitions.push(PartitionInfo {
                name: parts[0].into(),
                group_name: parts[1].into(),
                attributes: attrs,
                size: parts[3].parse().context("bad size")?,
            });
        }

        for i in &args.images {
            let parts: Vec<&str> = i.splitn(2, '=').collect();
            if parts.len() < 2 { bail!("bad image '{}': need name=path", i); }
            if let Some(p) = partitions.iter_mut().find(|p| p.name == parts[0]) {
                if p.size == 0 {
                    let meta = std::fs::metadata(parts[1])?;
                    p.size = meta.len();
                    log::info!("auto-sized '{}' to {} bytes", p.name, p.size);
                }
            }
            images.push((parts[0].into(), parts[1].into()));
        }
    }

    if partitions.is_empty() {
        bail!("No partitions to pack! Did you specify the correct directory or arguments?");
    }

    let mut config = SuperConfig {
        metadata_max_size: args.metadata_size,
        metadata_slots: args.metadata_slots,
        block_device_name: args.block_device.clone(),
        alignment: args.alignment,
        alignment_offset: 0,
        logical_block_size: 4096,
        groups,
        partitions,
        version,
        header_flags: if args.virtual_ab { LP_HEADER_FLAG_VIRTUAL_AB_DEVICE } else { 0 },
        device_size: 0, 
    };

    config.device_size = if args.device_size > 0 {
        args.device_size
    } else {
        let sz = auto_device_size(&config);
        log::info!("auto device_size: {} bytes ({:.1} GB)", sz, sz as f64 / 1e9);
        sz
    };

    let metadata = build_metadata(&config)?;
    writer::write_super(Path::new(&args.output), &metadata, &images, format)?;

    println!("Created {} — {} bytes, LP {}", args.output, config.device_size, version.label());
    Ok(())
}

// ---------------------------------------------------------------------------
// lpdump
// ---------------------------------------------------------------------------

#[derive(Args, Debug, Clone)]
pub struct LpdumpArgs {
    /// Path to super.img.
    pub image: PathBuf,

    /// Metadata slot (default 0).
    #[arg(short = 's', long, default_value = "0")]
    pub slot: u32,
}

pub fn run_lpdump(args: &LpdumpArgs) -> Result<()> {
    let md = reader::read_metadata(&args.image, args.slot)?;

    println!("LP Metadata version: {}.{}", md.header.major_version, md.header.minor_version);
    println!("Header size: {} bytes", md.header.header_size);
    if md.header.header_size >= 132 {
        println!("Header flags: 0x{:x}", md.header.flags);
    }
    println!("Metadata max size: {}", md.geometry.metadata_max_size);
    println!("Metadata slot count: {}", md.geometry.metadata_slot_count);
    println!("Logical block size: {}", md.geometry.logical_block_size);
    println!();

    println!("Block devices:");
    for bd in &md.block_devices {
        println!("  {}: size={}, first_logical_sector={}, alignment={}",
            read_name(&bd.partition_name), bd.size, bd.first_logical_sector, bd.alignment);
    }
    println!();

    println!("Groups:");
    for (i, g) in md.groups.iter().enumerate() {
        println!("  [{}] {}: max_size={}", i, g.name_str(), g.maximum_size);
    }
    println!();

    println!("Partitions:");
    for p in &md.partitions {
        let mut total_sec = 0u64;
        for i in 0..p.num_extents {
            total_sec += md.extents[(p.first_extent_index + i) as usize].num_sectors;
        }
        let gname = md.groups.get(p.group_index as usize)
            .map_or("?".into(), |g| g.name_str().to_string());
        println!("  {}: size={} ({:.1} MB), group={}, attrs=0x{:x}",
            p.name_str(), total_sec * LP_SECTOR_SIZE,
            (total_sec * LP_SECTOR_SIZE) as f64 / 1048576.0, gname, p.attributes);
        for i in 0..p.num_extents {
            let e = &md.extents[(p.first_extent_index + i) as usize];
            let tstr = if e.target_type == 0 { "linear" } else { "zero" };
            println!("    [{i}] {} sectors @ sector {} ({})", e.num_sectors, e.target_data, tstr);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// lpunpack
// ---------------------------------------------------------------------------

#[derive(Args, Debug, Clone)]
pub struct LpunpackArgs {
    /// Path to super.img.
    pub image: PathBuf,

    /// Output directory.
    #[arg(short = 'o', long, default_value = ".")]
    pub output_dir: String,

    /// Extract only this partition.
    #[arg(short = 'p', long)]
    pub partition: Option<String>,

    /// Metadata slot.
    #[arg(short = 's', long, default_value = "0")]
    pub slot: u32,
}

pub fn run_lpunpack(args: &LpunpackArgs) -> Result<()> {
    let super_data = std::fs::read(&args.image)
        .with_context(|| format!("read {}", args.image.display()))?;
    let md = reader::read_metadata(&args.image, args.slot)?;

    let out = Path::new(&args.output_dir);
    if !out.exists() { std::fs::create_dir_all(out)?; }

    for p in &md.partitions {
        let name = p.name_str();
        if let Some(ref f) = args.partition {
            if name != f { continue; }
        }
        if p.num_extents == 0 { continue; }

        let mut buf = Vec::new();
        for i in 0..p.num_extents {
            let e = &md.extents[(p.first_extent_index + i) as usize];
            match e.target_type {
                LP_TARGET_TYPE_LINEAR => {
                    let start = (e.target_data * LP_SECTOR_SIZE) as usize;
                    let len = (e.num_sectors * LP_SECTOR_SIZE) as usize;
                    if start + len > super_data.len() {
                        bail!("extent beyond file for '{}'", name);
                    }
                    buf.extend_from_slice(&super_data[start..start + len]);
                }
                LP_TARGET_TYPE_ZERO => {
                    buf.resize(buf.len() + (e.num_sectors * LP_SECTOR_SIZE) as usize, 0);
                }
                _ => bail!("unknown extent type {} for '{}'", e.target_type, name),
            }
        }

        let out_path = out.join(format!("{name}.img"));
        std::fs::write(&out_path, &buf)?;
        println!("extracted '{}': {} bytes → {}", name, buf.len(), out_path.display());
    }
    Ok(())
}