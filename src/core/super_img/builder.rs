//! Metadata builder — constructs LP metadata from a config, allocates extents,
//! computes all checksums. Version-aware (v1.0 / v1.1 / v1.2).

use anyhow::{ensure, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::lp_metadata::*;

// ---------------------------------------------------------------------------
// Builder-specific configuration structures
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PartitionInfo {
    pub name: String,
    pub group_name: String,
    pub attributes: u32,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub name: String,
    pub max_size: u64,
}

#[derive(Debug, Clone)]
pub struct SuperConfig {
    pub device_size: u64,
    pub metadata_max_size: u32,
    pub metadata_slots: u32,
    pub block_device_name: String,
    pub alignment: u32,
    pub alignment_offset: u32,
    pub logical_block_size: u32,
    pub groups: Vec<GroupInfo>,
    pub partitions: Vec<PartitionInfo>,
    pub version: LpVersion,
    pub header_flags: u32,
}

impl Default for SuperConfig {
    fn default() -> Self {
        Self {
            device_size: 0,
            metadata_max_size: 65536,
            metadata_slots: 2,
            block_device_name: "super".into(),
            alignment: LP_DEFAULT_ALIGNMENT,
            alignment_offset: 0,
            logical_block_size: 4096,
            groups: Vec::new(),
            partitions: Vec::new(),
            version: LpVersion::V1_0,
            header_flags: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Main builder functions
// ---------------------------------------------------------------------------

pub fn build_metadata(config: &SuperConfig) -> Result<LpMetadata> {
    ensure!(config.device_size > 0, "device_size must be > 0");
    ensure!(config.device_size % LP_SECTOR_SIZE == 0, "device_size must be multiple of 512");
    ensure!(config.metadata_max_size >= 512, "metadata_max_size >= 512");
    ensure!(config.metadata_slots >= 1, "metadata_slots >= 1");
    ensure!(config.alignment > 0 && config.alignment % LP_SECTOR_SIZE as u32 == 0,
        "alignment must be positive multiple of 512");

    // first_logical_sector
    let metadata_region = LP_PARTITION_RESERVED_BYTES
        + (LP_METADATA_GEOMETRY_SIZE as u64) * 2
        + (config.metadata_max_size as u64) * (config.metadata_slots as u64) * 2;

    let first_logical_sector = align_up(metadata_region, config.alignment as u64) / LP_SECTOR_SIZE;

    ensure!(first_logical_sector * LP_SECTOR_SIZE < config.device_size,
        "metadata region exceeds device_size");

    // Groups: index 0 = "default"
    let mut groups = Vec::new();
    groups.push(LpMetadataPartitionGroup::new("default", 0));
    let mut gmap: HashMap<String, u32> = HashMap::new();
    gmap.insert("default".into(), 0);

    for g in &config.groups {
        let idx = groups.len() as u32;
        groups.push(LpMetadataPartitionGroup::new(&g.name, g.max_size));
        gmap.insert(g.name.clone(), idx);
    }

    // Partitions + extents
    let mut partitions = Vec::new();
    let mut extents = Vec::new();
    let mut cur_sector = first_logical_sector;

    for p in &config.partitions {
        let gidx = gmap.get(&p.group_name).copied().ok_or_else(|| {
            anyhow::anyhow!("partition '{}': unknown group '{}'", p.name, p.group_name)
        })?;

        let mut part = LpMetadataPartition::new(&p.name, p.attributes, gidx);

        if p.size > 0 {
            cur_sector = align_up(cur_sector * LP_SECTOR_SIZE, config.alignment as u64) / LP_SECTOR_SIZE;
            let num_sectors = align_up(p.size, LP_SECTOR_SIZE) / LP_SECTOR_SIZE;
            let end_byte = (cur_sector + num_sectors) * LP_SECTOR_SIZE;
            ensure!(end_byte <= config.device_size,
                "partition '{}' (size={}) exceeds device_size {}", p.name, p.size, config.device_size);

            part.first_extent_index = extents.len() as u32;
            part.num_extents = 1;
            extents.push(LpMetadataExtent {
                num_sectors,
                target_type: LP_TARGET_TYPE_LINEAR,
                target_data: cur_sector,
                target_source: 0,
            });
            cur_sector += num_sectors;
        }
        partitions.push(part);
    }

    // Block device
    let mut bd_name = [0u8; LP_PARTITION_NAME_LEN];
    let nb = config.block_device_name.as_bytes();
    bd_name[..nb.len().min(35)].copy_from_slice(&nb[..nb.len().min(35)]);

    let block_devices = vec![LpMetadataBlockDevice {
        first_logical_sector,
        alignment: config.alignment,
        alignment_offset: config.alignment_offset,
        size: config.device_size,
        partition_name: bd_name,
        flags: 0,
    }];

    // Serialize tables
    let mut tables: Vec<u8> = Vec::new();
    let p_off: u32 = 0;
    for p in &partitions { tables.extend_from_slice(&p.to_bytes()[..]); }
    let e_off: u32 = tables.len() as u32;
    for e in &extents { tables.extend_from_slice(&e.to_bytes()[..]); }
    let g_off: u32 = tables.len() as u32;
    for g in &groups { tables.extend_from_slice(&g.to_bytes()[..]); }
    let bd_off: u32 = tables.len() as u32;
    for bd in &block_devices { tables.extend_from_slice(&bd.to_bytes()[..]); } 
    let tables_size: u32 = tables.len() as u32;

    let tables_checksum: [u8; 32] = Sha256::digest(&tables).into();

    let header_size = config.version.header_size();
    let minor_version = config.version.minor();

    let mut header = LpMetadataHeader {
        magic: LP_METADATA_HEADER_MAGIC,
        major_version: LP_METADATA_MAJOR_VERSION,
        minor_version,
        header_size,
        header_checksum: [0u8; 32],
        tables_size,
        tables_checksum,
        partitions: LpMetadataTableDescriptor {
            offset: p_off, num_entries: partitions.len() as u32,
            entry_size: LpMetadataPartition::SIZE as u32,
        },
        extents: LpMetadataTableDescriptor {
            offset: e_off, num_entries: extents.len() as u32,
            entry_size: LpMetadataExtent::SIZE as u32,
        },
        groups: LpMetadataTableDescriptor {
            offset: g_off, num_entries: groups.len() as u32,
            entry_size: LpMetadataPartitionGroup::SIZE as u32,
        },
        block_devices: LpMetadataTableDescriptor {
            offset: bd_off, num_entries: block_devices.len() as u32,
            entry_size: LpMetadataBlockDevice::SIZE as u32,
        },
        flags: config.header_flags,
    };

    // Header checksum: hash header_size bytes with checksum zeroed
    let mut hdr_bytes = header.to_bytes();
    hdr_bytes[12..44].fill(0); // zero checksum field
    let hdr_hash: [u8; 32] = Sha256::digest(&hdr_bytes[..header_size as usize]).into();
    header.header_checksum = hdr_hash;

    // Geometry
    let mut geometry = LpMetadataGeometry {
        magic: LP_METADATA_GEOMETRY_MAGIC,
        struct_size: LpMetadataGeometry::STRUCT_SIZE,
        checksum: [0u8; 32],
        metadata_max_size: config.metadata_max_size,
        metadata_slot_count: config.metadata_slots,
        logical_block_size: config.logical_block_size,
    };

    // FIX: Explicitly calculate and populate the geometry checksum
    let geo_bytes_temp = geometry.to_bytes();
    geometry.checksum.copy_from_slice(&geo_bytes_temp[8..40]);

    let total = header_size as usize + tables.len();
    ensure!(total <= config.metadata_max_size as usize,
        "metadata blob ({} bytes) exceeds metadata_max_size ({})", total, config.metadata_max_size);

    Ok(LpMetadata { geometry, header, partitions, extents, groups, block_devices })
}

/// Calculate minimum device_size for a given config.
pub fn auto_device_size(config: &SuperConfig) -> u64 {
    let metadata_region = LP_PARTITION_RESERVED_BYTES
        + (LP_METADATA_GEOMETRY_SIZE as u64) * 2
        + (config.metadata_max_size as u64) * (config.metadata_slots as u64) * 2;

    let al = config.alignment as u64;
    let mut data: u64 = 0;
    for p in &config.partitions {
        data = align_up(data, al);
        data += align_up(p.size, LP_SECTOR_SIZE);
    }
    align_up(align_up(metadata_region, al) + data, al)
}

fn align_up(v: u64, a: u64) -> u64 {
    if a == 0 { return v; }
    let r = v % a;
    if r == 0 { v } else { v + (a - r) }
}