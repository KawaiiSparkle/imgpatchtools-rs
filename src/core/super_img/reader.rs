//! Super image reader — parse LP metadata from an existing super.img.

use std::path::Path;

use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};

use super::lp_metadata::*;

pub fn read_metadata(path: &Path, slot: u32) -> Result<LpMetadata> {
    let data = std::fs::read(path)
        .with_context(|| format!("read {}", path.display()))?;

    let min = LP_PARTITION_RESERVED_BYTES as usize + LP_METADATA_GEOMETRY_SIZE as usize * 2;
    ensure!(data.len() >= min, "file too small for super image");

    let geo = parse_geometry(&data[LP_PARTITION_RESERVED_BYTES as usize..])?;

    let md_start = LP_PARTITION_RESERVED_BYTES as usize + LP_METADATA_GEOMETRY_SIZE as usize * 2;
    let slot_off = md_start + (slot as usize) * (geo.metadata_max_size as usize);
    ensure!(slot_off + geo.metadata_max_size as usize <= data.len(), "slot {} beyond file", slot);

    parse_metadata_blob(&data[slot_off..slot_off + geo.metadata_max_size as usize], geo)
}

fn parse_geometry(buf: &[u8]) -> Result<LpMetadataGeometry> {
    let magic = r32(buf, 0);
    ensure!(magic == LP_METADATA_GEOMETRY_MAGIC,
        "bad geometry magic: 0x{:08x}", magic);

    let struct_size = r32(buf, 4);
    let stored: [u8; 32] = buf[8..40].try_into().unwrap();
    let mut check = buf[..struct_size as usize].to_vec();
    check[8..40].fill(0);
    let computed: [u8; 32] = Sha256::digest(&check).into();
    ensure!(stored == computed, "geometry checksum mismatch");

    Ok(LpMetadataGeometry {
        magic,
        struct_size,
        checksum: stored,
        metadata_max_size: r32(buf, 40),
        metadata_slot_count: r32(buf, 44),
        logical_block_size: r32(buf, 48),
    })
}

fn parse_metadata_blob(blob: &[u8], geometry: LpMetadataGeometry) -> Result<LpMetadata> {
    ensure!(r32(blob, 0) == LP_METADATA_HEADER_MAGIC, "bad header magic");
    let major = r16(blob, 4);
    let minor = r16(blob, 6);
    ensure!(major == LP_METADATA_MAJOR_VERSION, "unsupported major version {}", major);

    let header_size = r32(blob, 8) as usize;
    let tables_size = r32(blob, 44) as usize;

    let p_desc = parse_td(blob, 80);
    let e_desc = parse_td(blob, 92);
    let g_desc = parse_td(blob, 104);
    let bd_desc = parse_td(blob, 116);

    // v1.0/v1.1 have header_size=128, no flags. v1.2 has header_size=256, flags at offset 128.
    let flags = if header_size > 128 { r32(blob, 128) } else { 0 };

    let t = &blob[header_size..header_size + tables_size];

    let partitions = (0..p_desc.num_entries)
        .map(|i| {
            let o = p_desc.offset as usize + i as usize * p_desc.entry_size as usize;
            parse_part(&t[o..])
        }).collect();
    let extents = (0..e_desc.num_entries)
        .map(|i| {
            let o = e_desc.offset as usize + i as usize * e_desc.entry_size as usize;
            parse_ext(&t[o..])
        }).collect();
    let groups = (0..g_desc.num_entries)
        .map(|i| {
            let o = g_desc.offset as usize + i as usize * g_desc.entry_size as usize;
            parse_grp(&t[o..])
        }).collect();
    let block_devices = (0..bd_desc.num_entries)
        .map(|i| {
            let o = bd_desc.offset as usize + i as usize * bd_desc.entry_size as usize;
            parse_bd(&t[o..])
        }).collect();

    let header = LpMetadataHeader {
        magic: LP_METADATA_HEADER_MAGIC, major_version: major, minor_version: minor,
        header_size: header_size as u32,
        header_checksum: blob[12..44].try_into().unwrap(),
        tables_size: tables_size as u32,
        tables_checksum: blob[48..80].try_into().unwrap(),
        partitions: p_desc, extents: e_desc, groups: g_desc, block_devices: bd_desc,
        flags,
    };

    Ok(LpMetadata { geometry, header, partitions, extents, groups, block_devices })
}

fn parse_td(b: &[u8], o: usize) -> LpMetadataTableDescriptor {
    LpMetadataTableDescriptor { offset: r32(b, o), num_entries: r32(b, o+4), entry_size: r32(b, o+8) }
}
fn parse_part(b: &[u8]) -> LpMetadataPartition {
    let mut name = [0u8; LP_PARTITION_NAME_LEN];
    name.copy_from_slice(&b[..36]);
    LpMetadataPartition { name, attributes: r32(b,36), first_extent_index: r32(b,40),
        num_extents: r32(b,44), group_index: r32(b,48) }
}
fn parse_ext(b: &[u8]) -> LpMetadataExtent {
    LpMetadataExtent { num_sectors: r64(b,0), target_type: r32(b,8), target_data: r64(b,12),
        target_source: r32(b,20) }
}
fn parse_grp(b: &[u8]) -> LpMetadataPartitionGroup {
    let mut name = [0u8; LP_PARTITION_NAME_LEN];
    name.copy_from_slice(&b[..36]);
    LpMetadataPartitionGroup { name, flags: r32(b,36), maximum_size: r64(b,40) }
}
fn parse_bd(b: &[u8]) -> LpMetadataBlockDevice {
    let mut name = [0u8; LP_PARTITION_NAME_LEN];
    name.copy_from_slice(&b[24..60]);
    LpMetadataBlockDevice {
        first_logical_sector: r64(b,0), alignment: r32(b,8), alignment_offset: r32(b,12),
        size: r64(b,16), partition_name: name, flags: r32(b,60),
    }
}