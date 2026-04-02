//! LP metadata structures — low-level binary format definitions.
//!
//! These structures match the on-disk format defined in AOSP liblp.

use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Constants (from AOSP liblp.h)
// ---------------------------------------------------------------------------

pub const LP_SECTOR_SIZE: u64 = 512;
pub const LP_PARTITION_RESERVED_BYTES: u64 = 4096;
pub const LP_METADATA_GEOMETRY_SIZE: u32 = 4096;

// Alignment for partitions (1 MiB)
pub const LP_DEFAULT_ALIGNMENT: u32 = 0x100000;  // 1 MiB

// Partition attributes
pub const LP_PARTITION_ATTR_NONE: u32 = 0x0;
pub const LP_PARTITION_ATTR_READONLY: u32 = 0x1;

// Magic values
pub const LP_METADATA_GEOMETRY_MAGIC: u32 = 0x616C4467;
pub const LP_METADATA_HEADER_MAGIC: u32 = 0x414C5030;  // on-disk LE bytes: "0PLA"

// Version constants
pub const LP_METADATA_MAJOR_VERSION: u16 = 10;

// Extent target types
pub const LP_TARGET_TYPE_LINEAR: u32 = 0;
pub const LP_TARGET_TYPE_ZERO: u32 = 1;

// Header flags (v1.2+)
pub const LP_HEADER_FLAG_VIRTUAL_AB_DEVICE: u32 = 0x1;

// Partition name length
pub const LP_PARTITION_NAME_LEN: usize = 36;

// ---------------------------------------------------------------------------
// Version enumeration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LpVersion {
    V1_0,  // Android 10
    V1_1,  // Android 11 (adds ATTR_UPDATED)
    V1_2,  // Android 12+ (adds header flags)
}

impl LpVersion {
    pub fn from_android_version(ver: &str) -> Option<Self> {
        match ver {
            "10" => Some(Self::V1_0),
            "11" => Some(Self::V1_1),
            "12" | "13" | "14" | "15" => Some(Self::V1_2),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::V1_0 => "v10.0",
            Self::V1_1 => "v10.1",
            Self::V1_2 => "v10.2",
        }
    }

    pub fn minor(&self) -> u16 {
        match self {
            Self::V1_0 => 0,
            Self::V1_1 => 1,
            Self::V1_2 => 2,
        }
    }

    pub fn header_size(&self) -> u32 {
        match self {
            // v1.0/v1.1: 4+2+2+4+32+4+32+12*4 = 128 bytes
            Self::V1_0 | Self::V1_1 => 128,
            // v1.2: 128 + 4(flags) + 124(reserved) = 256 bytes
            Self::V1_2 => 256,
        }
    }
}

// ---------------------------------------------------------------------------
// Table descriptor
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub struct LpMetadataTableDescriptor {
    pub offset: u32,
    pub num_entries: u32,
    pub entry_size: u32,
}

impl LpMetadataTableDescriptor {
    pub const SIZE: usize = 12;
    
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..4].copy_from_slice(&self.offset.to_le_bytes());
        buf[4..8].copy_from_slice(&self.num_entries.to_le_bytes());
        buf[8..12].copy_from_slice(&self.entry_size.to_le_bytes());
        buf
    }
}

// ---------------------------------------------------------------------------
// Geometry block
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadataGeometry {
    pub magic: u32,
    pub struct_size: u32,
    pub checksum: [u8; 32],
    pub metadata_max_size: u32,
    pub metadata_slot_count: u32,
    pub logical_block_size: u32,
}

impl LpMetadataGeometry {
    /// On-disk block size (geometry is stored in a full 4096-byte block).
    pub const BLOCK_SIZE: u32 = 4096;
    /// sizeof(LpMetadataGeometry) with __attribute__((packed)).
    /// 4+4+32+4+4+4 = 52. AOSP validates this field == 52.
    pub const STRUCT_SIZE: u32 = 52;

    /// Serialize into a full 4096-byte block (zero-padded).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; Self::BLOCK_SIZE as usize];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..8].copy_from_slice(&self.struct_size.to_le_bytes());
        buf[8..40].copy_from_slice(&self.checksum);
        buf[40..44].copy_from_slice(&self.metadata_max_size.to_le_bytes());
        buf[44..48].copy_from_slice(&self.metadata_slot_count.to_le_bytes());
        buf[48..52].copy_from_slice(&self.logical_block_size.to_le_bytes());
        buf
    }
    
    /// Serialize and compute SHA256 checksum.
    /// AOSP: "SHA256 of this struct, with this field set to 0" — hashes only
    /// the first struct_size (52) bytes, NOT the full 4096-byte block.
    pub fn to_block(&self) -> Vec<u8> {
        let mut buf = self.to_bytes();
        // Zero the checksum field, then hash only the packed struct portion.
        buf[8..40].fill(0);
        let hash: [u8; 32] = Sha256::digest(&buf[..Self::STRUCT_SIZE as usize]).into();
        buf[8..40].copy_from_slice(&hash);
        buf
    }
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadataHeader {
    pub magic: u32,
    pub major_version: u16,
    pub minor_version: u16,
    pub header_size: u32,
    pub header_checksum: [u8; 32],
    pub tables_size: u32,
    pub tables_checksum: [u8; 32],
    pub partitions: LpMetadataTableDescriptor,
    pub extents: LpMetadataTableDescriptor,
    pub groups: LpMetadataTableDescriptor,
    pub block_devices: LpMetadataTableDescriptor,
    pub flags: u32,
}

impl LpMetadataHeader {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.header_size as usize];
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4..6].copy_from_slice(&self.major_version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.minor_version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.header_size.to_le_bytes());
        buf[12..44].copy_from_slice(&self.header_checksum);
        buf[44..48].copy_from_slice(&self.tables_size.to_le_bytes());
        buf[48..80].copy_from_slice(&self.tables_checksum);
        buf[80..92].copy_from_slice(&self.partitions.to_bytes());
        buf[92..104].copy_from_slice(&self.extents.to_bytes());
        buf[104..116].copy_from_slice(&self.groups.to_bytes());
        buf[116..128].copy_from_slice(&self.block_devices.to_bytes());
        
        // flags at offset 128, only for v1.2+ (header_size > 128)
        if self.header_size > 128 {
            buf[128..132].copy_from_slice(&self.flags.to_le_bytes());
            // bytes 132..256 are reserved and left as zeros
        }
        
        buf
    }
}

// ---------------------------------------------------------------------------
// Partition
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadataPartition {
    pub name: [u8; LP_PARTITION_NAME_LEN],
    pub attributes: u32,
    pub first_extent_index: u32,
    pub num_extents: u32,
    pub group_index: u32,
}

impl LpMetadataPartition {
    pub const SIZE: usize = 52;
    
    pub fn new(name: &str, attributes: u32, group_index: u32) -> Self {
        let mut name_bytes = [0u8; LP_PARTITION_NAME_LEN];
        let bytes = name.as_bytes();
        name_bytes[..bytes.len().min(LP_PARTITION_NAME_LEN)].copy_from_slice(
            &bytes[..bytes.len().min(LP_PARTITION_NAME_LEN)]
        );
        
        Self {
            name: name_bytes,
            attributes,
            first_extent_index: 0,
            num_extents: 0,
            group_index,
        }
    }
    
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[..LP_PARTITION_NAME_LEN].copy_from_slice(&self.name);
        buf[36..40].copy_from_slice(&self.attributes.to_le_bytes());
        buf[40..44].copy_from_slice(&self.first_extent_index.to_le_bytes());
        buf[44..48].copy_from_slice(&self.num_extents.to_le_bytes());
        buf[48..52].copy_from_slice(&self.group_index.to_le_bytes());
        buf
    }
    
    pub fn name_str(&self) -> String {
        let null_pos = self.name.iter().position(|&b| b == 0).unwrap_or(LP_PARTITION_NAME_LEN);
        String::from_utf8_lossy(&self.name[..null_pos]).to_string()
    }
}

// ---------------------------------------------------------------------------
// Extent
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadataExtent {
    pub num_sectors: u64,
    pub target_type: u32,
    pub target_data: u64,
    pub target_source: u32,
}

impl LpMetadataExtent {
    pub const SIZE: usize = 24;
    
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.num_sectors.to_le_bytes());
        buf[8..12].copy_from_slice(&self.target_type.to_le_bytes());
        buf[12..20].copy_from_slice(&self.target_data.to_le_bytes());
        buf[20..24].copy_from_slice(&self.target_source.to_le_bytes());
        buf
    }
}

// ---------------------------------------------------------------------------
// Partition Group
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadataPartitionGroup {
    pub name: [u8; LP_PARTITION_NAME_LEN],
    pub flags: u32,
    pub maximum_size: u64,
}

impl LpMetadataPartitionGroup {
    pub const SIZE: usize = 48;
    
    pub fn new(name: &str, maximum_size: u64) -> Self {
        let mut name_bytes = [0u8; LP_PARTITION_NAME_LEN];
        let bytes = name.as_bytes();
        name_bytes[..bytes.len().min(LP_PARTITION_NAME_LEN)].copy_from_slice(
            &bytes[..bytes.len().min(LP_PARTITION_NAME_LEN)]
        );
        
        Self {
            name: name_bytes,
            flags: 0,
            maximum_size,
        }
    }
    
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[..LP_PARTITION_NAME_LEN].copy_from_slice(&self.name);
        buf[36..40].copy_from_slice(&self.flags.to_le_bytes());
        buf[40..48].copy_from_slice(&self.maximum_size.to_le_bytes());
        buf
    }
    
    pub fn name_str(&self) -> String {
        let null_pos = self.name.iter().position(|&b| b == 0).unwrap_or(LP_PARTITION_NAME_LEN);
        String::from_utf8_lossy(&self.name[..null_pos]).to_string()
    }
}

// ---------------------------------------------------------------------------
// Block Device
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadataBlockDevice {
    pub first_logical_sector: u64,
    pub alignment: u32,
    pub alignment_offset: u32,
    pub size: u64,
    pub partition_name: [u8; LP_PARTITION_NAME_LEN],
    pub flags: u32,
}

impl LpMetadataBlockDevice {
    pub const SIZE: usize = 64;
    
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut buf = [0u8; Self::SIZE];
        buf[0..8].copy_from_slice(&self.first_logical_sector.to_le_bytes());
        buf[8..12].copy_from_slice(&self.alignment.to_le_bytes());
        buf[12..16].copy_from_slice(&self.alignment_offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.size.to_le_bytes());
        buf[24..60].copy_from_slice(&self.partition_name);
        buf[60..64].copy_from_slice(&self.flags.to_le_bytes());
        buf
    }
}

// ---------------------------------------------------------------------------
// Combined Metadata
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct LpMetadata {
    pub geometry: LpMetadataGeometry,
    pub header: LpMetadataHeader,
    pub partitions: Vec<LpMetadataPartition>,
    pub extents: Vec<LpMetadataExtent>,
    pub groups: Vec<LpMetadataPartitionGroup>,
    pub block_devices: Vec<LpMetadataBlockDevice>,
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

pub fn read_name(bytes: &[u8; LP_PARTITION_NAME_LEN]) -> String {
    let null_pos = bytes.iter().position(|&b| b == 0).unwrap_or(LP_PARTITION_NAME_LEN);
    String::from_utf8_lossy(&bytes[..null_pos]).to_string()
}

// Helper functions for reading binary data
pub fn r16(buf: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([buf[off], buf[off + 1]])
}

pub fn r32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

pub fn r64(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes([
        buf[off], buf[off + 1], buf[off + 2], buf[off + 3],
        buf[off + 4], buf[off + 5], buf[off + 6], buf[off + 7],
    ])
}