//! Super image LP version detector — simplified for manual metadata extraction.
//!
//! NOTE: This tool only supports RAW format super images.
//! Users must manually extract metadata from their device's super partition using lpdump first.
//!
//! Detection priority:
//! 1. User-specified version (via CLI args) - RECOMMENDED
//! 2. Existing super.img metadata (via lpdump)
//! 3. Default: v10.0 + raw (safest fallback)

use crate::core::super_img::lp_metadata::{
    LP_METADATA_GEOMETRY_MAGIC, LP_METADATA_HEADER_MAGIC, LP_METADATA_MAJOR_VERSION,
    LpMetadataGeometry,
};
use anyhow::{Context, Result, bail};
use std::path::Path;

/// Detected LP version information
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LpVersionInfo {
    pub major: u16,
    pub minor: u16,
}

impl LpVersionInfo {
    pub fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Check if version supports Virtual A/B (snapshot)
    pub fn supports_virtual_ab(&self) -> bool {
        self.major >= 10 && self.minor >= 2
    }

    /// Check if version supports UPDATED partition attribute
    pub fn supports_updated_attr(&self) -> bool {
        self.major >= 10 && self.minor >= 1
    }

    /// Get Android version approximation
    pub fn android_version(&self) -> &'static str {
        match (self.major, self.minor) {
            (10, 0) => "10 (Q)",
            (10, 1) => "11 (R)",
            (10, 2) => "12+ (S/T/U)",
            _ => "Unknown",
        }
    }
}

impl std::fmt::Display for LpVersionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "v{}.{} ({})",
            self.major,
            self.minor,
            self.android_version()
        )
    }
}

/// Complete detection result for super image creation
#[derive(Debug, Clone)]
pub struct SuperImageProfile {
    /// LP metadata version
    pub lp_version: LpVersionInfo,
    /// Detected geometry (if available)
    pub geometry: Option<LpMetadataGeometry>,
    /// Source of detection
    pub detection_source: DetectionSource,
}

/// Where the detection info came from
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectionSource {
    UpdateBinary,
    ExistingSuper,
    FotaPayload,
    OpList,
    Default,
}

/// Detect LP version and recommended format from update-binary
///
/// Detect LP version from update-binary (ELF analysis)
///
/// NOTE: This is best-effort detection. For reliable results,
/// use --lp-version flag to manually specify version.
pub fn detect_from_update_binary(binary_path: &Path) -> Result<Option<SuperImageProfile>> {
    let data = std::fs::read(binary_path)
        .with_context(|| format!("Failed to read {}", binary_path.display()))?;

    // Search for LP metadata header magic pattern
    let header_magic = LP_METADATA_HEADER_MAGIC.to_le_bytes();
    let mut detected_version: Option<LpVersionInfo> = None;

    // Search for magic followed by version bytes
    for (idx, window) in data.windows(8).enumerate() {
        if window[0..4] == header_magic
            && idx + 8 <= data.len() {
                let major = u16::from_le_bytes([data[idx + 4], data[idx + 5]]);
                let minor = u16::from_le_bytes([data[idx + 6], data[idx + 7]]);

                if major == LP_METADATA_MAJOR_VERSION && minor <= 2 {
                    detected_version = Some(LpVersionInfo::new(major, minor));
                    break;
                }
            }

        // Alternative: Look for version constants
        if window[0..2] == [0x0A, 0x00] {
            let minor = u16::from_le_bytes([window[2], window[3]]);
            if minor <= 2 {
                detected_version = Some(LpVersionInfo::new(10, minor));
                break;
            }
        }
    }

    // Fallback: string-based detection
    if detected_version.is_none() {
        detected_version = detect_version_from_strings(&data);
    }

    let version = match detected_version {
        Some(v) => v,
        None => return Ok(None),
    };

    Ok(Some(SuperImageProfile {
        lp_version: version,
        geometry: None,
        detection_source: DetectionSource::UpdateBinary,
    }))
}

/// Detect from existing super.img file (RAW format only)
///
/// NOTE: Sparse images are NOT supported. Users must convert sparse to raw first:
///   simg2img sparse_super.img raw_super.img
pub fn detect_from_super_image(image_path: &Path) -> Result<Option<SuperImageProfile>> {
    let mut file = std::fs::File::open(image_path)
        .with_context(|| format!("Failed to open {}", image_path.display()))?;

    // Check first 4 bytes - reject sparse images
    let mut magic_buf = [0u8; 4];
    std::io::Read::read_exact(&mut file, &mut magic_buf)?;

    // Sparse magic = 0xED26FF3A (little endian: 0x3AFF26ED)
    const SPARSE_MAGIC_LE: [u8; 4] = [0x3A, 0xFF, 0x26, 0xED];
    if magic_buf == SPARSE_MAGIC_LE {
        bail!(
            "Sparse super images are not supported. \
             Please convert to raw first: simg2img {} raw_super.img",
            image_path.display()
        );
    }

    // For raw images, geometry is at offset 4096
    const GEOMETRY_OFFSET: u64 = 4096;
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(GEOMETRY_OFFSET))?;

    let mut geometry_buf = [0u8; 256];
    std::io::Read::read(&mut file, &mut geometry_buf)?;

    // Parse geometry
    let geometry = parse_geometry(&geometry_buf)?;

    // Read metadata header for version info
    let metadata_offset = GEOMETRY_OFFSET + 8192; // After primary + backup geometry
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(metadata_offset))?;
    let mut metadata_buf = [0u8; 256];
    std::io::Read::read(&mut file, &mut metadata_buf)?;

    let version = if metadata_buf[0..4] == LP_METADATA_HEADER_MAGIC.to_le_bytes() {
        let major = u16::from_le_bytes([metadata_buf[4], metadata_buf[5]]);
        let minor = u16::from_le_bytes([metadata_buf[6], metadata_buf[7]]);
        LpVersionInfo::new(major, minor)
    } else {
        // Fallback to default
        LpVersionInfo::new(10, 0)
    };

    Ok(Some(SuperImageProfile {
        lp_version: version,
        geometry: Some(geometry),
        detection_source: DetectionSource::ExistingSuper,
    }))
}

/// Detect version from debug strings in binary
fn detect_version_from_strings(data: &[u8]) -> Option<LpVersionInfo> {
    // Look for version strings like "10.0", "10.1", "10.2"
    for window in data.windows(4) {
        if window == b"10.0" {
            return Some(LpVersionInfo::new(10, 0));
        }
        if window == b"10.1" {
            return Some(LpVersionInfo::new(10, 1));
        }
        if window == b"10.2" {
            return Some(LpVersionInfo::new(10, 2));
        }
    }
    None
}

/// Parse geometry from bytes
fn parse_geometry(data: &[u8]) -> Result<LpMetadataGeometry> {
    if data.len() < 48 {
        bail!("Geometry data too short");
    }

    let magic = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if magic != LP_METADATA_GEOMETRY_MAGIC {
        bail!("Invalid geometry magic: {:08x}", magic);
    }

    // Basic geometry parsing
    let struct_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    let metadata_max_size = u32::from_le_bytes([data[40], data[41], data[42], data[43]]);
    let metadata_slot_count = u32::from_le_bytes([data[44], data[45], data[46], data[47]]);

    Ok(LpMetadataGeometry {
        magic,
        struct_size,
        checksum: [0; 32], // We don't validate checksum here
        metadata_max_size,
        metadata_slot_count,
        logical_block_size: 4096, // Default
    })
}

/// Smart detection combining multiple sources
pub fn detect_super_profile(
    update_binary: Option<&Path>,
    existing_super: Option<&Path>,
    op_list: Option<&Path>,
) -> Result<SuperImageProfile> {
    // Try detection from most reliable source first

    // 1. Existing super.img is most reliable
    if let Some(path) = existing_super
        && let Some(profile) = detect_from_super_image(path)? {
            return Ok(profile);
        }

    // 2. Update binary analysis
    if let Some(path) = update_binary
        && let Some(profile) = detect_from_update_binary(path)? {
            return Ok(profile);
        }

    // 3. Op list analysis (check for Virtual A/B operations)
    if let Some(path) = op_list
        && let Some(profile) = detect_from_op_list(path)? {
            return Ok(profile);
        }

    // 4. Default fallback: v10.0 + raw (safest for most devices)
    Ok(SuperImageProfile {
        lp_version: LpVersionInfo::new(10, 0),
        geometry: None,
        detection_source: DetectionSource::Default,
    })
}

/// Detect from dynamic_partitions_op_list
fn detect_from_op_list(op_list_path: &Path) -> Result<Option<SuperImageProfile>> {
    let content = std::fs::read_to_string(op_list_path)?;

    // Check for Virtual A/B specific operations
    let has_virtual_ab =
        content.contains("snapshot") || content.contains("cow") || content.contains("update-vab");

    let version = if has_virtual_ab {
        LpVersionInfo::new(10, 2) // Virtual A/B requires v10.2
    } else {
        // Check for features requiring v10.1
        let needs_v101 = content.contains("updated") || content.contains("resize");

        if needs_v101 {
            LpVersionInfo::new(10, 1)
        } else {
            LpVersionInfo::new(10, 0)
        }
    };

    Ok(Some(SuperImageProfile {
        lp_version: version,
        geometry: None,
        detection_source: DetectionSource::OpList,
    }))
}
