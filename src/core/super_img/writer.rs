//! Super image writer — raw and Android Sparse output formats.
//!
//! Memory usage is bounded by a fixed copy buffer regardless of partition
//! image size. A live progress bar shows throughput and ETA.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use sha2::{Digest, Sha256};

use super::sparse;

/// Copy buffer size — 4 MiB.
const COPY_BUF_SIZE: usize = 4 * 1024 * 1024;

/// Sparse format block size — must be 4096 to match LP logical_block_size.
const SPARSE_BLOCK_SIZE: u32 = 4096;

// ---------------------------------------------------------------------------
// Output format
// ---------------------------------------------------------------------------

/// Super image output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuperImageFormat {
    /// Raw binary — full device_size on disk.
    Raw,
    /// Android Sparse — zero regions stored as DONT_CARE, much smaller file.
    Sparse,
}

// ---------------------------------------------------------------------------
// Public API — unified entry point
// ---------------------------------------------------------------------------

/// Write a super.img in the chosen format.
pub fn write_super(
    output: &Path,
    metadata: &LpMetadata,
    images: &[(String, String)],
    format: SuperImageFormat,
) -> Result<()> {
    match format {
        SuperImageFormat::Raw => write_super_image(output, metadata, images),
        SuperImageFormat::Sparse => write_super_image_sparse(output, metadata, images),
    }
}

// ---------------------------------------------------------------------------
// Raw writer (unchanged from previous version)
// ---------------------------------------------------------------------------

pub fn write_super_image(
    output: &Path,
    metadata: &LpMetadata,
    images: &[(String, String)],
) -> Result<()> {
    let dev_size = metadata.block_devices[0].size;
    log::info!("writing super.img (raw): {} bytes → {}", dev_size, output.display());

    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output)
        .with_context(|| format!("create {}", output.display()))?;
    file.set_len(dev_size).context("set_len")?;

    let mut w = BufWriter::with_capacity(4 * 1024 * 1024, file);

    // Geometry × 2
    let geo = metadata.geometry.to_block();
    w.seek(SeekFrom::Start(LP_PARTITION_RESERVED_BYTES))?;
    w.write_all(&geo)?;
    w.write_all(&geo)?;

    // Metadata blob
    let blob = serialize_blob(metadata)?;
    let md_start = LP_PARTITION_RESERVED_BYTES + (LP_METADATA_GEOMETRY_SIZE as u64) * 2;

    for slot in 0..metadata.geometry.metadata_slot_count {
        let off = md_start + (slot as u64) * (metadata.geometry.metadata_max_size as u64);
        w.seek(SeekFrom::Start(off))?;
        w.write_all(&blob)?;
    }
    let bk_start = md_start
        + (metadata.geometry.metadata_slot_count as u64)
            * (metadata.geometry.metadata_max_size as u64);
    for slot in 0..metadata.geometry.metadata_slot_count {
        let off = bk_start + (slot as u64) * (metadata.geometry.metadata_max_size as u64);
        w.seek(SeekFrom::Start(off))?;
        w.write_all(&blob)?;
    }

    // Partition images — streamed with progress
    let total_image_bytes: u64 = images
        .iter()
        .map(|(_n, p)| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();

    let pb = make_bytes_progress_bar(total_image_bytes, "super.img");
    let mut buf = vec![0u8; COPY_BUF_SIZE];

    for (part_name, img_path) in images {
        let pidx = metadata
            .partitions
            .iter()
            .position(|p| p.name_str() == part_name);
        let pidx = match pidx {
            Some(i) => i,
            None => continue,
        };
        let part = &metadata.partitions[pidx];
        if part.num_extents == 0 {
            continue;
        }

        let src_file = File::open(img_path)
            .with_context(|| format!("open image {}", img_path))?;
        let src_len = src_file.metadata()?.len();
        let mut reader = BufReader::with_capacity(1 << 20, src_file);
        let mut img_off: u64 = 0;

        pb.set_prefix(format!("{}", part_name));

        for i in 0..part.num_extents {
            let ext = &metadata.extents[(part.first_extent_index + i) as usize];
            if ext.target_type != LP_TARGET_TYPE_LINEAR {
                continue;
            }
            let disk_off = ext.target_data * LP_SECTOR_SIZE;
            let ext_bytes = ext.num_sectors * LP_SECTOR_SIZE;
            let wlen = ext_bytes.min(src_len.saturating_sub(img_off));
            if wlen == 0 {
                break;
            }

            w.seek(SeekFrom::Start(disk_off))?;
            let mut remaining = wlen;
            while remaining > 0 {
                let chunk = (remaining as usize).min(COPY_BUF_SIZE);
                reader.read_exact(&mut buf[..chunk])?;
                w.write_all(&buf[..chunk])?;
                remaining -= chunk as u64;
                pb.inc(chunk as u64);
            }
            img_off += wlen;
        }
        log::info!("wrote partition '{}': {} bytes", part_name, img_off);
    }

    pb.finish_with_message("done");
    w.flush()?;
    log::info!("super.img (raw) complete: {}", output.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Sparse writer
// ---------------------------------------------------------------------------

/// A data region to be written as a RAW chunk in the sparse image.
struct SparseRegion {
    /// Starting block index in the output image.
    start_block: u32,
    /// Number of 4096-byte blocks.
    num_blocks: u32,
    /// Source of the data.
    source: RegionSource,
}

enum RegionSource {
    /// Small in-memory buffer (geometry + metadata).
    Buffer(Vec<u8>),
    /// Streamed from a file: (path, offset_in_file, byte_length).
    FileRange(String, u64, u64),
}

pub fn write_super_image_sparse(
    output: &Path,
    metadata: &LpMetadata,
    images: &[(String, String)],
) -> Result<()> {
    let dev_size = metadata.block_devices[0].size;
    let total_blocks = (dev_size / SPARSE_BLOCK_SIZE as u64) as u32;

    log::info!(
        "writing super.img (sparse): device_size={} ({} blocks) → {}",
        dev_size,
        total_blocks,
        output.display()
    );

    // -----------------------------------------------------------------------
    // 1. Build the metadata region buffer (geometry + all slots)
    // -----------------------------------------------------------------------
    let geo = metadata.geometry.to_block();
    let blob = serialize_blob(metadata)?;

    let meta_region_start = LP_PARTITION_RESERVED_BYTES;
    let md_start = meta_region_start + (LP_METADATA_GEOMETRY_SIZE as u64) * 2;
    let slot_size = metadata.geometry.metadata_max_size as u64;
    let num_slots = metadata.geometry.metadata_slot_count as u64;
    let meta_region_end = md_start + slot_size * num_slots * 2;
    let meta_region_len = (meta_region_end - meta_region_start) as usize;

    let mut meta_buf = vec![0u8; meta_region_len];

    // Geometry × 2
    meta_buf[..geo.len()].copy_from_slice(&geo);
    meta_buf[geo.len()..geo.len() * 2].copy_from_slice(&geo);

    // Primary metadata slots
    let prim_off = (LP_METADATA_GEOMETRY_SIZE as usize) * 2;
    for slot in 0..num_slots as usize {
        let off = prim_off + slot * slot_size as usize;
        meta_buf[off..off + blob.len()].copy_from_slice(&blob);
    }

    // Backup metadata slots
    let bk_off = prim_off + (num_slots as usize) * (slot_size as usize);
    for slot in 0..num_slots as usize {
        let off = bk_off + slot * slot_size as usize;
        meta_buf[off..off + blob.len()].copy_from_slice(&blob);
    }

    // -----------------------------------------------------------------------
    // 2. Build region list
    // -----------------------------------------------------------------------
    let mut regions: Vec<SparseRegion> = Vec::new();

    // Metadata region
    let meta_start_block = (meta_region_start / SPARSE_BLOCK_SIZE as u64) as u32;
    let meta_num_blocks = (meta_region_len as u64 / SPARSE_BLOCK_SIZE as u64) as u32;
    regions.push(SparseRegion {
        start_block: meta_start_block,
        num_blocks: meta_num_blocks,
        source: RegionSource::Buffer(meta_buf),
    });

    // Partition regions
    for (part_name, img_path) in images {
        let pidx = match metadata
            .partitions
            .iter()
            .position(|p| p.name_str() == part_name)
        {
            Some(i) => i,
            None => continue,
        };
        let part = &metadata.partitions[pidx];
        if part.num_extents == 0 {
            continue;
        }

        let img_len = std::fs::metadata(img_path)
            .with_context(|| format!("stat {}", img_path))?
            .len();
        let mut img_off: u64 = 0;

        for i in 0..part.num_extents {
            let ext = &metadata.extents[(part.first_extent_index + i) as usize];
            if ext.target_type != LP_TARGET_TYPE_LINEAR {
                continue;
            }

            let disk_off = ext.target_data * LP_SECTOR_SIZE;
            let ext_bytes = ext.num_sectors * LP_SECTOR_SIZE;
            let wlen = ext_bytes.min(img_len.saturating_sub(img_off));
            if wlen == 0 {
                break;
            }

            let start_block = (disk_off / SPARSE_BLOCK_SIZE as u64) as u32;
            // Round up to full blocks — pad with zeros if needed
            let num_blocks =
                ((wlen + SPARSE_BLOCK_SIZE as u64 - 1) / SPARSE_BLOCK_SIZE as u64) as u32;

            regions.push(SparseRegion {
                start_block,
                num_blocks,
                source: RegionSource::FileRange(img_path.clone(), img_off, wlen),
            });

            img_off += wlen;
        }
    }

    // Sort by position
    regions.sort_by_key(|r| r.start_block);

    // -----------------------------------------------------------------------
    // 3. Count chunks and compute total data bytes for progress
    // -----------------------------------------------------------------------
    let mut chunk_count: u32 = 0;
    let mut cursor: u32 = 0;
    let mut total_data_bytes: u64 = 0;

    for r in &regions {
        if r.start_block > cursor {
            chunk_count += 1; // DONT_CARE gap
        }
        chunk_count += 1; // RAW data
        total_data_bytes += r.num_blocks as u64 * SPARSE_BLOCK_SIZE as u64;
        cursor = r.start_block + r.num_blocks;
    }
    if cursor < total_blocks {
        chunk_count += 1; // trailing DONT_CARE
    }

    // -----------------------------------------------------------------------
    // 4. Write sparse file
    // -----------------------------------------------------------------------
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output)
        .with_context(|| format!("create {}", output.display()))?;
    let mut w = BufWriter::with_capacity(4 * 1024 * 1024, file);

    // Sparse header
    sparse::write_sparse_header(&mut w, SPARSE_BLOCK_SIZE, total_blocks, chunk_count)?;

    // Progress bar
    let pb = make_bytes_progress_bar(total_data_bytes, "sparse");

    let mut copy_buf = vec![0u8; COPY_BUF_SIZE];
    let mut cursor: u32 = 0;

    for r in &regions {
        // Gap before this region → DONT_CARE
        if r.start_block > cursor {
            sparse::write_dont_care_chunk(&mut w, r.start_block - cursor)?;
        }

        let chunk_bytes = r.num_blocks as u64 * SPARSE_BLOCK_SIZE as u64;

        // RAW chunk header
        sparse::write_raw_chunk_header(&mut w, r.num_blocks, SPARSE_BLOCK_SIZE)?;

        // RAW chunk data
        match &r.source {
            RegionSource::Buffer(data) => {
                w.write_all(data)?;
                // Pad to exact chunk_bytes if buffer is smaller
                let pad = chunk_bytes as usize - data.len();
                if pad > 0 {
                    let zeros = vec![0u8; pad];
                    w.write_all(&zeros)?;
                }
                pb.inc(chunk_bytes);
            }
            RegionSource::FileRange(path, offset, length) => {
                let f = File::open(path)
                    .with_context(|| format!("open {}", path))?;
                let mut reader = BufReader::with_capacity(1 << 20, f);
                reader.seek(SeekFrom::Start(*offset))?;

                // Stream the actual file data
                let mut remaining = *length;
                while remaining > 0 {
                    let chunk = (remaining as usize).min(COPY_BUF_SIZE);
                    reader.read_exact(&mut copy_buf[..chunk])?;
                    w.write_all(&copy_buf[..chunk])?;
                    remaining -= chunk as u64;
                    pb.inc(chunk as u64);
                }

                // Pad to block boundary with zeros
                let pad = chunk_bytes - length;
                if pad > 0 {
                    let zeros = vec![0u8; pad as usize];
                    w.write_all(&zeros)?;
                    pb.inc(pad);
                }
            }
        }

        cursor = r.start_block + r.num_blocks;
    }

    // Trailing DONT_CARE
    if cursor < total_blocks {
        sparse::write_dont_care_chunk(&mut w, total_blocks - cursor)?;
    }

    pb.finish_with_message("done");
    w.flush()?;

    let sparse_size = std::fs::metadata(output)?.len();
    log::info!(
        "super.img (sparse) complete: {} — sparse size {} ({:.1} MB, {:.0}% of raw)",
        output.display(),
        sparse_size,
        sparse_size as f64 / 1048576.0,
        sparse_size as f64 / dev_size as f64 * 100.0,
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn make_bytes_progress_bar(total: u64, prefix: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    let style = ProgressStyle::with_template(
        "{prefix:>12.cyan.bold} [{bar:40.green/dark_gray}] {bytes}/{total_bytes} ({percent}%) {binary_bytes_per_sec} ETA {eta}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("█▓░");
    pb.set_style(style);
    pb.set_prefix(prefix.to_string());
    pb
}

fn serialize_blob(metadata: &LpMetadata) -> Result<Vec<u8>> {
    let mut header = metadata.header.clone();

    // Tables
    let mut tables = Vec::new();
    for p in &metadata.partitions {
        tables.extend_from_slice(&p.to_bytes());
    }
    for e in &metadata.extents {
        tables.extend_from_slice(&e.to_bytes());
    }
    for g in &metadata.groups {
        tables.extend_from_slice(&g.to_bytes());
    }
    for bd in &metadata.block_devices {
        tables.extend_from_slice(&bd.to_bytes());
    }

    header.tables_checksum = Sha256::digest(&tables).into();
    header.tables_size = tables.len() as u32;

    // Header checksum
    header.header_checksum = [0u8; 32];
    let mut hb = header.to_bytes();
    let hcheck: [u8; 32] = Sha256::digest(&hb[..header.header_size as usize]).into();
    hb[12..44].copy_from_slice(&hcheck);

    let mut blob = Vec::with_capacity(metadata.geometry.metadata_max_size as usize);
    blob.extend_from_slice(&hb);
    blob.extend_from_slice(&tables);
    blob.resize(metadata.geometry.metadata_max_size as usize, 0);
    Ok(blob)
}