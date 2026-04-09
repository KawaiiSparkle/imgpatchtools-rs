//! `compute_hash_tree` command — dm-verity Merkle tree computation.
//!
//! Computes a dm-verity hash tree for the specified data blocks and writes
//! it to the designated location. Used for verified boot on Android.
//!
//! # Command format (v4 transfer list)
//!
//! ```text
//! compute_hash_tree <hash_tree_ranges> <source_ranges> <hash_alg> <salt_hex> <expected_root_hash>
//! ```
//!
//! Example:
//! ```text
//! compute_hash_tree 2,262144,262168 2,0,262144 sha256 <salt> <root_hash>
//! ```

use crate::core::blockimg::context::CommandContext;
use crate::core::blockimg::hash_tree::{HashAlgorithm, HashTreeBuilder};
use crate::core::blockimg::transfer_list::TransferCommand;
use crate::util::rangeset::RangeSet;
use anyhow::{Context, Result, ensure};

pub fn cmd_compute_hash_tree(ctx: &mut CommandContext, cmd: &TransferCommand) -> Result<()> {
    // Parse parameters (order matches C++ implementation)
    let hash_tree_ranges = cmd
        .hash_tree_ranges
        .as_ref()
        .context("compute_hash_tree: missing hash_tree_ranges")?;
    let source_ranges = cmd
        .hash_tree_source_ranges
        .as_ref()
        .context("compute_hash_tree: missing source_ranges")?;
    let hash_alg = cmd
        .hash_algorithm
        .as_ref()
        .context("compute_hash_tree: missing hash_algorithm")?;
    let salt_hex = cmd
        .hash_tree_salt
        .as_ref()
        .context("compute_hash_tree: missing salt")?;
    let expected_root_hash = cmd
        .hash_tree_root_hash
        .as_ref()
        .context("compute_hash_tree: missing expected_root_hash")?;

    // Validate hash_tree_ranges is contiguous (single range)
    ensure!(
        hash_tree_ranges.iter().count() == 1,
        "compute_hash_tree: hash_tree_ranges must be contiguous (single range)"
    );

    // Parse hash algorithm
    let hash_alg = HashAlgorithm::parse(hash_alg)?;

    // Parse salt from hex
    let salt = HashTreeBuilder::parse_hex_bytes(salt_hex)
        .with_context(|| format!("compute_hash_tree: invalid salt hex: {salt_hex}"))?;

    // Calculate data size and initialize builder
    let data_size = source_ranges.blocks() * ctx.block_size as u64;
    let mut builder = HashTreeBuilder::new(ctx.block_size, hash_alg);
    builder
        .initialize(data_size, salt)
        .context("compute_hash_tree: failed to initialize builder")?;

    log::info!(
        "compute_hash_tree: processing {} data blocks, writing {} tree blocks",
        source_ranges.blocks(),
        builder.tree_blocks()
    );

    // Read source blocks and update hash tree
    // Process in chunks to avoid large allocations
    const CHUNK_BLOCKS: u64 = 1024; // Process 4MB at a time
    let total_blocks = source_ranges.blocks();
    let mut processed_blocks = 0u64;

    for (range_start, range_end) in source_ranges.iter() {
        let _range_blocks = range_end - range_start;
        let mut current = range_start;

        while current < range_end {
            let chunk_end = (current + CHUNK_BLOCKS).min(range_end);
            let chunk_range = RangeSet::from_range(current, chunk_end);

            // Read chunk data
            let chunk_data = ctx.target.read_ranges(&chunk_range)?;

            // Update hash tree with each block in the chunk
            for block_idx in 0..(chunk_end - current) as usize {
                let offset = block_idx * ctx.block_size;
                let block = &chunk_data[offset..offset + ctx.block_size];
                builder.update(block)?;
            }

            processed_blocks += chunk_end - current;
            current = chunk_end;

            // Report progress periodically
            if processed_blocks.is_multiple_of(CHUNK_BLOCKS * 10) || processed_blocks == total_blocks {
                // Progress reporting is handled at command level
            }
        }
    }

    // Build the complete tree
    builder
        .build_tree()
        .context("compute_hash_tree: failed to build hash tree")?;

    // Verify root hash
    let actual_root_hash = HashTreeBuilder::bytes_to_hex(builder.root_hash());
    ensure!(
        actual_root_hash.eq_ignore_ascii_case(expected_root_hash),
        "compute_hash_tree: root hash mismatch! expected {}, got {}",
        expected_root_hash,
        actual_root_hash
    );

    log::info!(
        "compute_hash_tree: root hash verified: {}",
        actual_root_hash
    );

    // Serialize and write hash tree to target
    let tree_data = builder.serialize_tree();
    let expected_tree_size = (builder.tree_blocks() as usize) * ctx.block_size;
    ensure!(
        tree_data.len() == expected_tree_size,
        "compute_hash_tree: tree size mismatch: expected {}, got {}",
        expected_tree_size,
        tree_data.len()
    );

    // Write to hash_tree_ranges (which should have exactly the right size)
    ctx.target.write_ranges(hash_tree_ranges, &tree_data)?;

    log::info!(
        "compute_hash_tree: wrote {} bytes of hash tree to blocks {}",
        tree_data.len(),
        hash_tree_ranges
    );

    ctx.written_blocks += hash_tree_ranges.blocks();

    Ok(())
}
