//! dm-verity Merkle hash tree builder — port of AOSP `HashTreeBuilder`.
//!
//! Computes a Merkle tree for dm-verity verified boot. The tree is stored
//! in a contiguous region at the end of the partition.
//!
//! # dm-verity hash tree format
//!
//! 1. Leaf level: SHA256(block data + salt) for each 4KB block
//! 2. Internal levels: SHA256(child hashes concatenated + salt)
//! 3. Root hash: single hash at the top
//!
//! The tree is laid out bottom-up, with padding to block boundaries.

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};

/// Hash algorithm supported for dm-verity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    Sha256,
}

impl HashAlgorithm {
    /// Parse algorithm name (matches AOSP).
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "sha256" => Ok(Self::Sha256),
            other => bail!("unsupported hash algorithm: {other}"),
        }
    }

    /// Output size in bytes.
    pub fn digest_size(&self) -> usize {
        match self {
            Self::Sha256 => 32,
        }
    }

    /// Create a new hasher instance.
    pub fn new_hasher(&self) -> Box<dyn DynDigest> {
        match self {
            Self::Sha256 => Box::new(Sha256::new()),
        }
    }
}

/// Dynamic digest trait for polymorphic hashing.
pub trait DynDigest {
    fn update(&mut self, data: &[u8]);
    fn finalize(&mut self) -> Vec<u8>;
    fn finalize_reset(&mut self) -> Vec<u8>;
}

impl DynDigest for Sha256 {
    fn update(&mut self, data: &[u8]) {
        Digest::update(self, data);
    }
    fn finalize(&mut self) -> Vec<u8> {
        Digest::finalize(std::mem::replace(self, Sha256::new()))
            .as_slice()
            .to_vec()
    }
    fn finalize_reset(&mut self) -> Vec<u8> {
        let result = self.clone().finalize();
        Digest::reset(self);
        result.as_slice().to_vec()
    }
}

/// Merkle hash tree builder for dm-verity.
pub struct HashTreeBuilder {
    block_size: usize,
    hash_alg: HashAlgorithm,
    salt: Vec<u8>,
    /// Number of data blocks (leaf nodes).
    data_blocks: u64,
    /// Tree levels from bottom to top (each level is a vector of hashes).
    levels: Vec<Vec<u8>>,
}

impl HashTreeBuilder {
    /// Create a new builder.
    ///
    /// # Arguments
    /// * `block_size` - Usually 4096 bytes.
    /// * `hash_alg` - Hash algorithm (SHA256).
    pub fn new(block_size: usize, hash_alg: HashAlgorithm) -> Self {
        Self {
            block_size,
            hash_alg,
            salt: Vec::new(),
            data_blocks: 0,
            levels: Vec::new(),
        }
    }

    /// Initialize the builder with data size and salt.
    ///
    /// # Arguments
    /// * `data_size` - Total size of data to hash (must be block-aligned).
    /// * `salt` - Salt bytes (hex-decoded).
    pub fn initialize(&mut self, data_size: u64, salt: Vec<u8>) -> Result<()> {
        ensure!(
            data_size.is_multiple_of(self.block_size as u64),
            "data size {} is not block-aligned",
            data_size
        );
        self.data_blocks = data_size / self.block_size as u64;
        self.salt = salt;
        self.levels.clear();
        Ok(())
    }

    /// Compute number of blocks needed to store the hash tree.
    pub fn tree_blocks(&self) -> u64 {
        let hash_size = self.hash_alg.digest_size() as u64;
        let hashes_per_block = self.block_size as u64 / hash_size;

        let mut tree_blocks = 0u64;
        let mut level_blocks = self.data_blocks;

        while level_blocks > 1 {
            level_blocks = level_blocks.div_ceil(hashes_per_block);
            tree_blocks += level_blocks;
        }

        tree_blocks
    }

    /// Update the hash tree with a data block.
    ///
    /// Call this for each data block in order.
    pub fn update(&mut self, block_data: &[u8]) -> Result<()> {
        ensure!(
            block_data.len() == self.block_size,
            "block data size {} != block size {}",
            block_data.len(),
            self.block_size
        );

        // Compute leaf hash: SHA256(salt + block_data)
        let mut hasher = self.hash_alg.new_hasher();
        hasher.update(&self.salt);
        hasher.update(block_data);
        let hash = hasher.finalize();

        // Add to level 0 (leaves)
        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].extend_from_slice(&hash);

        Ok(())
    }

    /// Build the complete Merkle tree.
    ///
    /// After all `update()` calls, this builds internal levels.
    pub fn build_tree(&mut self) -> Result<()> {
        let hash_size = self.hash_alg.digest_size();
        let hashes_per_block = self.block_size / hash_size;

        let mut level_idx = 0;
        while self.levels[level_idx].len() / hash_size > 1 {
            let current_level = &self.levels[level_idx];
            let num_hashes = current_level.len() / hash_size;
            let num_parents = num_hashes.div_ceil(hashes_per_block);

            let mut next_level = Vec::with_capacity(num_parents * hash_size);

            for parent_idx in 0..num_parents {
                let start_hash = parent_idx * hashes_per_block;
                let end_hash = (start_hash + hashes_per_block).min(num_hashes);

                // Collect child hashes for this parent
                let mut hasher = self.hash_alg.new_hasher();
                hasher.update(&self.salt);

                for h in start_hash..end_hash {
                    let offset = h * hash_size;
                    hasher.update(&current_level[offset..offset + hash_size]);
                }

                let parent_hash = hasher.finalize();
                next_level.extend_from_slice(&parent_hash);
            }

            self.levels.push(next_level);
            level_idx += 1;
        }

        Ok(())
    }

    /// Get the root hash (top of the tree).
    pub fn root_hash(&self) -> &[u8] {
        if self.levels.is_empty() {
            return &[];
        }
        let top_level = self.levels.last().unwrap();
        if top_level.len() >= self.hash_alg.digest_size() {
            &top_level[0..self.hash_alg.digest_size()]
        } else {
            &[]
        }
    }

    /// Serialize the tree to a byte vector (for writing to disk).
    ///
    /// The tree is serialized bottom-up, with block-level padding.
    pub fn serialize_tree(&self) -> Vec<u8> {
        let tree_size = (self.tree_blocks() as usize) * self.block_size;
        let mut result = Vec::with_capacity(tree_size);

        // Write levels from bottom to top
        for level in &self.levels {
            result.extend_from_slice(level);
            // Pad to block boundary
            let padding = self.block_size - (level.len() % self.block_size);
            if padding != self.block_size {
                result.extend(std::iter::repeat_n(0, padding));
            }
        }

        result
    }

    /// Parse a hex string to bytes (matches AOSP `ParseBytesArrayFromString`).
    pub fn parse_hex_bytes(hex: &str) -> Result<Vec<u8>> {
        let hex = hex.trim();
        if hex.is_empty() {
            return Ok(Vec::new());
        }
        let hex = if hex.starts_with("0x") || hex.starts_with("0X") {
            &hex[2..]
        } else {
            hex
        };

        ensure!(
            hex.len() % 2 == 0,
            "hex string length {} is not even",
            hex.len()
        );

        (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16)
                    .with_context(|| format!("invalid hex at position {}", i))
            })
            .collect::<Result<Vec<u8>>>()
    }

    /// Convert bytes to hex string (matches AOSP `BytesArrayToString`).
    pub fn bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
