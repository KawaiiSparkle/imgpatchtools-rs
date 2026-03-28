//! Block-image verification — AOSP BlockImageVerifyFn port.

use std::path::Path;

use anyhow::{ensure, Context, Result};

use crate::core::blockimg::transfer_list::parse_transfer_list;
use crate::util::hash;
use crate::util::io::BlockFile;
use crate::util::rangeset::RangeSet;

const BLOCK_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// block_image_verify
// ---------------------------------------------------------------------------

/// Verify a source image against a transfer list (pre-update check).
///
/// # AOSP 语义
///
/// 对每条有 src_hash + src_ranges 的命令：
/// - 若 src_ranges 与"已被更早命令写入的块集合"有重叠 → **跳过**
///   （这些块的内容已被前序命令修改，无法在原始镜像上验证）
/// - 若有 stash_refs → **跳过**（缺少 stash 数据，无法重建完整源 buffer）
/// - 否则读 src_ranges → 与 src_hash 对比
///
/// 返回 `Ok(true)` 表示所有可验证命令通过，`Ok(false)` 表示有命令失败。
pub fn block_image_verify(
    target_path: &Path,
    transfer_list_path: &Path,
) -> Result<bool> {
    let tl_content = std::fs::read_to_string(transfer_list_path).with_context(|| {
        format!("verify: failed to read {}", transfer_list_path.display())
    })?;
    let tl = parse_transfer_list(&tl_content)
        .context("verify: failed to parse transfer list")?;

    if tl.version() < 3 {
        log::info!(
            "verify: v{} has no hash fields, skipping (pass)",
            tl.version()
        );
        return Ok(true);
    }

    let image = BlockFile::open(target_path, BLOCK_SIZE).with_context(|| {
        format!("verify: failed to open {}", target_path.display())
    })?;

    let mut pass_count: u64 = 0;
    let mut fail_count: u64 = 0;
    let mut skip_count: u64 = 0;

    // 追踪已被前序命令写入的块集合（target_ranges 的并集）。
    // src_ranges 与此集合重叠的命令必须跳过。
    let mut written_blocks = RangeSet::new();

    for (idx, cmd) in tl.commands.iter().enumerate() {
        // 1. 先将本命令的 target_ranges 并入 written_blocks（在决定跳过之前）。
        //    注意：要在验证之前记录，因为验证的是"当前命令之前"的状态。
        //    但目标块的记录应在本命令"执行"后生效，所以在验证后记录。
        //    → 我们在验证结束后统一更新。

        // 2. 过滤：只处理有源数据的命令
        if !cmd.cmd_type.has_source() {
            // zero/new/erase/stash/free：更新 written_blocks 后跳过
            if let Some(tgt) = cmd.target_ranges.as_ref() {
                written_blocks = written_blocks.merge(tgt);
            }
            skip_count += 1;
            continue;
        }

        // 3. 必须有 src_hash
        let expected_hash = match cmd.src_hash.as_deref() {
            Some(h) => h,
            None => {
                if let Some(tgt) = cmd.target_ranges.as_ref() {
                    written_blocks = written_blocks.merge(tgt);
                }
                skip_count += 1;
                continue;
            }
        };

        // 4. 必须有 src_ranges
        let ranges = match cmd.src_ranges.as_ref() {
            Some(r) => r,
            None => {
                if let Some(tgt) = cmd.target_ranges.as_ref() {
                    written_blocks = written_blocks.merge(tgt);
                }
                skip_count += 1;
                log::debug!(
                    "verify cmd[{idx}] ({}): skip (no src_ranges / all-stash)",
                    cmd.cmd_type.as_str()
                );
                continue;
            }
        };

        // 5. 含 stash_refs → 跳过
        if !cmd.src_stash_refs.is_empty() {
            if let Some(tgt) = cmd.target_ranges.as_ref() {
                written_blocks = written_blocks.merge(tgt);
            }
            skip_count += 1;
            log::debug!(
                "verify cmd[{idx}] ({}): skip (has stash_refs)",
                cmd.cmd_type.as_str()
            );
            continue;
        }

        // 6. src_ranges 与已写入块重叠 → 跳过
        //    这些块已被前序命令修改，原始镜像中的内容已不是 src_hash 期望的值。
        if written_blocks.overlaps(ranges) {
            if let Some(tgt) = cmd.target_ranges.as_ref() {
                written_blocks = written_blocks.merge(tgt);
            }
            skip_count += 1;
            log::debug!(
                "verify cmd[{idx}] ({}): skip (src_ranges overlaps previously written blocks)",
                cmd.cmd_type.as_str()
            );
            continue;
        }

        // 7. 执行验证
        let data = image.read_ranges(ranges).with_context(|| {
            format!("verify cmd[{idx}]: failed to read src_ranges")
        })?;
        let actual = hash::sha1_hex(&data);

        if actual.eq_ignore_ascii_case(expected_hash) {
            pass_count += 1;
            log::debug!("verify cmd[{idx}] ({}): PASS", cmd.cmd_type.as_str());
        } else {
            fail_count += 1;
            log::error!(
                "verify cmd[{idx}] ({}): FAIL — expected {}, got {}",
                cmd.cmd_type.as_str(),
                expected_hash,
                actual,
            );
        }

        // 8. 记录本命令写入的块
        if let Some(tgt) = cmd.target_ranges.as_ref() {
            written_blocks = written_blocks.merge(tgt);
        }
    }

    log::info!(
        "verify complete: {} passed, {} failed, {} skipped (v{})",
        pass_count, fail_count, skip_count,
        tl.version()
    );

    Ok(fail_count == 0)
}

// ---------------------------------------------------------------------------
// range_sha1
// ---------------------------------------------------------------------------

pub fn range_sha1(file_path: &Path, ranges: &RangeSet, block_size: usize) -> Result<String> {
    ensure!(block_size > 0, "block_size must be positive");
    let bf = BlockFile::open(file_path, block_size)
        .with_context(|| format!("range_sha1: failed to open {}", file_path.display()))?;
    let data = bf.read_ranges(ranges).context("range_sha1: failed to read")?;
    Ok(hash::sha1_hex(&data))
}

pub fn range_sha1_str(file_path: &Path, ranges_str: &str, block_size: usize) -> Result<String> {
    let ranges = RangeSet::parse(ranges_str).context("range_sha1: bad range string")?;
    range_sha1(file_path, &ranges, block_size)
}

// ---------------------------------------------------------------------------
// check_first_block
// ---------------------------------------------------------------------------

pub fn check_first_block(file_path: &Path, block_size: usize) -> Result<bool> {
    ensure!(block_size > 0, "block_size must be positive");
    let bf = BlockFile::open(file_path, block_size)
        .with_context(|| format!("check_first_block: failed to open {}", file_path.display()))?;
    ensure!(bf.total_blocks() > 0, "check_first_block: no complete blocks");
    let first = RangeSet::from_pairs(&[(0, 1)])?;
    let data = bf.read_ranges(&first).context("check_first_block: read failed")?;
    Ok(data.iter().any(|&b| b != 0))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    const BS: usize = BLOCK_SIZE;

    fn write_file(dir: &tempfile::TempDir, name: &str, data: &[u8]) -> std::path::PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, data).unwrap();
        path
    }

    #[test]
    fn verify_v1_passes() {
        let dir = tempfile::tempdir().unwrap();
        let tl = "1\n4\nnew 2,0,4\n";
        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let img_path = write_file(&dir, "img.bin", &vec![0u8; 4 * BS]);
        assert!(block_image_verify(&img_path, &tl_path).unwrap());
    }

    #[test]
    fn verify_v3_src_hash_pass() {
        let dir = tempfile::tempdir().unwrap();
        let source_data = vec![0xAAu8; 4 * BS];
        let src_hash = hash::sha1_hex(&source_data);
        let tl = format!("3\n8\n0\n0\nmove {src_hash} 2,4,8 4 2,0,4\n");
        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let mut img = source_data.clone();
        img.extend(vec![0u8; 4 * BS]);
        let img_path = write_file(&dir, "img.bin", &img);
        assert!(block_image_verify(&img_path, &tl_path).unwrap());
    }

    #[test]
    fn verify_v3_src_hash_fail() {
        let dir = tempfile::tempdir().unwrap();
        let expected = vec![0xAAu8; 4 * BS];
        let src_hash = hash::sha1_hex(&expected);
        let tl = format!("3\n8\n0\n0\nmove {src_hash} 2,4,8 4 2,0,4\n");
        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        // 实际内容是 0xFF
        let img_path = write_file(&dir, "img.bin", &vec![0xFFu8; 8 * BS]);
        assert!(!block_image_verify(&img_path, &tl_path).unwrap());
    }

    #[test]
    fn verify_skips_overlapping_src_ranges() {
        // move1 写入 [0,4)，move2 的 src_ranges 也是 [0,4) → 应跳过 move2
        let dir = tempfile::tempdir().unwrap();
        let data = vec![0xAAu8; 4 * BS];
        let h1 = hash::sha1_hex(&data);
        // move2 的 src_hash 是 0xFF 的哈希（肯定不匹配），但应该被跳过
        let h2 = "0000000000000000000000000000000000000000";
        let tl = format!(
            "3\n8\n0\n0\nmove {h1} 2,0,4 4 2,4,8\nmove {h2} 2,4,8 4 2,0,4\n"
        );
        let tl_path = write_file(&dir, "tl.txt", tl.as_bytes());
        let mut img = data.clone();
        img.extend(vec![0xBBu8; 4 * BS]);
        let img_path = write_file(&dir, "img.bin", &img);
        // move2 的 src_ranges=[0,4) 与 move1 的 target=[0,4) 重叠，应跳过，不 FAIL
        assert!(block_image_verify(&img_path, &tl_path).unwrap());
    }

    #[test]
    fn verify_bad_tl_fails() {
        let dir = tempfile::tempdir().unwrap();
        let tl_path = write_file(&dir, "tl.txt", b"garbage");
        let img_path = write_file(&dir, "img.bin", &vec![0u8; BS]);
        assert!(block_image_verify(&img_path, &tl_path).is_err());
    }
}