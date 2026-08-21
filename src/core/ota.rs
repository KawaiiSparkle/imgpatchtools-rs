//! OTA package reading utilities.
//!
//! This module centralises everything the unpacker needs to read a traditional
//! (block-based) Android OTA package **without shelling out to any external
//! binary** (notably no `7z`):
//!
//! - locate / read the `updater-script` (on disk or from inside an OTA zip);
//! - extract the `range_sha1(...)` parameters needed by `blockimg range-sha1`;
//! - extract the `apply_patch*(...)` parameters needed by `applypatch`;
//! - list and extract files from an OTA zip (via the `zip` crate).
//!
//! It deliberately does **not** implement the Edify language. The values we
//! need — target names, sizes, SHA-1 digests and patch paths — are plain string
//! literals in the script text, so a small, focused parser is sufficient and
//! keeps the bit-exactness surface small.

use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

/// Standard locations where an OTA updater script may live.
pub const SCRIPT_CANDIDATES: [&str; 4] = [
    "META-INF/com/google/android/updater-script",
    "META-INF/com/google/android/update-script",
    "META-INF/com/android/updater-script",
    "update-script",
];

// ---------------------------------------------------------------------------
// Script location (filesystem)
// ---------------------------------------------------------------------------

/// Find the updater script in the current directory or the standard META-INF
/// locations.
pub fn find_update_script() -> Result<PathBuf> {
    let current_dir = std::env::current_dir().context("get current directory")?;

    for candidate in SCRIPT_CANDIDATES {
        let path = current_dir.join(candidate);
        if path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "updater-script not found in current directory or {}",
        SCRIPT_CANDIDATES[0]
    )
}

/// Read an updater script from the filesystem as UTF-8 text.
pub fn read_update_script_from_fs(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))
}

// ---------------------------------------------------------------------------
// Script reading from an OTA zip (no external binary)
// ---------------------------------------------------------------------------

/// Read the updater script directly from inside an OTA zip package.
///
/// Iterates the central directory and returns the first script entry whose
/// name matches [`SCRIPT_CANDIDATES`]. No decompression tool is required.
pub fn read_update_script_from_zip(zip_path: &Path) -> Result<String> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("open OTA zip {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("read OTA zip {}", zip_path.display()))?;

    for candidate in SCRIPT_CANDIDATES {
        if let Ok(mut entry) = archive.by_name(candidate) {
            let mut buf = String::new();
            entry
                .read_to_string(&mut buf)
                .with_context(|| format!("read {candidate} from {}", zip_path.display()))?;
            return Ok(buf);
        }
    }

    bail!("updater-script not found in {}", zip_path.display())
}

/// Extract a single entry from an OTA zip to the given destination path.
///
/// If the destination's parent directory does not exist it is created.
pub fn extract_from_zip(zip_path: &Path, entry_name: &str, dest: &Path) -> Result<()> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("open OTA zip {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("read OTA zip {}", zip_path.display()))?;

    let mut entry = archive
        .by_name(entry_name)
        .with_context(|| format!("entry {entry_name} not found in {}", zip_path.display()))?;

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create dir {}", parent.display()))?;
    }

    let mut out = std::fs::File::create(dest)
        .with_context(|| format!("create {}", dest.display()))?;
    std::io::copy(&mut entry, &mut out)
        .with_context(|| format!("extract {entry_name} -> {}", dest.display()))?;

    Ok(())
}

/// List all entry names in an OTA zip (flattened, in central-directory order).
pub fn list_zip_entries(zip_path: &Path) -> Result<Vec<String>> {
    let file = std::fs::File::open(zip_path)
        .with_context(|| format!("open OTA zip {}", zip_path.display()))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("read OTA zip {}", zip_path.display()))?;

    let mut names = Vec::with_capacity(archive.len());
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .with_context(|| format!("read entry #{i} in {}", zip_path.display()))?;
        names.push(entry.name().to_string());
    }
    Ok(names)
}

// ---------------------------------------------------------------------------
// range_sha1 parameter extraction
// ---------------------------------------------------------------------------

/// Ranges plus optional expected SHA-1 for a partition, read from a script.
#[derive(Debug, Clone)]
pub struct RangeSha1Info {
    pub ranges: String,
    pub expected_sha1: Option<String>,
}

/// Extract `range_sha1(...)` ranges (and, when present, the expected SHA-1)
/// for a partition from updater-script text.
///
/// Handles both `range_sha1("path", "ranges") == "sha1"` and the reverse
/// comparison order.
pub fn parse_range_sha1(script: &str, partition_name: &str) -> Result<RangeSha1Info> {
    let patterns = [partition_name.to_string(), partition_name.replace('_', "/")];

    for line in script.lines() {
        let line = line.trim();
        if !line.contains("range_sha1") {
            continue;
        }
        for pattern in &patterns {
            if !line.contains(pattern.as_str()) {
                continue;
            }
            let Some(start) = line.find("range_sha1(") else {
                continue;
            };
            let Some(open_quote) = line[start..].find('"') else {
                continue;
            };
            let after_first = &line[start + open_quote + 1..];
            let Some(close_quote) = after_first.find('"') else {
                continue;
            };
            let after_path = &after_first[close_quote + 1..];
            let Some(comma) = after_path.find(',') else {
                continue;
            };
            let after_comma = after_path[comma + 1..].trim_start();
            if !after_comma.starts_with('"') {
                continue;
            }
            let Some(ranges_end) = after_comma[1..].find('"') else {
                continue;
            };
            let ranges = after_comma[1..1 + ranges_end].to_string();
            let after_call = &after_comma[1 + ranges_end + 1..];
            return Ok(RangeSha1Info {
                ranges,
                expected_sha1: extract_expected_sha1(after_call),
            });
        }
    }

    bail!("range_sha1 ranges not found for partition: {partition_name}")
}

/// Read `range_sha1` info for a partition from the on-disk updater script.
pub fn read_range_sha1_info_from_script(partition_name: &str) -> Result<RangeSha1Info> {
    let script_path = find_update_script()?;
    let content = read_update_script_from_fs(&script_path)?;
    parse_range_sha1(&content, partition_name)
}

/// Extract a 40-hex-digit SHA-1 following `==` in script text (e.g. `) == "abc…"`).
fn extract_expected_sha1(text: &str) -> Option<String> {
    let eq_pos = text.find("==")?;
    let after_eq = text[eq_pos + 2..].trim_start();
    let after_eq = after_eq.strip_prefix('"')?;
    let sha1_end = after_eq.find('"')?;
    let sha1 = &after_eq[..sha1_end];
    if sha1.len() == 40 && sha1.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(sha1.to_string())
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// apply_patch parameter extraction
// ---------------------------------------------------------------------------

/// Parameters for one `apply_patch` invocation, parsed from a script.
#[derive(Debug, Clone)]
pub struct ApplyPatchInfo {
    pub source_spec: String,
    pub target_sha1: String,
    pub target_size: u64,
    pub source_sha1: String,
    pub patch_file: PathBuf,
}

/// Extract the `apply_patch(...)` parameters for a partition from script text.
///
/// Scans the whole script (AOSP generates `apply_patch` calls across multiple
/// lines) and matches the call whose source references `partition_name`.
///
/// Supports both argument layouts used by AOSP:
///   apply_patch("src", "-", "target_sha1", size, "source_sha1", "patch/p")
///   apply_patch("src", "-", "target_sha1", size, "source_sha1:patch/p")
pub fn parse_apply_patch(script: &str, partition_name: &str) -> Result<ApplyPatchInfo> {
    let alt = partition_name.replace('_', "/");
    let needle = "apply_patch(";

    let mut search_from = 0;
    while let Some(rel) = script[search_from..].find(needle) {
        let open_paren = search_from + rel + needle.len() - 1; // index of '('
        let Some((args_text, close)) = balanced_group(script, open_paren) else {
            search_from += rel + needle.len();
            continue;
        };

        let args = split_args(args_text);
        if args.len() >= 5 {
            let source_spec = &args[0];
            if source_spec.contains(partition_name) || source_spec.contains(alt.as_str()) {
                if let Some(info) = build_apply_patch_info(&args) {
                    return Ok(info);
                }
            }
        }

        search_from = close + 1;
    }

    bail!("apply_patch command not found for partition: {partition_name}")
}

/// Read `apply_patch` info for a partition from the on-disk updater script.
pub fn parse_apply_patch_from_script(partition_name: &str) -> Result<ApplyPatchInfo> {
    let script_path = find_update_script()?;
    let content = read_update_script_from_fs(&script_path)?;
    parse_apply_patch(&content, partition_name)
}

/// Build an [`ApplyPatchInfo`] from parsed `apply_patch` arguments.
fn build_apply_patch_info(args: &[String]) -> Option<ApplyPatchInfo> {
    let source_spec = args[0].clone();
    let target_sha1 = args[2].clone();
    let target_size: u64 = args[3].parse().ok()?;

    let (source_sha1, raw_patch) = if args.len() >= 6 {
        (args[4].clone(), args[5].clone())
    } else if args[4].contains(':') {
        let (sha1, patch) = args[4].split_once(':')?;
        (sha1.to_string(), patch.to_string())
    } else {
        return None;
    };

    // The script may write `package:boot.img.p` to mean "inside the OTA zip";
    // the actual entry lives under `patch/` (or at the root) of the package.
    let patch_file = PathBuf::from(raw_patch.strip_prefix("package:").unwrap_or(&raw_patch));

    Some(ApplyPatchInfo {
        source_spec,
        target_sha1,
        target_size,
        source_sha1,
        patch_file,
    })
}

/// Return the text between a matched `(` at `open` and its balanced `)`,
/// together with the index of the closing paren. Skips over quoted strings.
fn balanced_group(s: &str, open: usize) -> Option<(&str, usize)> {
    let bytes = s.as_bytes();
    let mut depth = 0usize;
    let mut i = open;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some((&s[open + 1..i], i));
                }
            }
            b'"' => {
                // Skip a double-quoted string literal (handling `\"` escapes).
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split a comma-separated argument list, respecting double-quoted strings and
/// stripping surrounding quotes from each argument.
fn split_args(content: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for c in content.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            ',' if !in_quotes => {
                push_arg(&mut args, &mut current);
            }
            _ => current.push(c),
        }
    }
    push_arg(&mut args, &mut current);
    args
}

fn push_arg(args: &mut Vec<String>, current: &mut String) {
    let trimmed = current.trim();
    if trimmed.is_empty() {
        current.clear();
        return;
    }
    let arg = if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    };
    args.push(arg);
    current.clear();
}
