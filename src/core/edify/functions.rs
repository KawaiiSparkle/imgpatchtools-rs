//! Edify function registry and script runner.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use super::parser::{parse_edify, BinaryOperator, Expr};
use crate::core::super_img::op_list::DynamicPartitionState;
use crate::util::progress::{new_progress, ProgressReporter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    String(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn as_str(&self) -> &str {
        match self {
            Value::String(s) => s,
            Value::Blob(_) => "",
        }
    }
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::String(s) => !s.is_empty(),
            Value::Blob(b) => !b.is_empty(),
        }
    }
}

pub struct ScriptResult {
    pub value: Value,
    pub dynamic_partitions: Option<DynamicPartitionState>,
}

pub struct FunctionContext {
    pub current_function: String,
    pub progress: Box<dyn ProgressReporter>,
    pub workdir: String,
    pub dynamic_partitions: Option<DynamicPartitionState>,
    /// When true, getprop returns tolerant defaults instead of trying device props.
    /// Used by batch mode and when scripts reference compressed data files.
    pub offline_mode: bool,
    /// When true, skip all function calls except ui_print and block_image_update.
    /// Enabled when script contains compressed data files (.dat.br, .dat.lzma).
    pub skip_mode: bool,
    /// When true (default), only execute apply_patch, block_image_update and abort.
    /// When false (verify mode), execute all commands including verify and assertions.
    pub fast_mode: bool,
    /// Tracks extracted files: source filename -> first target partition name
    /// Used by package_extract_file to decide whether to rename (first time) or copy (subsequent)
    pub extracted_files: HashMap<String, String>,
}

impl FunctionContext {
    pub fn resolve_image_path(&self, device_path: &str) -> Option<PathBuf> {
        let d = Path::new(device_path);
        if d.exists() {
            return Some(d.to_path_buf());
        }

        let pn = Self::extract_partition_name(device_path);
        let img = Path::new(&self.workdir).join(format!("{pn}.img"));
        if img.exists() {
            return Some(img);
        }
        let raw = Path::new(&self.workdir).join(pn);
        if raw.exists() {
            return Some(raw);
        }
        None
    }

    pub fn resolve_or_create_image_path(&self, device_path: &str) -> PathBuf {
        if let Some(p) = self.resolve_image_path(device_path) {
            return p;
        }
        let pn = Self::extract_partition_name(device_path);
        Path::new(&self.workdir).join(format!("{pn}.img"))
    }

    pub fn extract_partition_name(device_path: &str) -> &str {
        let p = device_path.strip_prefix("EMMC:").unwrap_or(device_path);
        let n = if let Some(i) = p.rfind("/by-name/") {
            &p[i + 9..]
        } else if let Some(i) = p.rfind('/') {
            &p[i + 1..]
        } else {
            p
        };
        n.split(':').next().unwrap_or(n)
    }

    pub fn resolve_package_path(&self, path_in_package: &str) -> PathBuf {
        let p = Path::new(path_in_package);
        if p.is_absolute() || p.exists() {
            return p.to_path_buf();
        }
        Path::new(&self.workdir).join(path_in_package)
    }
}

pub type BuiltinFn = fn(&mut FunctionContext, &[Value]) -> Result<Value>;

pub struct FunctionRegistry {
    map: HashMap<String, BuiltinFn>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    pub fn register(&mut self, name: &str, f: BuiltinFn) {
        self.map.insert(name.to_string(), f);
    }
    pub fn get(&self, name: &str) -> Option<BuiltinFn> {
        self.map.get(name).copied()
    }
}

pub fn builtin_registry() -> FunctionRegistry {
    let mut r = FunctionRegistry::new();
    r.register("assert", fn_assert_fallback);
    r.register("abort", fn_abort);
    r.register("ui_print", fn_ui_print);
    r.register("show_progress", fn_noop);
    r.register("set_progress", fn_noop);
    r.register("getprop", fn_getprop);
    r.register("package_extract_file", fn_package_extract_file);
    r.register("apply_patch_check", fn_apply_patch_check);
    r.register("apply_patch", fn_apply_patch);
    r.register("apply_patch_space", fn_apply_patch_space);
    r.register("block_image_update", fn_block_image_update);
    r.register("block_image_verify", fn_block_image_verify);
    r.register("block_image_recover", fn_block_image_recover);
    r.register("range_sha1", fn_range_sha1);
    r.register("check_first_block", fn_check_first_block);
    r.register("map_partition", fn_map_partition);
    r.register("unmap_partition", fn_unmap_partition);
    r.register("update_dynamic_partitions", fn_update_dynamic_partitions);
    r.register("ifelse", fn_ifelse_fallback);
    r.register("equal", fn_equal);
    r.register("concat", fn_concat);

    // --- Integer comparison / arithmetic predicates ---
    r.register("less_than_int", fn_less_than_int);
    r.register("greater_than_int", fn_greater_than_int);
    r.register("max_of", fn_max_of);
    // Negation variants (the ! is part of the function name in edify)
    r.register("!less_than_int", fn_not_less_than_int);
    r.register("!greater_than_int", fn_not_greater_than_int);

    // --- String predicates ---
    r.register("not_equal", fn_not_equal);
    r.register("!", fn_not);
    r.register("matches", fn_matches);
    r.register("regex_match", fn_matches);

    // --- File / device helpers (offline-friendly stubs) ---
    r.register("mount", fn_noop_true);
    r.register("unmount", fn_noop_true);
    r.register("is_mounted", fn_is_mounted);
    r.register("format", fn_noop_true);
    r.register("wipe_cache", fn_noop_true);
    r.register("sleep", fn_noop_true);
    r.register("delete", fn_delete);
    r.register("rename", fn_noop_true);
    r.register("tune2fs", fn_noop_true);

    // --- Property / file reading ---
    r.register("file_getprop", fn_file_getprop);
    r.register("read_file", fn_read_file);

    // --- Image writing ---
    r.register("write_raw_image", fn_write_raw_image);

    // --- SHA1 / verification ---
    r.register("sha1_check", fn_sha1_check);
    r.register("verify_trustzone", fn_noop_true);
    r.register("allow_reboot", fn_noop_true);

    r
}

fn fn_noop(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String(String::new()))
}

fn fn_getprop(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let prop = args.first().map_or("", |a| a.as_str());

    // Try to read from build.prop in workdir (if available).
    if let Some(val) = lookup_build_prop(prop, &ctx.workdir) {
        return Ok(Value::String(val));
    }

    // Known safe defaults for common OTA properties.
    if let Some(val) = known_default(prop) {
        log::warn!(
            "[WARN] getprop(\"{}\") → returning default \"{}\" (no build.prop available)",
            prop,
            val
        );
        return Ok(Value::String(val));
    }

    // In offline mode, return empty string with warning (never abort).
    if ctx.offline_mode {
        log::warn!(
            "[WARN] getprop(\"{}\") → returning empty string (offline mode)",
            prop
        );
        return Ok(Value::String(String::new()));
    }

    // Default: return empty string (historical behaviour).
    log::warn!(
        "[WARN] getprop(\"{}\") → property not available, returning empty string",
        prop
    );
    Ok(Value::String(String::new()))
}

/// Attempt to read a property from build.prop in the workdir.
fn lookup_build_prop(prop: &str, workdir: &str) -> Option<String> {
    let build_prop = std::path::Path::new(workdir).join("build.prop");
    if !build_prop.exists() {
        return None;
    }
    let content = std::fs::read_to_string(&build_prop).ok()?;
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(eq_pos) = line.find('=') {
            let key = &line[..eq_pos];
            if key == prop {
                return Some(line[eq_pos + 1..].trim().to_string());
            }
        }
    }
    None
}

/// Known default values for commonly queried properties in OTA scripts.
fn known_default(prop: &str) -> Option<String> {
    match prop {
        "ro.product.device" | "ro.build.product" | "ro.hardware" => Some("generic".to_string()),
        "ro.build.display.id" => Some("generic".to_string()),
        "ro.build.version.sdk" => Some("30".to_string()),
        "ro.build.version.release" => Some("13".to_string()),
        "ro.build.type" => Some("user".to_string()),
        "ro.debuggable" => Some("0".to_string()),
        "ro.secure" => Some("1".to_string()),
        "ro.system.product.device" => Some("generic".to_string()),
        "ro.vendor.product.device" => Some("generic".to_string()),
        "ro.build.date.utc" | "ro.build.date" => Some("0".to_string()),
        _ => None,
    }
}

fn fn_package_extract_file(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("package_extract_file: need 1+ arg");
    }
    let src = args[0].as_str();
    let src_filename = Path::new(src)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| src.to_string());

    if args.len() == 1 {
        return Ok(Value::String(
            ctx.resolve_package_path(src).to_string_lossy().into(),
        ));
    }
    let dst = args[1].as_str();

    // Handle device paths (e.g., /dev/block/... or EMMC:...)
    // Extract partition name from the device path and add .img extension
    let partition_name = if dst.starts_with("/dev/") || dst.starts_with("EMMC:") {
        let name = FunctionContext::extract_partition_name(dst);
        format!("{}.img", name)
    } else {
        // For non-device paths, use the destination filename
        Path::new(dst)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| dst.to_string())
    };

    let dp = Path::new(&ctx.workdir).join(&partition_name);

    // Check if this source file has been extracted before
    let (sp, operation) = if let Some(first_target) = ctx.extracted_files.get(&src_filename) {
        // Source file already extracted - need to copy from the first target
        if first_target == &partition_name {
            // Same target, skip
            log::debug!("package_extract_file: {} already extracted to {}, skipping", src_filename, partition_name);
            return Ok(Value::String("t".into()));
        }
        // Different target - copy from the first extracted location
        let first_path = Path::new(&ctx.workdir).join(first_target);
        (first_path, "copy")
    } else {
        // First time seeing this source - resolve source path
        let workdir_path = Path::new(&ctx.workdir).join(&src_filename);
        let sp = if workdir_path.exists() {
            workdir_path
        } else {
            ctx.resolve_package_path(src)
        };
        
        // Skip if source and destination are the same file
        if sp == dp {
            log::debug!("package_extract_file: source and destination are the same, skipping");
            return Ok(Value::String("t".into()));
        }
        
        (sp, "rename")
    };

    if let Some(par) = dp.parent() {
        if !par.exists() {
            std::fs::create_dir_all(par)?;
        }
    }

    if operation == "rename" {
        // First extraction: rename the file
        log::info!("package_extract_file: renaming {} → {} (partition: {})", sp.display(), dp.display(), partition_name);
        std::fs::rename(&sp, &dp).with_context(|| format!("rename {} → {}", sp.display(), dp.display()))?;
        ctx.extracted_files.insert(src_filename.clone(), partition_name.clone());
    } else {
        // Subsequent extraction: copy from the first extracted location
        log::info!("package_extract_file: copying {} → {} (partition: {})", sp.display(), dp.display(), partition_name);
        std::fs::copy(&sp, &dp).with_context(|| format!("copy {} → {}", sp.display(), dp.display()))?;
    }

    Ok(Value::String("t".into()))
}

fn fn_ui_print(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let msg: String = args.iter().map(|a| a.as_str()).collect();
    println!("{msg}");
    Ok(Value::String(msg))
}

fn fn_map_partition(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("map_partition: need name");
    }
    Ok(Value::String(args[0].as_str().to_string()))
}

fn fn_unmap_partition(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String("t".into()))
}

fn fn_update_dynamic_partitions(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let path_str = args.first().map_or("", |a| a.as_str());
    let path = if Path::new(path_str).exists() {
        PathBuf::from(path_str)
    } else {
        ctx.resolve_package_path(path_str)
    };
    if !path.exists() {
        log::warn!(
            "update_dynamic_partitions: {} not found, skip",
            path.display()
        );
        return Ok(Value::String("t".into()));
    }
    let content = std::fs::read_to_string(&path)?;
    let state = crate::core::super_img::op_list::parse_op_list(&content)?;

    println!("\n--- Dynamic partition layout ---");
    for g in &state.groups {
        println!(
            "  group '{}': max={} ({:.1} GB)",
            g.name,
            g.max_size,
            g.max_size as f64 / 1e9
        );
    }
    for p in &state.partitions {
        println!(
            "  partition '{}': group='{}', size={} ({:.1} MB)",
            p.name,
            p.group_name,
            p.size,
            p.size as f64 / 1048576.0
        );
    }
    println!(
        "  total: {} ({:.1} GB)",
        state.total_size(),
        state.total_size() as f64 / 1e9
    );
    println!("---");

    ctx.dynamic_partitions = Some(state);
    Ok(Value::String("t".into()))
}

fn fn_apply_patch(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 5 {
        bail!("apply_patch: need 5+ args, got {}", args.len());
    }
    let source_spec = args[0].as_str();
    let target_spec = args[1].as_str();
    let target_sha1 = args[2].as_str();
    let target_size: u64 = args[3].as_str().parse().context("bad target_size")?;

    let source_path = ctx
        .resolve_image_path(source_spec)
        .ok_or_else(|| anyhow::anyhow!("cannot resolve source: {}", source_spec))?;
    let target_path = if target_spec == "-" {
        source_path.clone()
    } else {
        ctx.resolve_image_path(target_spec)
            .unwrap_or_else(|| PathBuf::from(target_spec))
    };

    let pairs = parse_sha1_patch_pairs(ctx, &args[4..])?;
    if pairs.is_empty() {
        bail!("no sha1:patch pairs");
    }

    let patch_path = if pairs.len() == 1 {
        pairs[0].1.clone()
    } else {
        let sd = std::fs::read(&source_path)?;
        let sh = crate::util::hash::sha1_hex(&sd);
        pairs
            .iter()
            .find(|(s, _)| s.eq_ignore_ascii_case(&sh))
            .map(|(_, p)| p.clone())
            .ok_or_else(|| anyhow::anyhow!("source SHA1 {} matches nothing", sh))?
    };

    log::info!(
        "apply_patch: {} → {} patch={}",
        source_path.display(),
        target_path.display(),
        patch_path.display()
    );
    crate::core::applypatch::apply::apply_patch(
        &source_path,
        &target_path,
        target_sha1,
        target_size,
        &patch_path,
    )?;
    Ok(Value::String("t".into()))
}

fn parse_sha1_patch_pairs(ctx: &FunctionContext, tail: &[Value]) -> Result<Vec<(String, PathBuf)>> {
    if tail.is_empty() {
        bail!("missing sha1:patch args");
    }
    let paired = tail.len() >= 2
        && tail.len() % 2 == 0
        && !(tail[1].as_str().len() == 40
            && tail[1].as_str().bytes().all(|b| b.is_ascii_hexdigit()));
    if paired {
        Ok(tail
            .chunks_exact(2)
            .map(|c| (c[0].as_str().into(), resolve_patch(ctx, c[1].as_str())))
            .collect())
    } else {
        tail.iter()
            .map(|a| {
                let s = a.as_str();
                if s.len() > 41 && s.as_bytes()[40] == b':' {
                    Ok((s[..40].into(), resolve_patch(ctx, &s[41..])))
                } else {
                    bail!("bad sha1:patch {:?}", s)
                }
            })
            .collect()
    }
}

fn resolve_patch(ctx: &FunctionContext, s: &str) -> PathBuf {
    if Path::new(s).exists() {
        PathBuf::from(s)
    } else {
        ctx.resolve_package_path(s)
    }
}

fn fn_apply_patch_check(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("apply_patch_check: need path");
    }
    let p = args[0].as_str();
    if p.starts_with("EMMC:") {
        return Ok(Value::String("t".into()));
    }
    Ok(Value::String(
        if Path::new(p).exists() || ctx.resolve_image_path(p).is_some() {
            "t"
        } else {
            ""
        }
        .into(),
    ))
}

fn fn_apply_patch_space(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String("t".into()))
}

fn fn_block_image_update(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 4 {
        bail!("block_image_update: need 4 args");
    }

    let part_name = FunctionContext::extract_partition_name(args[0].as_str());
    let tgt = ctx.resolve_or_create_image_path(args[0].as_str());
    let tl = ctx.resolve_package_path(args[1].as_str());
    let nd = ctx.resolve_package_path(args[2].as_str());
    let pd = ctx.resolve_package_path(args[3].as_str());

    println!("Patching {} image unconditionally...", part_name);

    crate::core::blockimg::update::block_image_update(
        &tgt,
        &tl,
        &nd,
        &pd,
        None,
        Path::new(&format!("{}/stash", ctx.workdir)),
        true,
        None,
    )?;

    cleanup_blockimg_files(&tl, &nd, &pd);
    Ok(Value::String("t".into()))
}

fn cleanup_blockimg_files(tl: &Path, nd: &Path, pd: &Path) {
    for path in &[tl, pd] {
        remove_if_exists(path);
    }
    remove_if_exists(nd);
    let br_path = PathBuf::from(format!("{}.br", nd.display()));
    remove_if_exists(&br_path);
}

fn remove_if_exists(path: &Path) {
    if path.exists() {
        match std::fs::remove_file(path) {
            Ok(()) => log::info!("cleanup: removed {}", path.display()),
            Err(e) => log::warn!("cleanup: failed to remove {}: {}", path.display(), e),
        }
    }
}

fn fn_block_image_verify(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("block_image_verify: need 2 args");
    }
    let tgt = match ctx.resolve_image_path(args[0].as_str()) {
        Some(p) => p,
        None => return Ok(Value::String(String::new())),
    };
    let tl = ctx.resolve_package_path(args[1].as_str());
    let ok = crate::core::blockimg::verify::block_image_verify(&tgt, &tl)?;
    Ok(Value::String(if ok { "t" } else { "" }.into()))
}

fn fn_block_image_recover(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::String(String::new()));
    }
    let resolved = match ctx.resolve_image_path(args[0].as_str()) {
        Some(p) => p,
        None => return Ok(Value::String(String::new())),
    };
    let ranges = match crate::util::rangeset::RangeSet::parse(args[1].as_str()) {
        Ok(r) => r,
        Err(_) => return Ok(Value::String(String::new())),
    };
    let bs: u64 = 4096;
    let max_blk = ranges.iter().map(|(_, e)| e).max().unwrap_or(0);
    let req = max_blk * bs;
    let flen = std::fs::metadata(&resolved).map(|m| m.len()).unwrap_or(0);
    if flen < req {
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&resolved) {
            let _ = f.set_len(req);
        }
    }
    match crate::core::blockimg::verify::range_sha1(&resolved, &ranges, bs as usize) {
        Ok(_) => Ok(Value::String("t".into())),
        Err(_) => Ok(Value::String(String::new())),
    }
}

fn fn_range_sha1(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("range_sha1: need 2 args");
    }
    let p = match ctx.resolve_image_path(args[0].as_str()) {
        Some(p) => p,
        None => return Ok(Value::String(String::new())),
    };
    Ok(Value::String(
        crate::core::blockimg::verify::range_sha1_str(&p, args[1].as_str(), 4096)?,
    ))
}

fn fn_check_first_block(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("check_first_block: need path");
    }
    let p = match ctx.resolve_image_path(args[0].as_str()) {
        Some(p) => p,
        None => return Ok(Value::String(String::new())),
    };
    Ok(Value::String(
        if crate::core::blockimg::verify::check_first_block(&p, 4096)? {
            "t"
        } else {
            ""
        }
        .into(),
    ))
}

fn fn_abort(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let msg = args.first().map_or("aborted", |v| v.as_str());
    if ctx.offline_mode {
        log::warn!("[OFFLINE] abort(\"{}\") → skipped (offline mode)", msg);
        return Ok(Value::String(String::new()));
    }
    bail!("abort: {}", msg);
}
fn fn_assert_fallback(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    for (i, a) in args.iter().enumerate() {
        if !a.is_truthy() {
            if ctx.offline_mode {
                log::warn!(
                    "[OFFLINE] assert failed on arg {} → skipped (offline mode)",
                    i
                );
                return Ok(Value::String(String::new()));
            }
            bail!("assert failed on argument {i}");
        }
    }
    Ok(Value::String("t".into()))
}
fn fn_concat(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    Ok(Value::String(args.iter().map(|a| a.as_str()).collect()))
}
fn fn_ifelse_fallback(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("ifelse: need 2+ args");
    }
    if args[0].is_truthy() {
        Ok(args[1].clone())
    } else {
        Ok(args.get(2).cloned().unwrap_or(Value::String(String::new())))
    }
}
fn fn_equal(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::String(String::new()));
    }
    Ok(Value::String(if args[0].as_str() == args[1].as_str() {
        "t".into()
    } else {
        String::new()
    }))
}

// ---------------------------------------------------------------------------
// Integer comparison / arithmetic predicates
// ---------------------------------------------------------------------------

/// Parse an integer from a string. Empty/invalid strings default to 0.
/// This is important for offline mode where getprop may return empty strings
/// — e.g. `!less_than_int(1773654484, getprop("ro.build.date.utc"))` should
/// not fail just because the property is unavailable.
fn parse_i64(s: &str) -> i64 {
    s.trim().parse::<i64>().unwrap_or(0)
}

fn fn_less_than_int(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("less_than_int: need 2 args");
    }
    let a = parse_i64(args[0].as_str());
    let b = parse_i64(args[1].as_str());
    Ok(Value::String(if a < b {
        "t".into()
    } else {
        String::new()
    }))
}

fn fn_not_less_than_int(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("!less_than_int: need 2 args");
    }
    let a = parse_i64(args[0].as_str());
    let b = parse_i64(args[1].as_str());
    Ok(Value::String(if a < b {
        String::new()
    } else {
        "t".into()
    }))
}

fn fn_greater_than_int(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("greater_than_int: need 2 args");
    }
    let a = parse_i64(args[0].as_str());
    let b = parse_i64(args[1].as_str());
    Ok(Value::String(if a > b {
        "t".into()
    } else {
        String::new()
    }))
}

fn fn_not_greater_than_int(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("!greater_than_int: need 2 args");
    }
    let a = parse_i64(args[0].as_str());
    let b = parse_i64(args[1].as_str());
    Ok(Value::String(if a > b {
        String::new()
    } else {
        "t".into()
    }))
}

fn fn_max_of(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("max_of: need 2 args");
    }
    let a = parse_i64(args[0].as_str());
    let b = parse_i64(args[1].as_str());
    Ok(Value::String(if a >= b {
        args[0].as_str().to_string()
    } else {
        args[1].as_str().to_string()
    }))
}

// ---------------------------------------------------------------------------
// String predicates
// ---------------------------------------------------------------------------

fn fn_not_equal(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        return Ok(Value::String(String::new()));
    }
    Ok(Value::String(if args[0].as_str() != args[1].as_str() {
        "t".into()
    } else {
        String::new()
    }))
}

fn fn_not(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let truthy = args.first().map_or(false, |a| a.is_truthy());
    Ok(Value::String(if truthy {
        String::new()
    } else {
        "t".into()
    }))
}

fn fn_matches(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("matches: need 2 args");
    }
    let s = args[0].as_str();
    let pattern = args[1].as_str();
    // AOSP matches() uses POSIX extended regex.
    match regex::Regex::new(pattern) {
        Ok(re) => Ok(Value::String(if re.is_match(s) {
            "t".into()
        } else {
            String::new()
        })),
        Err(_) => Ok(Value::String(String::new())),
    }
}

// ---------------------------------------------------------------------------
// File / device helpers (offline-friendly stubs)
// ---------------------------------------------------------------------------

fn fn_noop_true(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String("t".into()))
}

fn fn_is_mounted(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    // In offline/batch mode, always return empty (not mounted).
    let _path = args.first().map_or("", |a| a.as_str());
    log::warn!("[WARN] is_mounted({}) → \"\" (offline mode)", _path);
    Ok(Value::String(String::new()))
}

fn fn_delete(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("delete: need 1 arg");
    }
    let path = ctx.resolve_package_path(args[0].as_str());
    if path.exists() {
        match std::fs::remove_file(&path) {
            Ok(()) => log::info!("delete: removed {}", path.display()),
            Err(e) => log::warn!("delete: failed to remove {}: {}", path.display(), e),
        }
    }
    Ok(Value::String("t".into()))
}

// ---------------------------------------------------------------------------
// Property / file reading
// ---------------------------------------------------------------------------

fn fn_file_getprop(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("file_getprop: need 2 args (file, key)");
    }
    let file_path = ctx.resolve_package_path(args[0].as_str());
    let key = args[1].as_str();
    let content = match std::fs::read_to_string(&file_path) {
        Ok(c) => c,
        Err(_) => return Ok(Value::String(String::new())),
    };
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(eq_pos) = line.find('=') {
            if line[..eq_pos].trim() == key {
                return Ok(Value::String(line[eq_pos + 1..].trim().to_string()));
            }
        }
    }
    Ok(Value::String(String::new()))
}

fn fn_read_file(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("read_file: need 1 arg");
    }
    let path = ctx.resolve_package_path(args[0].as_str());
    match std::fs::read_to_string(&path) {
        Ok(content) => Ok(Value::String(content)),
        Err(_) => Ok(Value::String(String::new())),
    }
}

// ---------------------------------------------------------------------------
// Image writing
// ---------------------------------------------------------------------------

fn fn_write_raw_image(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("write_raw_image: need 2 args (src, dest)");
    }
    let src = ctx.resolve_package_path(args[0].as_str());
    let dest = ctx.resolve_or_create_image_path(args[1].as_str());
    if !src.exists() {
        log::warn!(
            "write_raw_image: source {} not found, skipping",
            src.display()
        );
        return Ok(Value::String("t".into()));
    }
    log::info!("write_raw_image: {} → {}", src.display(), dest.display());
    if let Some(parent) = dest.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    std::fs::copy(&src, &dest).with_context(|| {
        format!(
            "write_raw_image: copy {} → {}",
            src.display(),
            dest.display()
        )
    })?;
    Ok(Value::String("t".into()))
}

// ---------------------------------------------------------------------------
// SHA1 / verification
// ---------------------------------------------------------------------------

fn fn_sha1_check(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("sha1_check: need at least 1 arg");
    }
    let expected = args[0].as_str();
    // If only 1 arg, the hash of the data from the previous function is checked.
    if args.len() == 1 {
        // Single-arg form: sha1_check(expected_hash) — matches against
        // the sha1 of the most recently extracted data. In our offline context,
        // we can't know that, so return "t" to not block the script.
        log::warn!(
            "[WARN] sha1_check({}) → \"t\" (single-arg stub, offline mode)",
            expected
        );
        return Ok(Value::String("t".into()));
    }
    // Multi-arg form: sha1_check(expected, data, ...) — compare hashes.
    let data = args[1..]
        .iter()
        .map(|a| a.as_str())
        .collect::<Vec<_>>()
        .join("");
    let actual = crate::util::hash::sha1_hex(data.as_bytes());
    Ok(Value::String(if actual == expected {
        "t".into()
    } else {
        String::new()
    }))
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

pub fn run_script(
    script: &str,
    registry: &FunctionRegistry,
    workdir: &str,
) -> Result<ScriptResult> {
    run_script_offline(script, registry, workdir, false)
}

/// Run an edify script with fast mode or verify mode.
///
/// In fast mode (verify=false), only apply_patch, block_image_update and abort are executed.
/// In verify mode (verify=true), all commands including block_image_verify and assertions are executed.
pub fn run_script_with_mode(
    script: &str,
    registry: &FunctionRegistry,
    workdir: &str,
    verify: bool,
) -> Result<ScriptResult> {
    let has_compressed_data = script.contains(".dat.br") || script.contains(".dat.lzma");
    let effective_offline = has_compressed_data;

    if effective_offline {
        log::info!("edify: offline mode enabled (getprop will return tolerant defaults)");
    }
    if !verify {
        log::info!("edify: fast mode enabled (only apply_patch, block_image_update and abort will execute)");
    } else {
        log::info!("edify: verify mode enabled (all commands will execute)");
    }

    let ast = parse_edify(script).context("edify parse error")?;
    let mut ctx = FunctionContext {
        current_function: String::new(),
        progress: new_progress(false),
        workdir: workdir.to_string(),
        dynamic_partitions: None,
        offline_mode: effective_offline,
        skip_mode: has_compressed_data,
        fast_mode: !verify,
        extracted_files: HashMap::new(),
    };
    let value = eval(&ast, &mut ctx, registry)?;
    Ok(ScriptResult {
        value,
        dynamic_partitions: ctx.dynamic_partitions,
    })
}

/// Run an edify script with optional offline mode.
///
/// In offline mode, `getprop` returns tolerant defaults and never aborts.
/// This is used by batch processing where no real device properties are available.
///
/// When the script contains compressed data files (.dat.br, .dat.lzma), skip mode
/// is enabled which skips all function calls except ui_print and block_image_update.
pub fn run_script_offline(
    script: &str,
    registry: &FunctionRegistry,
    workdir: &str,
    offline_mode: bool,
) -> Result<ScriptResult> {
    // Pre-scan: detect if the script references compressed data files.
    // If so, automatically enable offline mode for getprop tolerance and skip mode.
    let has_compressed_data = script.contains(".dat.br") || script.contains(".dat.lzma");
    let effective_offline = offline_mode || has_compressed_data;

    if effective_offline {
        log::info!("edify: offline mode enabled (getprop will return tolerant defaults)");
    }
    if has_compressed_data {
        log::info!("edify: skip mode enabled (only ui_print and block_image_update will execute)");
    }

    let ast = parse_edify(script).context("edify parse error")?;
    let mut ctx = FunctionContext {
        current_function: String::new(),
        progress: new_progress(false),
        workdir: workdir.to_string(),
        dynamic_partitions: None,
        offline_mode: effective_offline,
        skip_mode: has_compressed_data,
        fast_mode: false, // Backward compatibility: run all commands
        extracted_files: HashMap::new(),
    };
    let value = eval(&ast, &mut ctx, registry)?;
    Ok(ScriptResult {
        value,
        dynamic_partitions: ctx.dynamic_partitions,
    })
}

fn eval(expr: &Expr, ctx: &mut FunctionContext, reg: &FunctionRegistry) -> Result<Value> {
    match expr {
        Expr::StringLiteral(s) => Ok(Value::String(s.clone())),
        Expr::FunctionCall { name, args } => {
            if name == "ifelse" {
                return eval_ifelse(args, ctx, reg);
            }
            if name == "assert" {
                return eval_assert(args, ctx, reg);
            }
            // Skip mode: allow ui_print, block_image_update, and package_extract_file
            // (package_extract_file is needed by block_image_update to resolve paths)
            if ctx.skip_mode && name != "ui_print" && name != "block_image_update" && name != "package_extract_file" {
                log::debug!("edify: skip_mode skipping function '{}'", name);
                return Ok(Value::String(String::new()));
            }
            // Fast mode: only execute apply_patch, block_image_update, abort and package_extract_file
            // (package_extract_file is needed by block_image_update to resolve file paths)
            if ctx.fast_mode && name != "apply_patch" && name != "block_image_update" && name != "abort" && name != "package_extract_file" {
                log::debug!("edify: fast_mode skipping function '{}'", name);
                return Ok(Value::String(String::new()));
            }

            ctx.current_function = name.clone();

            // Show simplified log message for main operations
            let start = std::time::Instant::now();
            let log_msg = get_simplified_log(name, args);
            let has_log = log_msg.is_some();
            if let Some(ref msg) = log_msg {
                log::info!("[{}] {}", name, msg);
            }

            let vals: Vec<Value> = args
                .iter()
                .map(|a| eval(a, ctx, reg))
                .collect::<Result<_>>()?;
            let f = reg
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("unknown function: {name}"))?;
            let result = f(ctx, &vals);

            // Log completion time for main operations
            if has_log {
                let elapsed = start.elapsed();
                log::info!("[{}] completed in {:.2}s", name, elapsed.as_secs_f64());
            }

            result
        }
        Expr::Sequence(es) => {
            let mut last = Value::String(String::new());
            for e in es {
                last = eval(e, ctx, reg)?;
            }
            Ok(last)
        }
        Expr::If {
            condition,
            then,
            else_,
        } => {
            if eval(condition, ctx, reg)?.is_truthy() {
                eval(then, ctx, reg)
            } else if let Some(e) = else_ {
                eval(e, ctx, reg)
            } else {
                Ok(Value::String(String::new()))
            }
        }
        Expr::BinaryOp { op, lhs, rhs } => match op {
            BinaryOperator::Or => {
                let l = eval(lhs, ctx, reg)?;
                if l.is_truthy() {
                    Ok(l)
                } else {
                    eval(rhs, ctx, reg)
                }
            }
            BinaryOperator::And => {
                let l = eval(lhs, ctx, reg)?;
                if !l.is_truthy() {
                    Ok(Value::String(String::new()))
                } else {
                    eval(rhs, ctx, reg)
                }
            }
            BinaryOperator::Eq => {
                let l = eval(lhs, ctx, reg)?;
                let r = eval(rhs, ctx, reg)?;
                Ok(Value::String(if l.as_str() == r.as_str() {
                    "t".into()
                } else {
                    String::new()
                }))
            }
            BinaryOperator::Add => {
                let l = eval(lhs, ctx, reg)?;
                let r = eval(rhs, ctx, reg)?;
                Ok(Value::String(format!("{}{}", l.as_str(), r.as_str())))
            }
        },
    }
}

/// Generate simplified log message for main operations.
/// Returns None if no simplified message should be shown.
fn get_simplified_log(name: &str, args: &[Expr]) -> Option<String> {
    match name {
        "apply_patch" => {
            // apply_patch(device_path, source_hash, target_hash, target_size, patch_path)
            // Show "Updating Boot Image" based on device path
            if let Some(Expr::StringLiteral(device_path)) = args.first() {
                let partition = extract_partition_name_from_path(device_path);
                let display_name = format_partition_display(partition);
                Some(format!("Updating {} Image", display_name))
            } else {
                Some("Updating Image".to_string())
            }
        }
        "block_image_update" => {
            // block_image_update(device_path, transfer_list, new_data, patch_data)
            // Show "Updating system Partition"
            if let Some(Expr::StringLiteral(device_path)) = args.first() {
                let partition = extract_partition_name_from_path(device_path);
                let display_name = format_partition_display(partition);
                Some(format!("Updating {} Partition", display_name))
            } else {
                Some("Updating Partition".to_string())
            }
        }
        _ => None,
    }
}

/// Extract partition name from device path like "/dev/block/.../by-name/boot" or "EMMC:/dev/.../boot:..."
fn extract_partition_name_from_path(path: &str) -> &str {
    let p = path.strip_prefix("EMMC:").unwrap_or(path);
    let n = if let Some(i) = p.rfind("/by-name/") {
        &p[i + 9..]
    } else if let Some(i) = p.rfind('/') {
        &p[i + 1..]
    } else {
        p
    };
    n.split(':').next().unwrap_or(n)
}

/// Format partition name for display (capitalize first letter, handle special cases)
fn format_partition_display(name: &str) -> String {
    let mut result = String::new();
    let mut capitalize_next = true;
    for c in name.chars() {
        if c == '_' || c == '-' {
            result.push(' ');
            capitalize_next = true;
        } else if capitalize_next {
            result.push(c.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(c);
        }
    }
    // Handle special cases
    match result.to_lowercase().as_str() {
        s if s.contains("boot") && !s.contains("bootloader") => "Boot".to_string(),
        s if s.contains("system") => "System".to_string(),
        s if s.contains("vendor") => "Vendor".to_string(),
        s if s.contains("product") => "Product".to_string(),
        s if s.contains("recovery") => "Recovery".to_string(),
        s if s.contains("userdata") || s == "data" => "Data".to_string(),
        s if s.contains("cache") => "Cache".to_string(),
        _ => result,
    }
}

fn eval_ifelse(args: &[Expr], ctx: &mut FunctionContext, reg: &FunctionRegistry) -> Result<Value> {
    if args.len() < 2 {
        bail!("ifelse: need 2+ args");
    }
    if eval(&args[0], ctx, reg)?.is_truthy() {
        eval(&args[1], ctx, reg)
    } else if args.len() > 2 {
        eval(&args[2], ctx, reg)
    } else {
        Ok(Value::String(String::new()))
    }
}

fn eval_assert(args: &[Expr], ctx: &mut FunctionContext, reg: &FunctionRegistry) -> Result<Value> {
    for (i, a) in args.iter().enumerate() {
        if !eval(a, ctx, reg)?.is_truthy() {
            if ctx.offline_mode {
                log::warn!(
                    "[OFFLINE] assert failed on argument {} → skipped (offline mode)",
                    i
                );
                return Ok(Value::String(String::new()));
            }
            bail!("assert failed on argument {i}");
        }
    }
    Ok(Value::String("t".into()))
}
