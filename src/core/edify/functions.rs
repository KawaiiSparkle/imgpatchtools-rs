//! Edify function registry and script runner.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use super::parser::{parse_edify, BinaryOperator, Expr};
use crate::util::progress::{new_progress, ProgressReporter};

// ---------------------------------------------------------------------------
// Value
// ---------------------------------------------------------------------------

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

    /// AOSP Edify truth semantics: empty string = false, everything else = true.
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::String(s) => !s.is_empty(),
            Value::Blob(b) => !b.is_empty(),
        }
    }
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

pub struct FunctionContext {
    pub current_function: String,
    pub progress: Box<dyn ProgressReporter>,
    pub workdir: String,
}

impl FunctionContext {
    /// Resolve Android device-ish paths to a local image path in workdir as fallback.
    pub fn resolve_image_path(&self, device_path: &str) -> Option<PathBuf> {
        // 1) Direct path (Android runtime)
        let direct_path = Path::new(device_path);
        if direct_path.exists() {
            return Some(direct_path.to_path_buf());
        }

        // 2) Strip prefix like "EMMC:" if present
        let device_path = device_path.strip_prefix("EMMC:").unwrap_or(device_path);

        // 3) Extract partition name: .../by-name/system -> system
        let partition_name = if let Some(idx) = device_path.rfind("/by-name/") {
            &device_path[idx + "/by-name/".len()..]
        } else if let Some(idx) = device_path.rfind('/') {
            &device_path[idx + 1..]
        } else {
            device_path
        };

        // 4) Remove any ":offset:..." suffixes if present
        let partition_name = partition_name.split(':').next().unwrap_or(partition_name);

        // 5) workdir/{partition}.img
        let img_path = Path::new(&self.workdir).join(format!("{partition_name}.img"));
        if img_path.exists() {
            return Some(img_path);
        }

        // 6) workdir/{partition}
        let raw_path = Path::new(&self.workdir).join(partition_name);
        if raw_path.exists() {
            return Some(raw_path);
        }

        None
    }

    /// Resolve a "package path" (inside OTA zip) to a local workdir path.
    /// In PC mode we assume files already exist under workdir.
    pub fn resolve_package_path(&self, path_in_package: &str) -> PathBuf {
        // If already absolute or exists as-is, keep it.
        let p = Path::new(path_in_package);
        if p.is_absolute() || p.exists() {
            return p.to_path_buf();
        }
        Path::new(&self.workdir).join(path_in_package)
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

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

/// Names of functions that require **lazy** (short-circuit) argument
/// evaluation.  Their arguments must NOT be pre-evaluated by the generic
/// `FunctionCall` branch in [`eval`]; instead, dedicated helpers handle
/// them.
pub fn builtin_registry() -> FunctionRegistry {
    let mut r = FunctionRegistry::new();
    // assert / abort — assert is handled lazily in eval(); abort is eager
    // (single arg, always evaluated).
    r.register("assert", fn_assert_fallback);
    r.register("abort", fn_abort);
    r.register("ui_print", fn_ui_print);
    r.register("show_progress", fn_show_progress);
    r.register("set_progress", fn_set_progress);
    r.register("getprop", fn_getprop);

    // Package
    r.register("package_extract_file", fn_package_extract_file);

    // Patch/blockimg related
    r.register("apply_patch_check", fn_apply_patch_check);
    r.register("apply_patch", fn_apply_patch);
    r.register("apply_patch_space", fn_apply_patch_space);
    r.register("block_image_update", fn_block_image_update);
    r.register("block_image_verify", fn_block_image_verify);
    r.register("block_image_recover", fn_block_image_recover);
    r.register("range_sha1", fn_range_sha1);
    r.register("check_first_block", fn_check_first_block);

    // Logic helpers used by scripts — ifelse is handled lazily in eval(),
    // this entry is a fallback that should never be reached.
    r.register("ifelse", fn_ifelse_fallback);
    r.register("equal", fn_equal);
    r.register("concat", fn_concat);

    r
}

// ---------------------------------------------------------------------------
// Critical functions for this script
// ---------------------------------------------------------------------------

fn fn_getprop(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let name = args.first().map_or("", |a| a.as_str());
    match name {
        "ro.product.device" => Ok(Value::String("IFLYTEKCB".to_string())),
        _ => Ok(Value::String(String::new())),
    }
}

fn fn_package_extract_file(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("package_extract_file: need at least 1 arg");
    }
    let src_in_pkg = args[0].as_str();

    // Single-arg form: return a path (used by block_image_* in your script)
    if args.len() == 1 {
        let p = ctx.resolve_package_path(src_in_pkg);
        return Ok(Value::String(p.to_string_lossy().into_owned()));
    }

    // Two-arg form: extract/copy to destination (best-effort stub)
    let dst = args[1].as_str();
    let src_path = ctx.resolve_package_path(src_in_pkg);
    let dst_path = Path::new(dst);

    std::fs::copy(&src_path, dst_path).with_context(|| {
        format!(
            "package_extract_file: copy {} -> {}",
            src_path.display(),
            dst_path.display()
        )
    })?;
    Ok(Value::String("t".into()))
}

fn fn_ui_print(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let msg: String = args.iter().map(|a| a.as_str()).collect();
    println!("{msg}");
    log::info!("ui_print: {msg}");
    Ok(Value::String(msg))
}

fn fn_show_progress(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String(String::new()))
}

fn fn_set_progress(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String(String::new()))
}

// ---------------------------------------------------------------------------
// apply_patch — AOSP semantics
// ---------------------------------------------------------------------------
//
// AOSP calling convention (from bootable/recovery/updater/install.cpp):
//
//   apply_patch(source, target, tgt_sha1, tgt_size,
//               sha1_1, patch_1 [, sha1_2, patch_2, ...])
//
// - source:    "EMMC:<path>:<size>:<sha1>[:<size>:<sha1>]..."
// - target:    path, or "-" meaning write back to source
// - tgt_sha1:  expected SHA-1 of the patched output
// - tgt_size:  expected byte length of the patched output
// - sha1_N:    SHA-1 of a known source state
// - patch_N:   corresponding patch file (or path from package_extract_file)
//
// There can be multiple (sha1, patch) pairs when the OTA supports
// patching from different source states. The implementation reads the
// source file, computes its SHA-1, and picks the matching pair.
//
// An alternative convention concatenates "sha1:/path" with `+`:
//   apply_patch(source, target, tgt_sha1, tgt_size, "sha1:/path")
// We support both forms.
// ---------------------------------------------------------------------------

fn fn_apply_patch(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 5 {
        bail!("apply_patch: need at least 5 args, got {}", args.len());
    }

    let source_spec = args[0].as_str();
    let target_spec = args[1].as_str();
    let target_sha1 = args[2].as_str();
    let target_size: u64 = args[3]
        .as_str()
        .parse()
        .context("apply_patch: bad target_size")?;

    // ------------------------------------------------------------------
    // 1. Resolve source path
    // ------------------------------------------------------------------
    let source_path = ctx.resolve_image_path(source_spec).ok_or_else(|| {
        anyhow::anyhow!("apply_patch: cannot resolve source path: {}", source_spec)
    })?;

    // ------------------------------------------------------------------
    // 2. Resolve target path ("-" means write back to source)
    // ------------------------------------------------------------------
    let target_path = if target_spec == "-" {
        source_path.clone()
    } else {
        ctx.resolve_image_path(target_spec)
            .unwrap_or_else(|| PathBuf::from(target_spec))
    };

    // ------------------------------------------------------------------
    // 3. Parse (sha1, patch_path) pairs from args[4..]
    // ------------------------------------------------------------------
    let tail = &args[4..];
    let pairs: Vec<(String, PathBuf)> = parse_sha1_patch_pairs(ctx, tail)?;

    if pairs.is_empty() {
        bail!("apply_patch: no sha1:patch pairs found");
    }

    // ------------------------------------------------------------------
    // 4. Select the correct patch by matching source SHA-1
    // ------------------------------------------------------------------
    let patch_path = if pairs.len() == 1 {
        // Common case: single pair, use it directly.
        log::info!(
            "apply_patch: single patch pair, sha1={}, patch={}",
            pairs[0].0,
            pairs[0].1.display()
        );
        pairs[0].1.clone()
    } else {
        // Multiple pairs: compute source SHA-1 and find match.
        let source_data = std::fs::read(&source_path).with_context(|| {
            format!("apply_patch: cannot read source {}", source_path.display())
        })?;
        let source_sha1 = crate::util::hash::sha1_hex(&source_data);

        let matched = pairs
            .iter()
            .find(|(sha1, _)| sha1.eq_ignore_ascii_case(&source_sha1));

        match matched {
            Some((sha1, path)) => {
                log::info!(
                    "apply_patch: matched source sha1 {} → patch {}",
                    sha1,
                    path.display()
                );
                path.clone()
            }
            None => {
                let available: Vec<&str> = pairs.iter().map(|(s, _)| s.as_str()).collect();
                bail!(
                    "apply_patch: source SHA1 {} does not match any provided: {:?}",
                    source_sha1,
                    available
                );
            }
        }
    };

    log::info!(
        "apply_patch: source={} target={} patch={} target_sha1={} target_size={}",
        source_path.display(),
        target_path.display(),
        patch_path.display(),
        target_sha1,
        target_size
    );

    // ------------------------------------------------------------------
    // 5. Call the core apply logic
    // ------------------------------------------------------------------
    crate::core::applypatch::apply::apply_patch(
        &source_path,
        &target_path,
        target_sha1,
        target_size,
        &patch_path,
    )?;

    Ok(Value::String("t".into()))
}

/// Parse the tail of `apply_patch` arguments into `(sha1, patch_path)` pairs.
///
/// Supports two AOSP formats:
///
/// **Format A** — paired args (even count, `tail[1]` is not a SHA-1):
///   `[sha1, path, sha1, path, ...]`
///
/// **Format B** — colon-joined (each arg is `"<40-hex-sha1>:<path>"`):
///   `["sha1:/path/to/patch", ...]`
fn parse_sha1_patch_pairs(
    ctx: &FunctionContext,
    tail: &[Value],
) -> Result<Vec<(String, PathBuf)>> {
    if tail.is_empty() {
        bail!("apply_patch: missing sha1:patch arguments");
    }

    // Heuristic: if we have an even number of args >= 2, and the second
    // arg does NOT look like a 40-char hex SHA-1, assume paired mode.
    let use_paired = tail.len() >= 2
        && tail.len() % 2 == 0
        && !looks_like_sha1(tail[1].as_str());

    if use_paired {
        // Format A: [sha1, patch, sha1, patch, ...]
        let mut pairs = Vec::new();
        for chunk in tail.chunks_exact(2) {
            let sha1 = chunk[0].as_str().to_string();
            let patch_str = chunk[1].as_str();
            let patch_path = resolve_patch_path(ctx, patch_str);
            pairs.push((sha1, patch_path));
        }
        Ok(pairs)
    } else {
        // Format B: ["sha1:/path/to/patch", ...]
        let mut pairs = Vec::new();
        for arg in tail {
            let s = arg.as_str();
            // SHA-1 is always 40 hex chars; split at the 41st byte (':').
            if s.len() > 41 && s.as_bytes()[40] == b':' {
                let sha1 = s[..40].to_string();
                let patch_str = &s[41..];
                let patch_path = resolve_patch_path(ctx, patch_str);
                pairs.push((sha1, patch_path));
            } else {
                bail!(
                    "apply_patch: cannot parse sha1:patch from {:?} \
                     (expected 40-char hex + ':' + path)",
                    s
                );
            }
        }
        Ok(pairs)
    }
}

/// Returns `true` if `s` looks like a 40-character hex SHA-1 digest.
fn looks_like_sha1(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Resolve a patch path string: if it already exists on disk use it as-is,
/// otherwise resolve relative to `workdir`.
fn resolve_patch_path(ctx: &FunctionContext, patch_str: &str) -> PathBuf {
    if Path::new(patch_str).exists() {
        PathBuf::from(patch_str)
    } else {
        ctx.resolve_package_path(patch_str)
    }
}

fn fn_apply_patch_check(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("apply_patch_check: need path");
    }
    let path = args[0].as_str();

    // For PC mode, this is typically an "EMMC:..." pseudo path; assume OK.
    if path.starts_with("EMMC:") {
        return Ok(Value::String("t".into()));
    }

    // Or try resolve fallback.
    if Path::new(path).exists() || ctx.resolve_image_path(path).is_some() {
        return Ok(Value::String("t".into()));
    }

    Ok(Value::String(String::new()))
}

fn fn_apply_patch_space(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let _size = args
        .first()
        .map_or(0u64, |a| a.as_str().parse::<u64>().unwrap_or(0));
    Ok(Value::String("t".into()))
}

fn fn_block_image_update(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 4 {
        bail!("block_image_update: need 4 args");
    }

    let target_path = args[0].as_str();
    let transfer_list = args[1].as_str();
    let new_data = args[2].as_str();
    let patch_data = args[3].as_str();

    let resolved_target = ctx
        .resolve_image_path(target_path)
        .unwrap_or_else(|| PathBuf::from(target_path));

    // Resolve new_data and patch_data relative to workdir
    let resolved_tl = ctx.resolve_package_path(transfer_list);
    let resolved_new_data = ctx.resolve_package_path(new_data);
    let resolved_patch_data = ctx.resolve_package_path(patch_data);

    crate::core::blockimg::update::block_image_update(
        &resolved_target,
        &resolved_tl,
        &resolved_new_data,
        &resolved_patch_data,
        None,
        Path::new(&format!("{}/stash", ctx.workdir)),
        false,
        None,
    )?;

    Ok(Value::String("t".into()))
}

fn fn_block_image_verify(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("block_image_verify: need 2 args");
    }

    let target_path = args[0].as_str();
    let tl_path = args[1].as_str();

    let resolved_target = ctx
        .resolve_image_path(target_path)
        .unwrap_or_else(|| PathBuf::from(target_path));

    let resolved_tl = ctx.resolve_package_path(tl_path);

    let ok = crate::core::blockimg::verify::block_image_verify(
        &resolved_target,
        &resolved_tl,
    )?;
    Ok(Value::String(if ok { "t" } else { "" }.into()))
}

fn fn_block_image_recover(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    log::warn!("block_image_recover: not supported in PC mode");
    // Return empty string = false (recovery not available on PC).
    Ok(Value::String(String::new()))
}

fn fn_range_sha1(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("range_sha1: need 2 args");
    }

    let device_path = args[0].as_str();
    let ranges = args[1].as_str();

    let resolved_path = ctx
        .resolve_image_path(device_path)
        .unwrap_or_else(|| PathBuf::from(device_path));

    let h = crate::core::blockimg::verify::range_sha1_str(&resolved_path, ranges, 4096)?;
    Ok(Value::String(h))
}

fn fn_check_first_block(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() {
        bail!("check_first_block: need path");
    }

    let device_path = args[0].as_str();
    let resolved_path = ctx
        .resolve_image_path(device_path)
        .unwrap_or_else(|| PathBuf::from(device_path));

    let ok = crate::core::blockimg::verify::check_first_block(&resolved_path, 4096)?;
    Ok(Value::String(if ok { "t" } else { "" }.into()))
}

fn fn_abort(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let msg = args.first().map_or("aborted", |v| v.as_str());
    bail!("abort: {msg}");
}

/// Fallback for `assert` when called via the registry (should not happen;
/// the lazy path in `eval` handles it).
fn fn_assert_fallback(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    for (i, arg) in args.iter().enumerate() {
        if !arg.is_truthy() {
            bail!("assert failed on argument {i}");
        }
    }
    Ok(Value::String("t".into()))
}

fn fn_concat(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let mut s = String::new();
    for a in args {
        s.push_str(a.as_str());
    }
    Ok(Value::String(s))
}

/// Fallback for `ifelse` when called via the registry (should not happen;
/// the lazy path in `eval` handles it).
fn fn_ifelse_fallback(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 {
        bail!("ifelse needs at least 2 args");
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
    Ok(Value::String(
        if args[0].as_str() == args[1].as_str() {
            "t".into()
        } else {
            String::new()
        },
    ))
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

pub fn run_script(script: &str, registry: &FunctionRegistry, workdir: &str) -> Result<Value> {
    let ast = parse_edify(script).context("edify parse error")?;
    let mut ctx = FunctionContext {
        current_function: String::new(),
        progress: new_progress(false),
        workdir: workdir.to_string(),
    };
    eval(&ast, &mut ctx, registry)
}

fn eval(expr: &Expr, ctx: &mut FunctionContext, registry: &FunctionRegistry) -> Result<Value> {
    match expr {
        Expr::StringLiteral(s) => Ok(Value::String(s.clone())),

        Expr::FunctionCall { name, args } => {
            // -------------------------------------------------------
            // Lazy-evaluated built-ins: ifelse, assert
            // These must NOT eagerly evaluate all arguments.
            // -------------------------------------------------------
            if name == "ifelse" {
                return eval_ifelse(args, ctx, registry);
            }
            if name == "assert" {
                return eval_assert(args, ctx, registry);
            }

            // -------------------------------------------------------
            // Normal (eager) function call: evaluate all args first.
            // -------------------------------------------------------
            ctx.current_function = name.clone();
            let arg_values: Vec<Value> = args
                .iter()
                .map(|a| eval(a, ctx, registry))
                .collect::<Result<_>>()?;
            let f = registry
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("unknown function: {name}"))?;
            f(ctx, &arg_values)
        }

        Expr::Sequence(exprs) => {
            let mut last = Value::String(String::new());
            for e in exprs {
                last = eval(e, ctx, registry)?;
            }
            Ok(last)
        }

        Expr::If { condition, then, else_ } => {
            let cond = eval(condition, ctx, registry)?;
            if cond.is_truthy() {
                eval(then, ctx, registry)
            } else if let Some(e) = else_ {
                eval(e, ctx, registry)
            } else {
                Ok(Value::String(String::new()))
            }
        }

        Expr::BinaryOp { op, lhs, rhs } => match op {
            BinaryOperator::Or => {
                let l = eval(lhs, ctx, registry)?;
                if l.is_truthy() {
                    Ok(l)
                } else {
                    eval(rhs, ctx, registry)
                }
            }
            BinaryOperator::And => {
                let l = eval(lhs, ctx, registry)?;
                if !l.is_truthy() {
                    Ok(Value::String(String::new()))
                } else {
                    eval(rhs, ctx, registry)
                }
            }
            BinaryOperator::Eq => {
                let l = eval(lhs, ctx, registry)?;
                let r = eval(rhs, ctx, registry)?;
                Ok(Value::String(if l.as_str() == r.as_str() {
                    "t".into()
                } else {
                    String::new()
                }))
            }
            BinaryOperator::Add => {
                let l = eval(lhs, ctx, registry)?;
                let r = eval(rhs, ctx, registry)?;
                Ok(Value::String(format!("{}{}", l.as_str(), r.as_str())))
            }
        },
    }
}

// ---------------------------------------------------------------------------
// Lazy-evaluated built-ins
// ---------------------------------------------------------------------------

/// `ifelse(condition, then_expr [, else_expr])`
///
/// Evaluate `condition` first. If truthy, evaluate and return `then_expr`;
/// otherwise evaluate and return `else_expr` (or empty string if absent).
///
/// **Only the selected branch is evaluated** — this matches AOSP Edify
/// semantics where `ifelse` is a special form, not an eager function.
fn eval_ifelse(
    args: &[Expr],
    ctx: &mut FunctionContext,
    registry: &FunctionRegistry,
) -> Result<Value> {
    if args.len() < 2 {
        bail!("ifelse: need at least 2 arguments (condition, then [, else])");
    }

    let cond = eval(&args[0], ctx, registry)?;

    if cond.is_truthy() {
        eval(&args[1], ctx, registry)
    } else if args.len() > 2 {
        eval(&args[2], ctx, registry)
    } else {
        Ok(Value::String(String::new()))
    }
}

/// `assert(expr1, expr2, ...)`
///
/// Evaluate each argument in order. If any evaluates to a falsy value,
/// immediately bail with an error — subsequent arguments are **not**
/// evaluated. This matches AOSP behaviour where `assert` short-circuits.
fn eval_assert(
    args: &[Expr],
    ctx: &mut FunctionContext,
    registry: &FunctionRegistry,
) -> Result<Value> {
    for (i, arg_expr) in args.iter().enumerate() {
        let val = eval(arg_expr, ctx, registry)?;
        if !val.is_truthy() {
            bail!("assert failed on argument {i}");
        }
    }
    Ok(Value::String("t".into()))
}