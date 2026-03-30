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
        match self { Value::String(s) => s, Value::Blob(_) => "" }
    }
    pub fn is_truthy(&self) -> bool {
        match self { Value::String(s) => !s.is_empty(), Value::Blob(b) => !b.is_empty() }
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
}

impl FunctionContext {
    pub fn resolve_image_path(&self, device_path: &str) -> Option<PathBuf> {
        let d = Path::new(device_path);
        if d.exists() { return Some(d.to_path_buf()); }

        let pn = Self::extract_partition_name(device_path);
        let img = Path::new(&self.workdir).join(format!("{pn}.img"));
        if img.exists() { return Some(img); }
        let raw = Path::new(&self.workdir).join(pn);
        if raw.exists() { return Some(raw); }
        None
    }

    pub fn resolve_or_create_image_path(&self, device_path: &str) -> PathBuf {
        if let Some(p) = self.resolve_image_path(device_path) { return p; }
        let pn = Self::extract_partition_name(device_path);
        Path::new(&self.workdir).join(format!("{pn}.img"))
    }

    fn extract_partition_name(device_path: &str) -> &str {
        let p = device_path.strip_prefix("EMMC:").unwrap_or(device_path);
        let n = if let Some(i) = p.rfind("/by-name/") { &p[i + 9..] }
                else if let Some(i) = p.rfind('/') { &p[i + 1..] }
                else { p };
        n.split(':').next().unwrap_or(n)
    }

    pub fn resolve_package_path(&self, path_in_package: &str) -> PathBuf {
        let p = Path::new(path_in_package);
        if p.is_absolute() || p.exists() { return p.to_path_buf(); }
        Path::new(&self.workdir).join(path_in_package)
    }
}

pub type BuiltinFn = fn(&mut FunctionContext, &[Value]) -> Result<Value>;

pub struct FunctionRegistry { map: HashMap<String, BuiltinFn> }

impl FunctionRegistry {
    pub fn new() -> Self { Self { map: HashMap::new() } }
    pub fn register(&mut self, name: &str, f: BuiltinFn) { self.map.insert(name.to_string(), f); }
    pub fn get(&self, name: &str) -> Option<BuiltinFn> { self.map.get(name).copied() }
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
    r
}

fn fn_noop(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String(String::new()))
}

fn fn_getprop(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let n = args.first().map_or("", |a| a.as_str());
    Ok(Value::String(match n { "ro.product.device" => "IFLYTEKCB".into(), _ => String::new() }))
}

fn fn_package_extract_file(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() { bail!("package_extract_file: need 1+ arg"); }
    let src = args[0].as_str();
    if args.len() == 1 {
        return Ok(Value::String(ctx.resolve_package_path(src).to_string_lossy().into()));
    }
    let dst = args[1].as_str();
    let sp = ctx.resolve_package_path(src);
    let dp = if Path::new(dst).is_absolute() && !Path::new(dst).exists() {
        ctx.resolve_or_create_image_path(dst)
    } else if Path::new(dst).exists() { PathBuf::from(dst) }
      else { ctx.resolve_package_path(dst) };
    if let Some(par) = dp.parent() { if !par.exists() { std::fs::create_dir_all(par)?; } }
    log::info!("package_extract_file: {} → {}", sp.display(), dp.display());
    std::fs::copy(&sp, &dp).with_context(|| format!("copy {} → {}", sp.display(), dp.display()))?;
    Ok(Value::String("t".into()))
}

fn fn_ui_print(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let msg: String = args.iter().map(|a| a.as_str()).collect();
    println!("{msg}");
    Ok(Value::String(msg))
}

fn fn_map_partition(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() { bail!("map_partition: need name"); }
    Ok(Value::String(args[0].as_str().to_string()))
}

fn fn_unmap_partition(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String("t".into()))
}

fn fn_update_dynamic_partitions(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    let path_str = args.first().map_or("", |a| a.as_str());
    let path = if Path::new(path_str).exists() { PathBuf::from(path_str) }
               else { ctx.resolve_package_path(path_str) };
    if !path.exists() {
        log::warn!("update_dynamic_partitions: {} not found, skip", path.display());
        return Ok(Value::String("t".into()));
    }
    let content = std::fs::read_to_string(&path)?;
    let state = crate::core::super_img::op_list::parse_op_list(&content)?;

    println!("\n--- Dynamic partition layout ---");
    for g in &state.groups {
        println!("  group '{}': max={} ({:.1} GB)", g.name, g.max_size, g.max_size as f64 / 1e9);
    }
    for p in &state.partitions {
        println!("  partition '{}': group='{}', size={} ({:.1} MB)",
            p.name, p.group_name, p.size, p.size as f64 / 1048576.0);
    }
    println!("  total: {} ({:.1} GB)", state.total_size(), state.total_size() as f64 / 1e9);
    println!("---");

    ctx.dynamic_partitions = Some(state);
    Ok(Value::String("t".into()))
}

fn fn_apply_patch(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 5 { bail!("apply_patch: need 5+ args, got {}", args.len()); }
    let source_spec = args[0].as_str();
    let target_spec = args[1].as_str();
    let target_sha1 = args[2].as_str();
    let target_size: u64 = args[3].as_str().parse().context("bad target_size")?;

    let source_path = ctx.resolve_image_path(source_spec)
        .ok_or_else(|| anyhow::anyhow!("cannot resolve source: {}", source_spec))?;
    let target_path = if target_spec == "-" { source_path.clone() }
        else { ctx.resolve_image_path(target_spec).unwrap_or_else(|| PathBuf::from(target_spec)) };

    let pairs = parse_sha1_patch_pairs(ctx, &args[4..])?;
    if pairs.is_empty() { bail!("no sha1:patch pairs"); }

    let patch_path = if pairs.len() == 1 { pairs[0].1.clone() } else {
        let sd = std::fs::read(&source_path)?;
        let sh = crate::util::hash::sha1_hex(&sd);
        pairs.iter().find(|(s,_)| s.eq_ignore_ascii_case(&sh))
            .map(|(_,p)| p.clone())
            .ok_or_else(|| anyhow::anyhow!("source SHA1 {} matches nothing", sh))?
    };

    log::info!("apply_patch: {} → {} patch={}", source_path.display(), target_path.display(), patch_path.display());
    crate::core::applypatch::apply::apply_patch(&source_path, &target_path, target_sha1, target_size, &patch_path)?;
    Ok(Value::String("t".into()))
}

fn parse_sha1_patch_pairs(ctx: &FunctionContext, tail: &[Value]) -> Result<Vec<(String, PathBuf)>> {
    if tail.is_empty() { bail!("missing sha1:patch args"); }
    let paired = tail.len() >= 2 && tail.len() % 2 == 0
        && !(tail[1].as_str().len() == 40 && tail[1].as_str().bytes().all(|b| b.is_ascii_hexdigit()));
    if paired {
        Ok(tail.chunks_exact(2).map(|c| {
            (c[0].as_str().into(), resolve_patch(ctx, c[1].as_str()))
        }).collect())
    } else {
        tail.iter().map(|a| {
            let s = a.as_str();
            if s.len() > 41 && s.as_bytes()[40] == b':' {
                Ok((s[..40].into(), resolve_patch(ctx, &s[41..])))
            } else { bail!("bad sha1:patch {:?}", s) }
        }).collect()
    }
}

fn resolve_patch(ctx: &FunctionContext, s: &str) -> PathBuf {
    if Path::new(s).exists() { PathBuf::from(s) } else { ctx.resolve_package_path(s) }
}

fn fn_apply_patch_check(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() { bail!("apply_patch_check: need path"); }
    let p = args[0].as_str();
    if p.starts_with("EMMC:") { return Ok(Value::String("t".into())); }
    Ok(Value::String(if Path::new(p).exists() || ctx.resolve_image_path(p).is_some() { "t" } else { "" }.into()))
}

fn fn_apply_patch_space(_ctx: &mut FunctionContext, _args: &[Value]) -> Result<Value> {
    Ok(Value::String("t".into()))
}

fn fn_block_image_update(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 4 { bail!("block_image_update: need 4 args"); }

    let part_name = FunctionContext::extract_partition_name(args[0].as_str());
    let tgt = ctx.resolve_or_create_image_path(args[0].as_str());
    let tl = ctx.resolve_package_path(args[1].as_str());
    let nd = ctx.resolve_package_path(args[2].as_str());
    let pd = ctx.resolve_package_path(args[3].as_str());

    println!("Patching {} image unconditionally...", part_name);

    crate::core::blockimg::update::block_image_update(
        &tgt, &tl, &nd, &pd, None,
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
    if args.len() < 2 { bail!("block_image_verify: need 2 args"); }
    let tgt = match ctx.resolve_image_path(args[0].as_str()) { Some(p) => p, None => return Ok(Value::String(String::new())) };
    let tl = ctx.resolve_package_path(args[1].as_str());
    let ok = crate::core::blockimg::verify::block_image_verify(&tgt, &tl)?;
    Ok(Value::String(if ok { "t" } else { "" }.into()))
}

fn fn_block_image_recover(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 { return Ok(Value::String(String::new())); }
    let resolved = match ctx.resolve_image_path(args[0].as_str()) { Some(p) => p, None => return Ok(Value::String(String::new())) };
    let ranges = match crate::util::rangeset::RangeSet::parse(args[1].as_str()) { Ok(r) => r, Err(_) => return Ok(Value::String(String::new())) };
    let bs: u64 = 4096;
    let max_blk = ranges.iter().map(|(_,e)| e).max().unwrap_or(0);
    let req = max_blk * bs;
    let flen = std::fs::metadata(&resolved).map(|m| m.len()).unwrap_or(0);
    if flen < req {
        if let Ok(f) = std::fs::OpenOptions::new().write(true).open(&resolved) { let _ = f.set_len(req); }
    }
    match crate::core::blockimg::verify::range_sha1(&resolved, &ranges, bs as usize) {
        Ok(_) => Ok(Value::String("t".into())),
        Err(_) => Ok(Value::String(String::new())),
    }
}

fn fn_range_sha1(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 { bail!("range_sha1: need 2 args"); }
    let p = match ctx.resolve_image_path(args[0].as_str()) { Some(p) => p, None => return Ok(Value::String(String::new())) };
    Ok(Value::String(crate::core::blockimg::verify::range_sha1_str(&p, args[1].as_str(), 4096)?))
}

fn fn_check_first_block(ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.is_empty() { bail!("check_first_block: need path"); }
    let p = match ctx.resolve_image_path(args[0].as_str()) { Some(p) => p, None => return Ok(Value::String(String::new())) };
    Ok(Value::String(if crate::core::blockimg::verify::check_first_block(&p, 4096)? { "t" } else { "" }.into()))
}

fn fn_abort(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    bail!("abort: {}", args.first().map_or("aborted", |v| v.as_str()));
}
fn fn_assert_fallback(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    for (i, a) in args.iter().enumerate() { if !a.is_truthy() { bail!("assert failed on arg {i}"); } }
    Ok(Value::String("t".into()))
}
fn fn_concat(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    Ok(Value::String(args.iter().map(|a| a.as_str()).collect()))
}
fn fn_ifelse_fallback(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 { bail!("ifelse: need 2+ args"); }
    if args[0].is_truthy() { Ok(args[1].clone()) }
    else { Ok(args.get(2).cloned().unwrap_or(Value::String(String::new()))) }
}
fn fn_equal(_ctx: &mut FunctionContext, args: &[Value]) -> Result<Value> {
    if args.len() < 2 { return Ok(Value::String(String::new())); }
    Ok(Value::String(if args[0].as_str() == args[1].as_str() { "t".into() } else { String::new() }))
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

pub fn run_script(script: &str, registry: &FunctionRegistry, workdir: &str) -> Result<ScriptResult> {
    let ast = parse_edify(script).context("edify parse error")?;
    let mut ctx = FunctionContext {
        current_function: String::new(),
        progress: new_progress(false),
        workdir: workdir.to_string(),
        dynamic_partitions: None,
    };
    let value = eval(&ast, &mut ctx, registry)?;
    Ok(ScriptResult { value, dynamic_partitions: ctx.dynamic_partitions })
}

fn eval(expr: &Expr, ctx: &mut FunctionContext, reg: &FunctionRegistry) -> Result<Value> {
    match expr {
        Expr::StringLiteral(s) => Ok(Value::String(s.clone())),
        Expr::FunctionCall { name, args } => {
            if name == "ifelse" { return eval_ifelse(args, ctx, reg); }
            if name == "assert" { return eval_assert(args, ctx, reg); }
            ctx.current_function = name.clone();
            let vals: Vec<Value> = args.iter().map(|a| eval(a, ctx, reg)).collect::<Result<_>>()?;
            let f = reg.get(name).ok_or_else(|| anyhow::anyhow!("unknown function: {name}"))?;
            f(ctx, &vals)
        }
        Expr::Sequence(es) => {
            let mut last = Value::String(String::new());
            for e in es { last = eval(e, ctx, reg)?; }
            Ok(last)
        }
        Expr::If { condition, then, else_ } => {
            if eval(condition, ctx, reg)?.is_truthy() { eval(then, ctx, reg) }
            else if let Some(e) = else_ { eval(e, ctx, reg) }
            else { Ok(Value::String(String::new())) }
        }
        Expr::BinaryOp { op, lhs, rhs } => match op {
            BinaryOperator::Or => { let l = eval(lhs, ctx, reg)?; if l.is_truthy() { Ok(l) } else { eval(rhs, ctx, reg) } }
            BinaryOperator::And => { let l = eval(lhs, ctx, reg)?; if !l.is_truthy() { Ok(Value::String(String::new())) } else { eval(rhs, ctx, reg) } }
            BinaryOperator::Eq => { let l = eval(lhs, ctx, reg)?; let r = eval(rhs, ctx, reg)?; Ok(Value::String(if l.as_str() == r.as_str() { "t".into() } else { String::new() })) }
            BinaryOperator::Add => { let l = eval(lhs, ctx, reg)?; let r = eval(rhs, ctx, reg)?; Ok(Value::String(format!("{}{}", l.as_str(), r.as_str()))) }
        },
    }
}

fn eval_ifelse(args: &[Expr], ctx: &mut FunctionContext, reg: &FunctionRegistry) -> Result<Value> {
    if args.len() < 2 { bail!("ifelse: need 2+ args"); }
    if eval(&args[0], ctx, reg)?.is_truthy() { eval(&args[1], ctx, reg) }
    else if args.len() > 2 { eval(&args[2], ctx, reg) }
    else { Ok(Value::String(String::new())) }
}

fn eval_assert(args: &[Expr], ctx: &mut FunctionContext, reg: &FunctionRegistry) -> Result<Value> {
    for (i, a) in args.iter().enumerate() {
        if !eval(a, ctx, reg)?.is_truthy() { bail!("assert failed on argument {i}"); }
    }
    Ok(Value::String("t".into()))
}