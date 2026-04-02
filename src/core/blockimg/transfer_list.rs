use crate::util::rangeset::RangeSet;
use anyhow::{bail, ensure, Context, Result};

pub const MIN_VERSION: u32 = 1;
pub const MAX_VERSION: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandType {
    Zero,
    New,
    Erase,
    Move,
    Bsdiff,
    Imgdiff,
    Stash,
    Free,
}

impl CommandType {
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "zero" => Ok(Self::Zero),
            "new" => Ok(Self::New),
            "erase" => Ok(Self::Erase),
            "move" => Ok(Self::Move),
            "bsdiff" => Ok(Self::Bsdiff),
            "imgdiff" => Ok(Self::Imgdiff),
            "stash" => Ok(Self::Stash),
            "free" => Ok(Self::Free),
            other => bail!("unknown command: {other:?}"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::New => "new",
            Self::Erase => "erase",
            Self::Move => "move",
            Self::Bsdiff => "bsdiff",
            Self::Imgdiff => "imgdiff",
            Self::Stash => "stash",
            Self::Free => "free",
        }
    }

    pub fn has_source(&self) -> bool {
        matches!(self, Self::Move | Self::Bsdiff | Self::Imgdiff)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferListHeader {
    pub version: u32,
    pub total_blocks: u64,
    pub stash_max_entries: u32,
    pub stash_max_blocks: u32,
}

#[derive(Debug, Clone)]
pub struct TransferCommand {
    pub cmd_type: CommandType,
    pub target_ranges: Option<RangeSet>,
    pub src_block_count: Option<u64>,
    pub src_ranges: Option<RangeSet>,
    pub src_buffer_map: Option<RangeSet>,
    pub stash_id: Option<String>,
    pub patch_offset: Option<u64>,
    pub patch_len: Option<u64>,
    pub target_hash: Option<String>,
    pub src_hash: Option<String>,
    /// stash refs: (stash_id, ranges_in_source_buffer)
    pub src_stash_refs: Vec<(String, RangeSet)>,
    pub raw_line: String,
}

#[derive(Debug, Clone)]
pub struct TransferList {
    pub header: TransferListHeader,
    pub commands: Vec<TransferCommand>,
}

impl TransferList {
    pub fn len(&self) -> usize {
        self.commands.len()
    }
    pub fn version(&self) -> u32 {
        self.header.version
    }
    pub fn total_blocks(&self) -> u64 {
        self.header.total_blocks
    }
}

pub fn parse_transfer_list(content: &str) -> Result<TransferList> {
    let lines: Vec<&str> = content
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();

    let header = parse_header(&lines)?;
    let cmd_start = if header.version >= 2 { 4 } else { 2 };

    ensure!(
        lines.len() >= cmd_start,
        "transfer list has only {} lines, expected at least {cmd_start}",
        lines.len()
    );

    let mut commands = Vec::with_capacity(lines.len() - cmd_start);
    for (idx, &line) in lines[cmd_start..].iter().enumerate() {
        let cmd = parse_command(line, header.version).with_context(|| {
            format!("line {} (command #{}): {line:?}", cmd_start + idx + 1, idx)
        })?;
        commands.push(cmd);
    }

    Ok(TransferList { header, commands })
}

fn parse_header(lines: &[&str]) -> Result<TransferListHeader> {
    ensure!(!lines.is_empty(), "transfer list is empty");
    ensure!(lines.len() >= 2, "transfer list missing total_blocks line");

    let version: u32 = lines[0]
        .parse()
        .with_context(|| format!("bad version: {:?}", lines[0]))?;
    ensure!(
        version >= MIN_VERSION && version <= MAX_VERSION,
        "unsupported transfer-list version {version} (expected {MIN_VERSION}–{MAX_VERSION})"
    );

    let total_blocks: u64 = lines[1]
        .parse()
        .with_context(|| format!("bad total_blocks: {:?}", lines[1]))?;

    let (stash_max_entries, stash_max_blocks) = if version >= 2 {
        ensure!(
            lines.len() >= 4,
            "v{version} transfer list missing stash header lines"
        );
        let e: u32 = lines[2]
            .parse()
            .with_context(|| format!("bad stash_max_entries: {:?}", lines[2]))?;
        let b: u32 = lines[3]
            .parse()
            .with_context(|| format!("bad stash_max_blocks: {:?}", lines[3]))?;
        (e, b)
    } else {
        (0, 0)
    };

    Ok(TransferListHeader {
        version,
        total_blocks,
        stash_max_entries,
        stash_max_blocks,
    })
}

fn parse_command(line: &str, version: u32) -> Result<TransferCommand> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    ensure!(!tokens.is_empty(), "empty command line");

    let cmd_type = CommandType::parse(tokens[0])?;
    let mut pos = 1usize;

    match cmd_type {
        CommandType::Zero | CommandType::New | CommandType::Erase => {
            parse_simple_cmd(cmd_type, &tokens, &mut pos, line)
        }
        CommandType::Stash => parse_stash_cmd(&tokens, &mut pos, line, version),
        CommandType::Free => parse_free_cmd(&tokens, &mut pos, line, version),
        CommandType::Move => parse_move_cmd(&tokens, &mut pos, line, version),
        CommandType::Bsdiff | CommandType::Imgdiff => {
            parse_patch_cmd(cmd_type, &tokens, &mut pos, line, version)
        }
    }
}

fn parse_simple_cmd(
    cmd_type: CommandType,
    tokens: &[&str],
    pos: &mut usize,
    line: &str,
) -> Result<TransferCommand> {
    let tgt = read_rangeset(tokens, pos, "target_ranges")?;
    Ok(TransferCommand {
        cmd_type,
        target_ranges: Some(tgt),
        src_block_count: None,
        src_ranges: None,
        src_buffer_map: None,
        stash_id: None,
        patch_offset: None,
        patch_len: None,
        target_hash: None,
        src_hash: None,
        src_stash_refs: Vec::new(),
        raw_line: line.to_string(),
    })
}

fn parse_stash_cmd(
    tokens: &[&str],
    pos: &mut usize,
    line: &str,
    version: u32,
) -> Result<TransferCommand> {
    ensure!(version >= 2, "stash command not supported in v{version}");
    let id = read_token(tokens, pos, "stash_id")?.to_string();
    let src = read_rangeset(tokens, pos, "src_ranges")?;
    Ok(TransferCommand {
        cmd_type: CommandType::Stash,
        target_ranges: None,
        src_block_count: None,
        src_ranges: Some(src),
        src_buffer_map: None,
        stash_id: Some(id),
        patch_offset: None,
        patch_len: None,
        target_hash: None,
        src_hash: None,
        src_stash_refs: Vec::new(),
        raw_line: line.to_string(),
    })
}

fn parse_free_cmd(
    tokens: &[&str],
    pos: &mut usize,
    line: &str,
    version: u32,
) -> Result<TransferCommand> {
    ensure!(version >= 2, "free command not supported in v{version}");
    let id = read_token(tokens, pos, "stash_id")?.to_string();
    Ok(TransferCommand {
        cmd_type: CommandType::Free,
        target_ranges: None,
        src_block_count: None,
        src_ranges: None,
        src_buffer_map: None,
        stash_id: Some(id),
        patch_offset: None,
        patch_len: None,
        target_hash: None,
        src_hash: None,
        src_stash_refs: Vec::new(),
        raw_line: line.to_string(),
    })
}

fn looks_like_rangeset(tok: &str) -> bool {
    tok.contains(',') && tok.chars().all(|c| c.is_ascii_digit() || c == ',')
}
fn parse_move_cmd(
    tokens: &[&str],
    pos: &mut usize,
    line: &str,
    version: u32,
) -> Result<TransferCommand> {
    let mut target_hash: Option<String> = None;
    let mut src_hash: Option<String> = None;

    if version >= 4 {
        // AOSP LoadSrcTgtVersion3(onehash=true): 只有一个 hash，同时作为 src 和 tgt
        let h = read_token(tokens, pos, "hash")?.to_string();
        // 下一个 token 如果不是 rangeset，说明是双 hash 格式
        if *pos < tokens.len() && !looks_like_rangeset(tokens[*pos]) {
            src_hash = Some(h);
            target_hash = Some(read_token(tokens, pos, "tgt_hash")?.to_string());
        } else {
            src_hash = Some(h.clone());
            target_hash = Some(h);
        }
    } else if version >= 3 {
        src_hash = Some(read_token(tokens, pos, "src_hash")?.to_string());
    }

    if version == 1 {
        let tgt = read_rangeset(tokens, pos, "target_ranges")?;
        let src = read_rangeset(tokens, pos, "src_ranges")?;
        return Ok(TransferCommand {
            cmd_type: CommandType::Move,
            target_ranges: Some(tgt),
            src_block_count: None,
            src_ranges: Some(src),
            src_buffer_map: None,
            stash_id: None,
            patch_offset: None,
            patch_len: None,
            target_hash,
            src_hash,
            src_stash_refs: Vec::new(),
            raw_line: line.to_string(),
        });
    }

    let tgt = read_rangeset(tokens, pos, "target_ranges")?;
    let (nblk, src_ranges, src_buffer_map, stash_refs) = parse_source_spec(tokens, pos, version)?;

    Ok(TransferCommand {
        cmd_type: CommandType::Move,
        target_ranges: Some(tgt),
        src_block_count: nblk,
        src_ranges,
        src_buffer_map,
        stash_id: None,
        patch_offset: None,
        patch_len: None,
        target_hash,
        src_hash,
        src_stash_refs: stash_refs,
        raw_line: line.to_string(),
    })
}

fn parse_patch_cmd(
    cmd_type: CommandType,
    tokens: &[&str],
    pos: &mut usize,
    line: &str,
    version: u32,
) -> Result<TransferCommand> {
    let patch_offset = read_u64(tokens, pos, "patch_offset")?;
    let patch_len = read_u64(tokens, pos, "patch_len")?;

    let mut target_hash: Option<String> = None;
    let mut src_hash: Option<String> = None;

    if version >= 4 {
        // AOSP LoadSrcTgtVersion3(onehash=false): src_hash 在前，tgt_hash 在后
        src_hash = Some(read_token(tokens, pos, "src_hash")?.to_string());
        target_hash = Some(read_token(tokens, pos, "tgt_hash")?.to_string());
    } else if version >= 3 {
        src_hash = Some(read_token(tokens, pos, "src_hash")?.to_string());
    }
    if version == 1 {
        let tgt = read_rangeset(tokens, pos, "target_ranges")?;
        let src = read_rangeset(tokens, pos, "src_ranges")?;
        return Ok(TransferCommand {
            cmd_type,
            target_ranges: Some(tgt),
            src_block_count: None,
            src_ranges: Some(src),
            src_buffer_map: None,
            stash_id: None,
            patch_offset: Some(patch_offset),
            patch_len: Some(patch_len),
            target_hash,
            src_hash,
            src_stash_refs: Vec::new(),
            raw_line: line.to_string(),
        });
    }

    let tgt = read_rangeset(tokens, pos, "target_ranges")?;
    let (nblk, src_ranges, src_buffer_map, stash_refs) = parse_source_spec(tokens, pos, version)?;

    Ok(TransferCommand {
        cmd_type,
        target_ranges: Some(tgt),
        src_block_count: nblk,
        src_ranges,
        src_buffer_map,
        stash_id: None,
        patch_offset: Some(patch_offset),
        patch_len: Some(patch_len),
        target_hash,
        src_hash,
        src_stash_refs: stash_refs,
        raw_line: line.to_string(),
    })
}

/// Returns (src_block_count, src_ranges, stash_refs)
fn parse_source_spec(
    tokens: &[&str],
    pos: &mut usize,
    version: u32,
) -> Result<(
    Option<u64>,
    Option<RangeSet>,
    Option<RangeSet>,
    Vec<(String, RangeSet)>,
)> {
    if version == 1 {
        let src = read_rangeset(tokens, pos, "src_ranges")?;
        return Ok((None, Some(src), None, Vec::new()));
    }

    let nblk = read_u64(tokens, pos, "src_block_count")?;

    let src_token = read_token(tokens, pos, "src_ranges_or_dash")?;
    let src_ranges = if src_token == "-" {
        None
    } else {
        Some(RangeSet::parse(src_token).context("parsing direct source ranges")?)
    };

    // v2+ tail tokens may contain:
    // - an optional bare rangeset (buffer map for direct src_ranges)
    // - stash refs "id:ranges" (ranges are buffer positions)
    let mut src_buffer_map: Option<RangeSet> = None;
    let mut stash_refs = Vec::new();

    while *pos < tokens.len() {
        let tok = tokens[*pos];
        *pos += 1;

        if tok.contains(':') {
            let (id, rs) = parse_stash_ref(tok)?;
            stash_refs.push((id, rs));
        } else if looks_like_rangeset(tok) {
            // First bare rangeset is the buffer map. If multiple appear, merge them.
            let rs = RangeSet::parse(tok)
                .with_context(|| format!("bad buffer map rangeset: {tok:?}"))?;
            src_buffer_map = Some(match src_buffer_map {
                None => rs,
                Some(existing) => existing.merge(&rs),
            });
        } else {
            log::debug!("Skipping extra token in source spec: {}", tok);
        }
    }

    Ok((Some(nblk), src_ranges, src_buffer_map, stash_refs))
}

fn parse_stash_ref(token: &str) -> Result<(String, RangeSet)> {
    let colon = token
        .find(':')
        .ok_or_else(|| anyhow::anyhow!("stash ref missing ':' separator: {token:?}"))?;
    let id = &token[..colon];
    let range_str = &token[colon + 1..];
    ensure!(!id.is_empty(), "empty stash id in ref: {token:?}");
    ensure!(
        !range_str.is_empty(),
        "empty range set in stash ref: {token:?}"
    );
    let rs = RangeSet::parse(range_str)
        .with_context(|| format!("bad range set in stash ref: {token:?}"))?;
    Ok((id.to_string(), rs))
}

fn read_token<'a>(tokens: &[&'a str], pos: &mut usize, label: &str) -> Result<&'a str> {
    ensure!(
        *pos < tokens.len(),
        "unexpected end of tokens while reading {label} (pos={}, len={})",
        *pos,
        tokens.len()
    );
    let tok = tokens[*pos];
    *pos += 1;
    Ok(tok)
}

fn read_u64(tokens: &[&str], pos: &mut usize, label: &str) -> Result<u64> {
    let tok = read_token(tokens, pos, label)?;
    tok.parse::<u64>()
        .with_context(|| format!("{label}: expected u64, got {tok:?}"))
}

fn read_rangeset(tokens: &[&str], pos: &mut usize, label: &str) -> Result<RangeSet> {
    let tok = read_token(tokens, pos, label)?;
    RangeSet::parse(tok).with_context(|| format!("{label}: bad range set: {tok:?}"))
}
