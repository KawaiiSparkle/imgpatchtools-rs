//! Parser for `dynamic_partitions_op_list`.
//! Format stable from Android 10 through 16.

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone)]
pub struct GroupState {
    pub name: String,
    pub max_size: u64,
}

#[derive(Debug, Clone)]
pub struct PartitionState {
    pub name: String,
    pub group_name: String,
    pub size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DynamicPartitionState {
    pub groups: Vec<GroupState>,
    pub partitions: Vec<PartitionState>,
}

impl DynamicPartitionState {
    pub fn new() -> Self { Self::default() }

    pub fn total_size(&self) -> u64 {
        self.partitions.iter().map(|p| p.size).sum()
    }

    pub fn find_partition(&self, name: &str) -> Option<&PartitionState> {
        self.partitions.iter().find(|p| p.name == name)
    }

    pub fn find_group(&self, name: &str) -> Option<&GroupState> {
        self.groups.iter().find(|g| g.name == name)
    }
}

pub fn parse_op_list(content: &str) -> Result<DynamicPartitionState> {
    let mut s = DynamicPartitionState::new();

    for (ln, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let t: Vec<&str> = line.split_whitespace().collect();
        if t.is_empty() { continue; }

        let ctx = || format!("line {}: {:?}", ln + 1, line);

        match t[0] {
            "remove_all_groups" => {
                s.partitions.clear();
                s.groups.clear();
            }
            "add_group" => {
                if t.len() < 3 { bail!("add_group needs 2 args ({})", ctx()); }
                let max: u64 = t[2].parse().with_context(ctx)?;
                if s.find_group(t[1]).is_some() {
                    bail!("add_group: '{}' exists ({})", t[1], ctx());
                }
                s.groups.push(GroupState { name: t[1].into(), max_size: max });
            }
            "remove_group" => {
                if t.len() < 2 { bail!("remove_group needs 1 arg ({})", ctx()); }
                if s.partitions.iter().any(|p| p.group_name == t[1]) {
                    bail!("remove_group: '{}' not empty ({})", t[1], ctx());
                }
                let before = s.groups.len();
                s.groups.retain(|g| g.name != t[1]);
                if s.groups.len() == before {
                    bail!("remove_group: '{}' not found ({})", t[1], ctx());
                }
            }
            "resize_group" => {
                if t.len() < 3 { bail!("resize_group needs 2 args ({})", ctx()); }
                let max: u64 = t[2].parse().with_context(ctx)?;
                let g = s.groups.iter_mut().find(|g| g.name == t[1])
                    .ok_or_else(|| anyhow::anyhow!("resize_group: '{}' not found ({})", t[1], ctx()))?;
                g.max_size = max;
            }
            "add" => {
                if t.len() < 3 { bail!("add needs 2 args ({})", ctx()); }
                if s.find_group(t[2]).is_none() {
                    bail!("add: group '{}' not found ({})", t[2], ctx());
                }
                if s.find_partition(t[1]).is_some() {
                    bail!("add: '{}' exists ({})", t[1], ctx());
                }
                // AOSP: add <name> <group> [size] — size is optional
                let sz: u64 = if t.len() >= 4 {
                    t[3].parse().with_context(ctx)?
                } else {
                    0
                };
                s.partitions.push(PartitionState {
                    name: t[1].into(), group_name: t[2].into(), size: sz,
                });
            }
            "remove" => {
                if t.len() < 2 { bail!("remove needs 1 arg ({})", ctx()); }
                let before = s.partitions.len();
                s.partitions.retain(|p| p.name != t[1]);
                if s.partitions.len() == before {
                    bail!("remove: '{}' not found ({})", t[1], ctx());
                }
            }
            "resize" => {
                if t.len() < 3 { bail!("resize needs 2 args ({})", ctx()); }
                let sz: u64 = t[2].parse().with_context(ctx)?;
                let p = s.partitions.iter_mut().find(|p| p.name == t[1])
                    .ok_or_else(|| anyhow::anyhow!("resize: '{}' not found ({})", t[1], ctx()))?;
                p.size = sz;
            }
            "move" => {
                if t.len() < 3 { bail!("move needs 2 args ({})", ctx()); }
                if s.find_group(t[2]).is_none() {
                    bail!("move: group '{}' not found ({})", t[2], ctx());
                }
                let p = s.partitions.iter_mut().find(|p| p.name == t[1])
                    .ok_or_else(|| anyhow::anyhow!("move: '{}' not found ({})", t[1], ctx()))?;
                p.group_name = t[2].into();
            }
            other => bail!("unknown op '{}' ({})", other, ctx()),
        }
    }

    log::info!("op_list: {} groups, {} partitions, {} bytes total",
        s.groups.len(), s.partitions.len(), s.total_size());
    Ok(s)
}

