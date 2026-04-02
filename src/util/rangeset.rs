//! Block-range set implementation — complete port of AOSP `rangeset.h` / `rangeset.cpp`.
//!
//! A [`RangeSet`] represents an ordered, non-overlapping collection of
//! half-open `[start, end)` block ranges, exactly matching the format used
//! in AOSP transfer lists and the block-image updater.
//!
//! # Wire format
//!
//! The transfer-list text format is:
//! ```text
//! count,start1,end1,start2,end2,...
//! ```
//! where `count` is the number of individual integers that follow (always even),
//! and each `(startN, endN)` pair denotes the half-open range `[startN, endN)`.

use std::fmt;

use anyhow::{ensure, Context, Result};

// ---------------------------------------------------------------------------
// Range — a single half-open [start, end) pair
// ---------------------------------------------------------------------------

/// A single half-open block range `[start, end)`.
///
/// Invariant: `start < end` (enforced at construction).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Range {
    /// First block in the range (inclusive).
    pub start: u64,
    /// One-past-the-last block (exclusive).
    pub end: u64,
}

impl Range {
    /// Create a new range, returning an error if `start >= end`.
    pub fn new(start: u64, end: u64) -> Result<Self> {
        ensure!(
            start < end,
            "invalid range: start ({start}) must be less than end ({end})"
        );
        Ok(Self { start, end })
    }

    /// Number of blocks in this range.
    #[inline]
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    /// Whether this range is empty (always `false` by invariant, but
    /// provided for completeness).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// Returns `true` if `self` and `other` share at least one block.
    #[inline]
    pub fn overlaps(&self, other: &Range) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Returns `true` if `block` is contained in `[start, end)`.
    #[inline]
    pub fn contains(&self, block: u64) -> bool {
        block >= self.start && block < self.end
    }
}

// ---------------------------------------------------------------------------
// RangeSet
// ---------------------------------------------------------------------------

/// An ordered, non-overlapping set of half-open block ranges.
///
/// Mirrors the AOSP `RangeSet` class from `otautil/rangeset.h`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RangeSet {
    ranges: Vec<Range>,
}

// ---- Construction ---------------------------------------------------------

impl RangeSet {
    /// Create an empty `RangeSet`.
    #[inline]
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Create a `RangeSet` from an already-sorted, non-overlapping vector of
    /// ranges. No validation is performed — the caller **must** guarantee the
    /// invariants.
    ///
    /// This is an internal fast-path used by set-operation helpers.
    fn from_raw(ranges: Vec<Range>) -> Self {
        Self { ranges }
    }

    /// Parse the AOSP transfer-list range format.
    ///
    /// Format: `"count,s1,e1,s2,e2,..."` where `count` is the number of
    /// individual integers that follow (must be even).
    ///
    /// # Errors
    ///
    /// Returns an error if the string is malformed, the count is odd, or any
    /// range has `start >= end`.
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        ensure!(!s.is_empty(), "empty range string");

        let tokens: Vec<&str> = s.split(',').collect();
        ensure!(!tokens.is_empty(), "empty range token list");

        let count: usize = tokens[0].parse().context("invalid range count token")?;

        ensure!(count % 2 == 0, "range count must be even, got {count}");
        ensure!(
            tokens.len() == count + 1,
            "expected {} tokens after count, found {}",
            count,
            tokens.len() - 1
        );

        let num_ranges = count / 2;
        let mut ranges = Vec::with_capacity(num_ranges);

        for i in 0..num_ranges {
            let start: u64 = tokens[1 + 2 * i]
                .parse()
                .with_context(|| format!("invalid range start at pair {i}"))?;
            let end: u64 = tokens[2 + 2 * i]
                .parse()
                .with_context(|| format!("invalid range end at pair {i}"))?;
            let r = Range::new(start, end).with_context(|| format!("invalid range at pair {i}"))?;
            ranges.push(r);
        }

        let rs = Self { ranges };
        rs.validate()?;
        Ok(rs)
    }

    /// Build a `RangeSet` from a slice of `(start, end)` tuples.
    ///
    /// The pairs must be sorted and non-overlapping.
    pub fn from_pairs(pairs: &[(u64, u64)]) -> Result<Self> {
        let mut ranges = Vec::with_capacity(pairs.len());
        for &(s, e) in pairs {
            ranges.push(Range::new(s, e)?);
        }
        let rs = Self { ranges };
        rs.validate()?;
        Ok(rs)
    }

    /// Create a `RangeSet` containing the single contiguous range `[start, end)`.
    ///
    /// Returns an empty `RangeSet` if `start >= end`.
    ///
    /// This is a convenience constructor used by
    /// [`crate::core::blockimg::update::initialise_target_from_source`] to
    /// express "all blocks from 0 to N".
    pub fn from_range(start: u64, end: u64) -> Self {
        if start >= end {
            return Self::new();
        }
        // Range::new cannot fail here because start < end is guaranteed.
        Self {
            ranges: vec![Range { start, end }],
        }
    }

    /// Validate that ranges are sorted and non-overlapping.
    fn validate(&self) -> Result<()> {
        for i in 1..self.ranges.len() {
            let prev = &self.ranges[i - 1];
            let curr = &self.ranges[i];
            ensure!(
                prev.end <= curr.start,
                "ranges are not sorted or overlap: [{}, {}) and [{}, {})",
                prev.start,
                prev.end,
                curr.start,
                curr.end,
            );
        }
        Ok(())
    }
}

// ---- Accessors ------------------------------------------------------------

impl RangeSet {
    /// Total number of blocks across all ranges.
    ///
    /// Corresponds to AOSP `RangeSet::blocks()`.
    pub fn blocks(&self) -> u64 {
        self.ranges.iter().map(|r| r.len()).sum()
    }

    /// Returns `true` if the set contains zero ranges.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Number of contiguous range pairs.
    #[inline]
    pub fn range_count(&self) -> usize {
        self.ranges.len()
    }

    /// Access the underlying range pairs as a slice.
    #[inline]
    pub fn ranges(&self) -> &[Range] {
        &self.ranges
    }

    /// Returns `true` if `self` and `other` share at least one block.
    ///
    /// Corresponds to AOSP `RangeSet::Overlaps()`.
    /// Both sets are sorted, so we use a merge-style O(n+m) scan.
    pub fn overlaps(&self, other: &RangeSet) -> bool {
        let (mut i, mut j) = (0, 0);
        while i < self.ranges.len() && j < other.ranges.len() {
            let a = &self.ranges[i];
            let b = &other.ranges[j];
            if a.overlaps(b) {
                return true;
            }
            if a.end <= b.start {
                i += 1;
            } else {
                j += 1;
            }
        }
        false
    }

    /// Returns `true` if the given `block` falls within any range.
    ///
    /// Uses binary search for O(log n).
    ///
    /// Corresponds to AOSP `RangeSet::Contains(size_t block)`.
    pub fn contains(&self, block: u64) -> bool {
        self.ranges
            .binary_search_by(|r| {
                if block < r.start {
                    std::cmp::Ordering::Greater
                } else if block >= r.end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    /// Map a linear index (0-based across all blocks in the set) to the
    /// actual block number.
    ///
    /// Corresponds to AOSP `RangeSet::GetBlockNumber()`.
    ///
    /// Returns `None` if `idx >= self.blocks()`.
    pub fn get_block_number(&self, idx: u64) -> Option<u64> {
        let mut remaining = idx;
        for r in &self.ranges {
            let len = r.len();
            if remaining < len {
                return Some(r.start + remaining);
            }
            remaining -= len;
        }
        None
    }

    /// Extract a contiguous sub-portion of the linear block sequence.
    ///
    /// Starting from linear offset `start` (0-based), extract `len` blocks
    /// and return the resulting `RangeSet`.
    ///
    /// Corresponds to AOSP `RangeSet::GetSubRanges()`.
    ///
    /// # Panics
    ///
    /// Returns an empty `RangeSet` if the requested window exceeds bounds
    /// (matching AOSP behaviour of clamping).
    pub fn get_sub_ranges(&self, start: u64, len: u64) -> RangeSet {
        if len == 0 {
            return RangeSet::new();
        }
        let mut result = Vec::new();
        let mut remaining_skip = start;
        let mut remaining_take = len;

        for r in &self.ranges {
            if remaining_take == 0 {
                break;
            }
            let range_len = r.len();

            // Still skipping?
            if remaining_skip >= range_len {
                remaining_skip -= range_len;
                continue;
            }

            let effective_start = r.start + remaining_skip;
            let available = range_len - remaining_skip;
            remaining_skip = 0;

            let take = remaining_take.min(available);
            // Safety: take > 0 because remaining_take > 0 and available > 0.
            // effective_start + take <= r.end, so range invariant holds.
            result.push(Range {
                start: effective_start,
                end: effective_start + take,
            });
            remaining_take -= take;
        }
        RangeSet::from_raw(result)
    }

    /// Split the range set into pieces, each containing at most `limit`
    /// blocks.
    ///
    /// Corresponds to AOSP `RangeSet::Split()`.
    pub fn split(&self, limit: u64) -> Vec<RangeSet> {
        if limit == 0 || self.is_empty() {
            return vec![self.clone()];
        }
        let total = self.blocks();
        if total <= limit {
            return vec![self.clone()];
        }

        let mut result = Vec::new();
        let mut offset: u64 = 0;
        while offset < total {
            let chunk_len = limit.min(total - offset);
            result.push(self.get_sub_ranges(offset, chunk_len));
            offset += chunk_len;
        }
        result
    }
}

// ---- Set operations -------------------------------------------------------

impl RangeSet {
    /// Compute the union of `self` and `other`.
    ///
    /// Overlapping or adjacent ranges are coalesced.
    ///
    /// Corresponds to AOSP `RangeSet operator+(const RangeSet&)` / merge
    /// semantics. The AOSP `Merge` function in `blockimg.cpp` context merges
    /// two sorted range sets into a union.
    pub fn merge(&self, other: &RangeSet) -> RangeSet {
        if self.is_empty() {
            return other.clone();
        }
        if other.is_empty() {
            return self.clone();
        }

        // Merge-sort both sorted sequences, then coalesce.
        let mut all = Vec::with_capacity(self.ranges.len() + other.ranges.len());
        let (mut i, mut j) = (0, 0);
        while i < self.ranges.len() && j < other.ranges.len() {
            if self.ranges[i].start <= other.ranges[j].start {
                all.push(self.ranges[i]);
                i += 1;
            } else {
                all.push(other.ranges[j]);
                j += 1;
            }
        }
        while i < self.ranges.len() {
            all.push(self.ranges[i]);
            i += 1;
        }
        while j < other.ranges.len() {
            all.push(other.ranges[j]);
            j += 1;
        }

        RangeSet::from_raw(coalesce(all))
    }

    /// Compute `self - other` (set difference).
    ///
    /// Returns a new `RangeSet` containing all blocks in `self` that are
    /// **not** in `other`.
    ///
    /// Corresponds to AOSP `RangeSet::operator-(const RangeSet&)`.
    pub fn subtract(&self, other: &RangeSet) -> RangeSet {
        if self.is_empty() || other.is_empty() {
            return self.clone();
        }

        let mut result = Vec::new();
        let mut j = 0;

        for &r in &self.ranges {
            let mut cur_start = r.start;
            let cur_end = r.end;

            while j < other.ranges.len() && other.ranges[j].end <= cur_start {
                j += 1;
            }

            let mut k = j;
            while k < other.ranges.len() && other.ranges[k].start < cur_end {
                let sub = &other.ranges[k];

                if sub.start > cur_start {
                    result.push(Range {
                        start: cur_start,
                        end: sub.start,
                    });
                }

                cur_start = cur_start.max(sub.end);
                k += 1;
            }

            if cur_start < cur_end {
                result.push(Range {
                    start: cur_start,
                    end: cur_end,
                });
            }
        }

        RangeSet::from_raw(result)
    }

    /// Compute the intersection of `self` and `other`.
    ///
    /// Returns a new `RangeSet` containing only blocks present in **both**
    /// sets. Not directly in the AOSP public API but used internally and
    /// is the natural complement to `subtract` and `merge`.
    pub fn intersect(&self, other: &RangeSet) -> RangeSet {
        if self.is_empty() || other.is_empty() {
            return RangeSet::new();
        }

        let mut result = Vec::new();
        let (mut i, mut j) = (0usize, 0usize);

        while i < self.ranges.len() && j < other.ranges.len() {
            let a = &self.ranges[i];
            let b = &other.ranges[j];

            let lo = a.start.max(b.start);
            let hi = a.end.min(b.end);

            if lo < hi {
                result.push(Range { start: lo, end: hi });
            }

            if a.end < b.end {
                i += 1;
            } else {
                j += 1;
            }
        }

        RangeSet::from_raw(result)
    }
}

/// Coalesce a sorted (by start) vector of ranges, merging overlapping or
/// adjacent entries.
fn coalesce(sorted: Vec<Range>) -> Vec<Range> {
    if sorted.is_empty() {
        return sorted;
    }
    let mut out: Vec<Range> = Vec::with_capacity(sorted.len());
    let mut cur = sorted[0];

    for &r in &sorted[1..] {
        if r.start <= cur.end {
            // Overlapping or adjacent — extend.
            if r.end > cur.end {
                cur.end = r.end;
            }
        } else {
            out.push(cur);
            cur = r;
        }
    }
    out.push(cur);
    out
}

// ---- Display (serialization back to AOSP text format) ---------------------

impl fmt::Display for RangeSet {
    /// Serialize to the AOSP transfer-list format:
    /// `"count,s1,e1,s2,e2,..."`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = self.ranges.len() * 2;
        write!(f, "{count}")?;
        for r in &self.ranges {
            write!(f, ",{},{}", r.start, r.end)?;
        }
        Ok(())
    }
}

// ---- Default --------------------------------------------------------------

impl Default for RangeSet {
    fn default() -> Self {
        Self::new()
    }
}

// ---- Iterator over (start, end) pairs ------------------------------------

/// An iterator over the `(start, end)` pairs of a [`RangeSet`].
pub struct RangeSetIter<'a> {
    inner: std::slice::Iter<'a, Range>,
}

impl<'a> Iterator for RangeSetIter<'a> {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| (r.start, r.end))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for RangeSetIter<'_> {}

impl<'a> IntoIterator for &'a RangeSet {
    type Item = (u64, u64);
    type IntoIter = RangeSetIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl RangeSet {
    /// Returns an iterator over all `(start, end)` pairs.
    #[inline]
    pub fn iter(&self) -> RangeSetIter<'_> {
        RangeSetIter {
            inner: self.ranges.iter(),
        }
    }
}

// ---- Owning iterator -----------------------------------------------------

/// An owning iterator over the `(start, end)` pairs of a [`RangeSet`].
pub struct RangeSetIntoIter {
    inner: std::vec::IntoIter<Range>,
}

impl Iterator for RangeSetIntoIter {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| (r.start, r.end))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for RangeSetIntoIter {}

impl IntoIterator for RangeSet {
    type Item = (u64, u64);
    type IntoIter = RangeSetIntoIter;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        RangeSetIntoIter {
            inner: self.ranges.into_iter(),
        }
    }
}

// ---- Block-level iterator -------------------------------------------------

/// An iterator that yields every individual block number in a [`RangeSet`],
/// in order. Useful for sequential block I/O.
pub struct BlockIter<'a> {
    ranges: &'a [Range],
    range_idx: usize,
    block: u64,
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let r = self.ranges.get(self.range_idx)?;
            if self.block < r.end {
                let b = self.block;
                self.block += 1;
                return Some(b);
            }
            self.range_idx += 1;
            if let Some(next_r) = self.ranges.get(self.range_idx) {
                self.block = next_r.start;
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Compute remaining blocks for a tight bound.
        let mut remaining: u64 = 0;
        if let Some(r) = self.ranges.get(self.range_idx) {
            if self.block < r.end {
                remaining += r.end - self.block;
            }
            for r in &self.ranges[self.range_idx + 1..] {
                remaining += r.len();
            }
        }
        let r = remaining as usize;
        (r, Some(r))
    }
}

impl ExactSizeIterator for BlockIter<'_> {}

impl RangeSet {
    /// Returns an iterator that yields every individual block number in
    /// the set, in ascending order.
    pub fn block_iter(&self) -> BlockIter<'_> {
        let block = self.ranges.first().map_or(0, |r| r.start);
        BlockIter {
            ranges: &self.ranges,
            range_idx: 0,
            block,
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- parse / Display round-trip ---------------------------------------

    #[test]
    fn parse_simple() {
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        assert_eq!(rs.range_count(), 2);
        assert_eq!(rs.ranges()[0], Range { start: 1, end: 3 });
        assert_eq!(rs.ranges()[1], Range { start: 5, end: 9 });
    }

    #[test]
    fn parse_single_range() {
        let rs = RangeSet::parse("2,0,10").unwrap();
        assert_eq!(rs.range_count(), 1);
        assert_eq!(rs.blocks(), 10);
    }

    #[test]
    fn parse_display_roundtrip() {
        let input = "6,1,3,5,9,15,20";
        let rs = RangeSet::parse(input).unwrap();
        assert_eq!(rs.to_string(), input);
    }

    #[test]
    fn parse_empty_fails() {
        assert!(RangeSet::parse("").is_err());
    }

    #[test]
    fn parse_odd_count_fails() {
        assert!(RangeSet::parse("3,1,2,3").is_err());
    }

    #[test]
    fn parse_wrong_token_count_fails() {
        assert!(RangeSet::parse("4,1,3,5").is_err());
    }

    #[test]
    fn parse_reversed_range_fails() {
        assert!(RangeSet::parse("2,5,3").is_err());
    }

    #[test]
    fn parse_overlapping_ranges_fails() {
        assert!(RangeSet::parse("4,1,5,3,8").is_err());
    }

    #[test]
    fn parse_whitespace_trimmed() {
        let rs = RangeSet::parse("  2,0,5  ").unwrap();
        assert_eq!(rs.blocks(), 5);
    }

    // ---- blocks -----------------------------------------------------------

    #[test]
    fn blocks_empty() {
        let rs = RangeSet::new();
        assert_eq!(rs.blocks(), 0);
    }

    #[test]
    fn blocks_multi() {
        // [1,3) = 2 blocks, [5,9) = 4 blocks → total 6
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        assert_eq!(rs.blocks(), 6);
    }

    // ---- contains ---------------------------------------------------------

    #[test]
    fn contains_basic() {
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        assert!(!rs.contains(0));
        assert!(rs.contains(1));
        assert!(rs.contains(2));
        assert!(!rs.contains(3));
        assert!(!rs.contains(4));
        assert!(rs.contains(5));
        assert!(rs.contains(8));
        assert!(!rs.contains(9));
        assert!(!rs.contains(100));
    }

    #[test]
    fn contains_empty() {
        let rs = RangeSet::new();
        assert!(!rs.contains(0));
    }

    // ---- overlaps ---------------------------------------------------------

    #[test]
    fn overlaps_true() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,3,8").unwrap();
        assert!(a.overlaps(&b));
        assert!(b.overlaps(&a));
    }

    #[test]
    fn overlaps_adjacent_is_false() {
        // AOSP: "3,5" and "5,7" are NOT overlapped.
        let a = RangeSet::parse("2,3,5").unwrap();
        let b = RangeSet::parse("2,5,7").unwrap();
        assert!(!a.overlaps(&b));
        assert!(!b.overlaps(&a));
    }

    #[test]
    fn overlaps_disjoint() {
        let a = RangeSet::parse("2,1,3").unwrap();
        let b = RangeSet::parse("2,5,9").unwrap();
        assert!(!a.overlaps(&b));
    }

    #[test]
    fn overlaps_empty() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::new();
        assert!(!a.overlaps(&b));
    }

    // ---- get_block_number -------------------------------------------------

    #[test]
    fn get_block_number_basic() {
        // Ranges: [1,3) [5,9)  → linear: 1,2,5,6,7,8
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        assert_eq!(rs.get_block_number(0), Some(1));
        assert_eq!(rs.get_block_number(1), Some(2));
        assert_eq!(rs.get_block_number(2), Some(5));
        assert_eq!(rs.get_block_number(3), Some(6));
        assert_eq!(rs.get_block_number(4), Some(7));
        assert_eq!(rs.get_block_number(5), Some(8));
        assert_eq!(rs.get_block_number(6), None);
    }

    #[test]
    fn get_block_number_empty() {
        let rs = RangeSet::new();
        assert_eq!(rs.get_block_number(0), None);
    }

    // ---- get_sub_ranges ---------------------------------------------------

    #[test]
    fn get_sub_ranges_basic() {
        // [0,10) → sub at offset 3, len 4 → [3,7)
        let rs = RangeSet::parse("2,0,10").unwrap();
        let sub = rs.get_sub_ranges(3, 4);
        assert_eq!(sub.to_string(), "2,3,7");
    }

    #[test]
    fn get_sub_ranges_spans_multiple() {
        // [1,3) [5,9) → linear: 1,2,5,6,7,8
        // sub at offset 1, len 3 → blocks 2,5,6 → [2,3) [5,7)
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        let sub = rs.get_sub_ranges(1, 3);
        assert_eq!(sub.to_string(), "4,2,3,5,7");
    }

    #[test]
    fn get_sub_ranges_full() {
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        let sub = rs.get_sub_ranges(0, rs.blocks());
        assert_eq!(sub, rs);
    }

    #[test]
    fn get_sub_ranges_zero_len() {
        let rs = RangeSet::parse("2,0,10").unwrap();
        let sub = rs.get_sub_ranges(5, 0);
        assert!(sub.is_empty());
    }

    #[test]
    fn get_sub_ranges_beyond_end() {
        let rs = RangeSet::parse("2,0,5").unwrap();
        let sub = rs.get_sub_ranges(3, 100);
        // Can only take blocks 3,4 → [3,5)
        assert_eq!(sub.to_string(), "2,3,5");
        assert_eq!(sub.blocks(), 2);
    }

    // ---- split ------------------------------------------------------------

    #[test]
    fn split_basic() {
        // 10 blocks split by 3 → [3, 3, 3, 1]
        let rs = RangeSet::parse("2,0,10").unwrap();
        let parts = rs.split(3);
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0].blocks(), 3);
        assert_eq!(parts[1].blocks(), 3);
        assert_eq!(parts[2].blocks(), 3);
        assert_eq!(parts[3].blocks(), 1);

        assert_eq!(parts[0].to_string(), "2,0,3");
        assert_eq!(parts[1].to_string(), "2,3,6");
        assert_eq!(parts[2].to_string(), "2,6,9");
        assert_eq!(parts[3].to_string(), "2,9,10");
    }

    #[test]
    fn split_fits_in_one() {
        let rs = RangeSet::parse("2,0,5").unwrap();
        let parts = rs.split(10);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0], rs);
    }

    #[test]
    fn split_exact_multiple() {
        let rs = RangeSet::parse("2,0,6").unwrap();
        let parts = rs.split(3);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].to_string(), "2,0,3");
        assert_eq!(parts[1].to_string(), "2,3,6");
    }

    #[test]
    fn split_multi_range() {
        // [0,2) [5,8) → 5 blocks total, split by 2
        let rs = RangeSet::parse("4,0,2,5,8").unwrap();
        let parts = rs.split(2);
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0].to_string(), "2,0,2"); // blocks 0,1
        assert_eq!(parts[1].to_string(), "2,5,7"); // blocks 5,6
        assert_eq!(parts[2].to_string(), "2,7,8"); // block  7
    }

    // ---- merge ------------------------------------------------------------

    #[test]
    fn merge_disjoint() {
        let a = RangeSet::parse("2,1,3").unwrap();
        let b = RangeSet::parse("2,5,9").unwrap();
        let m = a.merge(&b);
        assert_eq!(m.to_string(), "4,1,3,5,9");
    }

    #[test]
    fn merge_overlapping() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,3,8").unwrap();
        let m = a.merge(&b);
        assert_eq!(m.to_string(), "2,1,8");
    }

    #[test]
    fn merge_adjacent() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,5,9").unwrap();
        let m = a.merge(&b);
        assert_eq!(m.to_string(), "2,1,9");
    }

    #[test]
    fn merge_with_empty() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::new();
        assert_eq!(a.merge(&b), a);
        assert_eq!(b.merge(&a), a);
    }

    #[test]
    fn merge_identical() {
        let a = RangeSet::parse("4,1,3,5,9").unwrap();
        let m = a.merge(&a);
        assert_eq!(m, a);
    }

    #[test]
    fn merge_complex() {
        // a: [0,3) [10,15)    b: [2,5) [7,8) [12,20)
        // union: [0,5) [7,8) [10,20)
        let a = RangeSet::parse("4,0,3,10,15").unwrap();
        let b = RangeSet::parse("6,2,5,7,8,12,20").unwrap();
        let m = a.merge(&b);
        assert_eq!(m.to_string(), "6,0,5,7,8,10,20");
    }

    // ---- subtract ---------------------------------------------------------

    #[test]
    fn subtract_no_overlap() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,6,9").unwrap();
        assert_eq!(a.subtract(&b), a);
    }

    #[test]
    fn subtract_complete() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,0,10").unwrap();
        assert!(a.subtract(&b).is_empty());
    }

    #[test]
    fn subtract_partial_front() {
        // [1,5) - [0,3) = [3,5)
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,0,3").unwrap();
        let d = a.subtract(&b);
        assert_eq!(d.to_string(), "2,3,5");
    }

    #[test]
    fn subtract_partial_back() {
        // [1,5) - [3,8) = [1,3)
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::parse("2,3,8").unwrap();
        let d = a.subtract(&b);
        assert_eq!(d.to_string(), "2,1,3");
    }

    #[test]
    fn subtract_middle() {
        // [0,10) - [3,7) = [0,3) [7,10)
        let a = RangeSet::parse("2,0,10").unwrap();
        let b = RangeSet::parse("2,3,7").unwrap();
        let d = a.subtract(&b);
        assert_eq!(d.to_string(), "4,0,3,7,10");
    }

    #[test]
    fn subtract_with_empty() {
        let a = RangeSet::parse("2,1,5").unwrap();
        let b = RangeSet::new();
        assert_eq!(a.subtract(&b), a);
    }

    #[test]
    fn subtract_complex() {
        // a: [0,10) [20,30)
        // b: [5,7) [8,25)
        // result: [0,5) [7,8) [25,30)
        let a = RangeSet::parse("4,0,10,20,30").unwrap();
        let b = RangeSet::parse("4,5,7,8,25").unwrap();
        let d = a.subtract(&b);
        assert_eq!(d.to_string(), "6,0,5,7,8,25,30");
    }

    // ---- intersect --------------------------------------------------------

    #[test]
    fn intersect_basic() {
        let a = RangeSet::parse("2,1,8").unwrap();
        let b = RangeSet::parse("2,3,10").unwrap();
        let i = a.intersect(&b);
        assert_eq!(i.to_string(), "2,3,8");
    }

    #[test]
    fn intersect_disjoint() {
        let a = RangeSet::parse("2,1,3").unwrap();
        let b = RangeSet::parse("2,5,9").unwrap();
        assert!(a.intersect(&b).is_empty());
    }

    #[test]
    fn intersect_complex() {
        // a: [0,5) [10,20)    b: [3,12) [15,25)
        // intersection: [3,5) [10,12) [15,20)
        let a = RangeSet::parse("4,0,5,10,20").unwrap();
        let b = RangeSet::parse("4,3,12,15,25").unwrap();
        let i = a.intersect(&b);
        assert_eq!(i.to_string(), "6,3,5,10,12,15,20");
    }

    // ---- iterators --------------------------------------------------------

    #[test]
    fn iter_pairs() {
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        let pairs: Vec<(u64, u64)> = rs.iter().collect();
        assert_eq!(pairs, vec![(1, 3), (5, 9)]);
    }

    #[test]
    fn into_iter_pairs() {
        let rs = RangeSet::parse("4,1,3,5,9").unwrap();
        let pairs: Vec<(u64, u64)> = rs.into_iter().collect();
        assert_eq!(pairs, vec![(1, 3), (5, 9)]);
    }

    #[test]
    fn for_loop_ref() {
        let rs = RangeSet::parse("2,0,3").unwrap();
        let mut pairs = Vec::new();
        for (s, e) in &rs {
            pairs.push((s, e));
        }
        assert_eq!(pairs, vec![(0, 3)]);
    }

    #[test]
    fn block_iter_basic() {
        let rs = RangeSet::parse("4,1,3,5,8").unwrap();
        let blocks: Vec<u64> = rs.block_iter().collect();
        assert_eq!(blocks, vec![1, 2, 5, 6, 7]);
    }

    #[test]
    fn block_iter_empty() {
        let rs = RangeSet::new();
        let blocks: Vec<u64> = rs.block_iter().collect();
        assert!(blocks.is_empty());
    }

    #[test]
    fn block_iter_exact_size() {
        let rs = RangeSet::parse("4,0,3,10,15").unwrap();
        let iter = rs.block_iter();
        assert_eq!(iter.len(), 8);
    }

    // ---- from_pairs -------------------------------------------------------

    #[test]
    fn from_pairs_basic() {
        let rs = RangeSet::from_pairs(&[(1, 3), (5, 9)]).unwrap();
        assert_eq!(rs.to_string(), "4,1,3,5,9");
    }

    #[test]
    fn from_pairs_empty() {
        let rs = RangeSet::from_pairs(&[]).unwrap();
        assert!(rs.is_empty());
    }

    #[test]
    fn from_pairs_overlapping_fails() {
        assert!(RangeSet::from_pairs(&[(1, 5), (3, 8)]).is_err());
    }

    // ---- Display for empty ------------------------------------------------

    #[test]
    fn display_empty() {
        let rs = RangeSet::new();
        assert_eq!(rs.to_string(), "0");
    }

    // ---- identity properties ----------------------------------------------

    #[test]
    fn subtract_then_merge_restores_original() {
        let full = RangeSet::parse("2,0,100").unwrap();
        let hole = RangeSet::parse("2,30,50").unwrap();
        let diff = full.subtract(&hole);
        let restored = diff.merge(&hole);
        assert_eq!(restored, full);
    }

    #[test]
    fn merge_is_commutative() {
        let a = RangeSet::parse("4,0,3,10,15").unwrap();
        let b = RangeSet::parse("4,2,5,7,8").unwrap();
        assert_eq!(a.merge(&b), b.merge(&a));
    }

    #[test]
    fn subtract_self_is_empty() {
        let a = RangeSet::parse("4,1,3,5,9").unwrap();
        assert!(a.subtract(&a).is_empty());
    }

    #[test]
    fn intersect_self_is_identity() {
        let a = RangeSet::parse("4,1,3,5,9").unwrap();
        assert_eq!(a.intersect(&a), a);
    }

    // ---- large range smoke ------------------------------------------------

    #[test]
    fn large_range_blocks() {
        // A range spanning 1M blocks.
        let rs = RangeSet::parse("2,0,1000000").unwrap();
        assert_eq!(rs.blocks(), 1_000_000);
        assert!(rs.contains(999_999));
        assert!(!rs.contains(1_000_000));
    }

    #[test]
    fn split_preserves_total_blocks() {
        let rs = RangeSet::parse("6,0,100,200,350,500,510").unwrap();
        let parts = rs.split(37);
        let total: u64 = parts.iter().map(|p| p.blocks()).sum();
        assert_eq!(total, rs.blocks());

        // Also ensure the merged result equals original.
        let mut merged = RangeSet::new();
        for p in &parts {
            merged = merged.merge(p);
        }
        assert_eq!(merged, rs);
    }
}
