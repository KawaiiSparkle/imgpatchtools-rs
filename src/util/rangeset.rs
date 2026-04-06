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

        ensure!(count.is_multiple_of(2), "range count must be even, got {count}");
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
