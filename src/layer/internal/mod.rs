pub mod base;
pub mod base_merge;
pub mod child;
mod object_iterator;
mod predicate_iterator;
pub mod rollup;
mod subject_iterator;

use super::id_map::*;
use super::layer::*;
use tdb_succinct::tfc::interval::IntervalEndIndex;
use tdb_succinct::*;

use std::ops::Range;

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::convert::TryInto;

pub use base::*;
pub use child::*;
pub use object_iterator::*;
pub use predicate_iterator::*;
pub use rollup::*;
pub use subject_iterator::*;

#[derive(Clone)]
pub enum InternalLayer {
    Base(BaseLayer),
    Child(ChildLayer),
    Rollup(RollupLayer),
}

use tfc::block::IdLookupResult;
use InternalLayer::*;

/// One band's prefix range: a front-coded dict position walked forward
/// while the name carries the prefix. `base` is the cumulative id-space
/// size of all parent bands, turning inner positions into global ids.
/// Successor of a string in dict order: increment the last non-max char.
/// UTF-8 byte order preserves code-point order, so every extension of
/// `prefix` sorts before its successor, making the successor the exclusive
/// upper bound of the prefix range. `None` means no successor exists
/// (empty or all-max-char input) and the range runs to the end.
fn prefix_successor(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(&last) = chars.last() {
        if last == char::MAX {
            chars.pop();
            continue;
        }
        let mut c = last as u32 + 1;
        while c <= char::MAX as u32 {
            if let Some(next) = char::from_u32(c) {
                chars.pop();
                chars.push(next);
                return Some(chars.into_iter().collect());
            }
            c += 1;
        }
        chars.pop();
    }
    None
}

/// First position in `dict` whose entry is >= `key` (lower bound).
///
/// `StringDict::id` only approximates the insertion point: `Closest` can
/// point at a block boundary below the true bound, and `NotFound` only
/// means the key precedes the first block head or the dict is empty. The
/// seek is refined by walking forward while entries still sort below the
/// key, which stays within a block of work.
fn dict_lower_bound(dict: &StringDict, len: u64, key: &str) -> u64 {
    let key_owned = key.to_string();
    let mut pos = match dict.id(&key_owned) {
        IdLookupResult::Found(i) => i,
        IdLookupResult::Closest(i) => i + 1,
        IdLookupResult::NotFound => 1,
    };
    while pos <= len {
        match dict.get(pos as usize) {
            Some(entry) if entry.as_str() < key => pos += 1,
            _ => break,
        }
    }
    // Guard against Closest landing above the insertion point.
    while pos > 1 {
        match dict.get((pos - 1) as usize) {
            Some(entry) if entry.as_str() >= key => pos -= 1,
            _ => break,
        }
    }
    pos
}

/// Lazily decode one (edge band, object, side) member-run slice. The
/// subject-sorted run is bisected to `[s_lo, s_hi)` once, then walked
/// forward yielding `(subject, sign)` for entries matching `predicate`.
/// For a wavelet-mapped name band the slice covers the band's whole
/// subject space and each subject's inner position is checked against
/// `[a_lo, a_hi)`.
///
/// `subjects`/`objects` are the edge band's index arrays; `None` means the
/// array position is the id itself.
struct MemberSlice {
    subjects: Option<MonotonicLogArray>,
    s_p: AdjacencyList,
    o_ps: AdjacencyList,
    predicate: u64,
    sign: i64,
    pos: u64,
    end: u64,
    a_map: Option<IdMap>,
    a_base: u64,
    a_lo: u64,
    a_hi: u64,
}

impl MemberSlice {
    #[allow(clippy::too_many_arguments)]
    fn new(
        subjects: Option<MonotonicLogArray>,
        objects: Option<MonotonicLogArray>,
        o_ps: AdjacencyList,
        s_p: AdjacencyList,
        object: u64,
        s_lo: u64,
        s_hi: u64,
        predicate: u64,
        sign: i64,
        a_map: Option<IdMap>,
        a_base: u64,
        a_lo: u64,
        a_hi: u64,
    ) -> Option<Self> {
        // 0-based index of `object` in the band's object space.
        let o_pos = match &objects {
            Some(objects) => {
                let pos = objects.nearest_index_of(object);
                if pos >= objects.len() || objects.entry(pos) != object {
                    return None;
                }
                pos
            }
            None => {
                if object == 0 || object > o_ps.left_count() as u64 {
                    return None;
                }
                (object - 1) as usize
            }
        };
        let left = o_pos as u64 + 1;
        let run_lo = o_ps.offset_for(left);
        let run_hi = if left == o_ps.left_count() as u64 {
            o_ps.right_count() as u64
        } else {
            o_ps.offset_for(left + 1)
        };
        if run_lo >= run_hi {
            return None;
        }

        // Subject bounds as 1-based mapped-subject positions (or raw ids
        // when the band has no subjects array).
        let (ms_lo, ms_hi) = match &subjects {
            Some(subjects) => (
                subjects.nearest_index_of(s_lo) as u64 + 1,
                subjects.nearest_index_of(s_hi) as u64 + 1,
            ),
            None => (s_lo, s_hi),
        };
        if ms_lo >= ms_hi {
            return None;
        }

        // shortcut: pair_num 0 is run-tail padding and reads as
        // below-bound; mid-run padding would break the bisect — revisit
        // if the format ever allows padding inside a run.
        let mapped_at = |pos: u64| -> u64 {
            let pair_num = o_ps.num_at_pos(pos);
            if pair_num == 0 {
                0
            } else {
                s_p.left_at_pos(pair_num - 1)
            }
        };
        let bisect = |bound: u64| -> u64 {
            let mut lo = run_lo;
            let mut hi = run_hi;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                if mapped_at(mid) < bound {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            lo
        };
        let start = bisect(ms_lo);
        let end = bisect(ms_hi);
        if start >= end {
            return None;
        }

        Some(Self {
            subjects,
            s_p,
            o_ps,
            predicate,
            sign,
            pos: start,
            end,
            a_map,
            a_base,
            a_lo,
            a_hi,
        })
    }
}

impl Iterator for MemberSlice {
    type Item = (u64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos < self.end {
            let pair_num = self.o_ps.num_at_pos(self.pos);
            self.pos += 1;
            if pair_num == 0 {
                continue;
            }
            let (mapped_subject, predicate) = self.s_p.pair_at_pos(pair_num - 1);
            if predicate != self.predicate {
                continue;
            }
            let subject = match &self.subjects {
                Some(subjects) => subjects.entry(mapped_subject as usize - 1),
                None => mapped_subject,
            };
            if let Some(map) = &self.a_map {
                let inner = map.outer_to_inner(subject - self.a_base);
                if inner < self.a_lo || inner >= self.a_hi {
                    continue;
                }
            }
            return Some((subject, self.sign));
        }
        None
    }
}

/// Merge one name band's member slices by subject id, yielding
/// `(subject, net)` for subjects with net membership > 0 — a subject's
/// events arrive grouped since every slice is subject-sorted.
struct BandMembers {
    slices: Vec<std::iter::Peekable<MemberSlice>>,
    heap: BinaryHeap<(Reverse<u64>, usize)>,
}

impl BandMembers {
    fn new(slices: Vec<MemberSlice>) -> Self {
        let mut slices: Vec<std::iter::Peekable<MemberSlice>> =
            slices.into_iter().map(|s| s.peekable()).collect();
        let mut heap = BinaryHeap::new();
        for (i, slice) in slices.iter_mut().enumerate() {
            if let Some((subject, _)) = slice.peek() {
                heap.push((Reverse(*subject), i));
            }
        }
        BandMembers { slices, heap }
    }
}

impl Iterator for BandMembers {
    type Item = (u64, i64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (Reverse(subject), idx) = self.heap.pop()?;
            let (_, sign) = self.slices[idx].next().unwrap();
            let mut net = sign;
            if let Some((next_subject, _)) = self.slices[idx].peek() {
                self.heap.push((Reverse(*next_subject), idx));
            }
            while matches!(self.heap.peek(), Some((Reverse(s), _)) if *s == subject) {
                let (_, dup_idx) = self.heap.pop().unwrap();
                let (_, sign) = self.slices[dup_idx].next().unwrap();
                net += sign;
                if let Some((next_subject, _)) = self.slices[dup_idx].peek() {
                    self.heap.push((Reverse(*next_subject), dup_idx));
                }
            }
            if net > 0 {
                return Some((subject, net));
            }
        }
    }
}

/// One band's prefix range: `lo..hi` are dictionary positions of names
/// carrying the prefix, `base` the band's offset in the combined
/// node+value id space. `identity` bands assign `outer = base + inner`
/// so the prefix maps to a contiguous outer range; wavelet (rollup)
/// bands permute it.
struct PrefixBand {
    base: u64,
    node_len: u64,
    val_len: u64,
    lo: u64,
    hi: u64,
    identity: bool,
}

/// The per-band prefix dictionary ranges for `layers`.
fn prefix_band_ranges(layers: &[&InternalLayer], prefix: &str) -> Vec<PrefixBand> {
    let end_key = prefix_successor(prefix);
    let mut bands = Vec::with_capacity(layers.len());
    let mut base = 0u64;
    for layer in layers.iter() {
        let node_len = layer.node_dict_len() as u64;
        let val_len = layer.value_dict_len() as u64;
        let node_dict = layer.node_dictionary();
        let lo = dict_lower_bound(node_dict, node_len, prefix);
        let hi = match end_key.as_ref() {
            Some(end_key) => dict_lower_bound(node_dict, node_len, end_key),
            None => node_len + 1,
        };
        bands.push(PrefixBand {
            base,
            node_len,
            val_len,
            lo,
            hi,
            identity: layer.node_value_id_map().id_wtree.is_none(),
        });
        base += node_len + val_len;
    }
    bands
}

/// The member slices joining name band `a_idx` with every edge band at or
/// above it: positive and negative runs of each object in `objects`.
fn member_slices(
    layers: &[&InternalLayer],
    bands: &[PrefixBand],
    a_idx: usize,
    predicate: u64,
    objects: &HashSet<u64>,
) -> Vec<MemberSlice> {
    let a = &bands[a_idx];
    let (s_lo, s_hi) = if a.identity {
        (a.base + a.lo, a.base + a.hi)
    } else {
        (a.base + 1, a.base + a.node_len + a.val_len + 1)
    };
    let a_map = if a.identity {
        None
    } else {
        Some(layers[a_idx].node_value_id_map().clone())
    };
    let mut slices = Vec::new();
    for band in layers.iter().skip(a_idx) {
        for (subjects, band_objects, o_ps, s_p, sign) in [
            (
                band.pos_subjects(),
                band.pos_objects(),
                Some(band.pos_o_ps_adjacency_list()),
                Some(band.pos_s_p_adjacency_list()),
                1i64,
            ),
            (
                band.neg_subjects(),
                band.neg_objects(),
                band.neg_o_ps_adjacency_list(),
                band.neg_s_p_adjacency_list(),
                -1i64,
            ),
        ] {
            let (o_ps, s_p) = match (o_ps, s_p) {
                (Some(o_ps), Some(s_p)) => (o_ps, s_p),
                _ => continue,
            };
            for object in objects {
                if let Some(slice) = MemberSlice::new(
                    subjects.cloned(),
                    band_objects.cloned(),
                    o_ps.clone(),
                    s_p.clone(),
                    *object,
                    s_lo,
                    s_hi,
                    predicate,
                    sign,
                    a_map.clone(),
                    a.base,
                    a.lo,
                    a.hi,
                ) {
                    slices.push(slice);
                }
            }
        }
    }
    slices
}

struct BandPrefixCursor {
    layer: InternalLayer,
    base: u64,
    pos: u64,
    len: u64,
    prefix: String,
}

impl Iterator for BandPrefixCursor {
    type Item = (u64, String);

    fn next(&mut self) -> Option<Self::Item> {
        while self.pos <= self.len {
            match self.layer.node_dict_get(self.pos as usize) {
                Some(name) if name.starts_with(&self.prefix) => {
                    let id = self.base + self.layer.node_value_id_map().inner_to_outer(self.pos);
                    self.pos += 1;
                    return Some((id, name));
                }
                _ => break,
            }
        }
        None
    }
}

/// K-way merge over per-band name-sorted sources. Each source yields
/// (id, name) sorted by name, so a max-heap on (Reverse(name), rank,
/// band) yields global name order lazily. Names present in several bands
/// dedup to the topmost band's id: equal names pop in descending rank
/// order, so the first pop wins and later occurrences are skipped.
struct PrefixSubjects {
    bands: Vec<(
        usize,
        std::iter::Peekable<Box<dyn Iterator<Item = (u64, String)> + Send>>,
    )>,
    heap: BinaryHeap<(Reverse<String>, usize, usize)>,
}

impl PrefixSubjects {
    fn new(
        mut bands: Vec<(
            usize,
            std::iter::Peekable<Box<dyn Iterator<Item = (u64, String)> + Send>>,
        )>,
    ) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, (rank, band)) in bands.iter_mut().enumerate() {
            if let Some((_, name)) = band.peek() {
                heap.push((Reverse(name.clone()), *rank, i));
            }
        }
        PrefixSubjects { bands, heap }
    }

    fn refresh(&mut self, idx: usize) {
        let (rank, band) = &mut self.bands[idx];
        if let Some((_, name)) = band.peek() {
            self.heap.push((Reverse(name.clone()), *rank, idx));
        }
    }
}

impl Iterator for PrefixSubjects {
    type Item = (u64, String);

    fn next(&mut self) -> Option<Self::Item> {
        let (Reverse(name), _, idx) = self.heap.pop()?;
        let (id, _) = self.bands[idx].1.next().unwrap();
        self.refresh(idx);

        while matches!(self.heap.peek(), Some((Reverse(n), _, _)) if *n == name) {
            let (_, _, dup_idx) = self.heap.pop().unwrap();
            self.bands[dup_idx].1.next();
            self.refresh(dup_idx);
        }

        Some((id, name))
    }
}

impl InternalLayer {
    pub fn name(&self) -> [u32; 5] {
        match self {
            Base(base) => base.name,
            Child(child) => child.name,
            Rollup(rollup) => rollup.original,
        }
    }

    pub fn parent_name(&self) -> Option<[u32; 5]> {
        match self {
            Base(_) => None,
            Child(child) => Some(child.parent.name()),
            Rollup(rollup) => rollup.original_parent,
        }
    }

    pub fn immediate_parent(&self) -> Option<&InternalLayer> {
        match self {
            Base(_) => None,
            Child(child) => Some(&*child.parent),
            Rollup(rollup) => rollup.internal.immediate_parent(),
        }
    }

    pub fn layer_stack_size(&self) -> usize {
        let mut count = 1;
        let mut l = self;
        while let Some(p) = l.immediate_parent() {
            l = p;
            count += 1;
        }

        count
    }

    pub fn node_dictionary(&self) -> &StringDict {
        match self {
            Base(base) => &base.node_dictionary,
            Child(child) => &child.node_dictionary,
            Rollup(rollup) => rollup.internal.node_dictionary(),
        }
    }

    pub fn predicate_dictionary(&self) -> &StringDict {
        match self {
            Base(base) => &base.predicate_dictionary,
            Child(child) => &child.predicate_dictionary,
            Rollup(rollup) => rollup.internal.predicate_dictionary(),
        }
    }

    pub fn value_dictionary(&self) -> &TypedDict {
        match self {
            Base(base) => &base.value_dictionary,
            Child(child) => &child.value_dictionary,
            Rollup(rollup) => rollup.internal.value_dictionary(),
        }
    }

    pub fn node_value_id_map(&self) -> &IdMap {
        match self {
            Base(base) => &base.node_value_idmap,
            Child(child) => &child.node_value_idmap,
            Rollup(rollup) => rollup.internal.node_value_id_map(),
        }
    }

    pub fn predicate_id_map(&self) -> &IdMap {
        match self {
            Base(base) => &base.predicate_idmap,
            Child(child) => &child.predicate_idmap,
            Rollup(rollup) => rollup.internal.predicate_id_map(),
        }
    }

    pub fn parent_node_value_count(&self) -> usize {
        match self {
            Base(_) => 0,
            Child(child) => child.parent_node_value_count,
            Rollup(rollup) => rollup.internal.parent_node_value_count(),
        }
    }

    pub fn parent_predicate_count(&self) -> usize {
        match self {
            Base(_) => 0,
            Child(child) => child.parent_predicate_count,
            Rollup(rollup) => rollup.internal.parent_predicate_count(),
        }
    }

    pub fn pos_s_p_adjacency_list(&self) -> &AdjacencyList {
        match self {
            Base(base) => &base.s_p_adjacency_list,
            Child(child) => &child.pos_s_p_adjacency_list,
            Rollup(rollup) => rollup.internal.pos_s_p_adjacency_list(),
        }
    }

    pub fn pos_sp_o_adjacency_list(&self) -> &AdjacencyList {
        match self {
            Base(base) => &base.sp_o_adjacency_list,
            Child(child) => &child.pos_sp_o_adjacency_list,
            Rollup(rollup) => rollup.internal.pos_sp_o_adjacency_list(),
        }
    }

    pub fn pos_o_ps_adjacency_list(&self) -> &AdjacencyList {
        match self {
            Base(base) => &base.o_ps_adjacency_list,
            Child(child) => &child.pos_o_ps_adjacency_list,
            Rollup(rollup) => rollup.internal.pos_o_ps_adjacency_list(),
        }
    }

    pub fn neg_s_p_adjacency_list(&self) -> Option<&AdjacencyList> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_s_p_adjacency_list),
            Rollup(rollup) => rollup.internal.neg_s_p_adjacency_list(),
        }
    }

    pub fn neg_sp_o_adjacency_list(&self) -> Option<&AdjacencyList> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_sp_o_adjacency_list),
            Rollup(rollup) => rollup.internal.neg_sp_o_adjacency_list(),
        }
    }

    pub fn neg_o_ps_adjacency_list(&self) -> Option<&AdjacencyList> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_o_ps_adjacency_list),
            Rollup(rollup) => rollup.internal.neg_o_ps_adjacency_list(),
        }
    }

    pub fn pos_predicate_wavelet_tree(&self) -> &WaveletTree {
        match self {
            Base(base) => &base.predicate_wavelet_tree,
            Child(child) => &child.pos_predicate_wavelet_tree,
            Rollup(rollup) => rollup.internal.pos_predicate_wavelet_tree(),
        }
    }

    /// Dense-rank index on DateTimeInterval end timestamps. Present on every
    /// layer: read from files when they exist, built in memory at open for
    /// older layers (which also got their 48-byte entries upgraded to 52).
    pub fn interval_index(&self) -> &IntervalEndIndex {
        match self {
            Base(base) => &base.interval_index,
            Child(child) => &child.interval_index,
            Rollup(rollup) => rollup.internal.interval_index(),
        }
    }

    /// Ids of DateTimeInterval values with `start <= (secs, nanos)`.
    pub fn interval_ids_start_le(&self, secs: i64, nanos: u32) -> Range<u64> {
        self.value_dictionary().interval_ids_start_le(secs, nanos)
    }

    /// Ids of DateTimeInterval values with `start < (secs, nanos)`.
    pub fn interval_ids_start_lt(&self, secs: i64, nanos: u32) -> Range<u64> {
        self.value_dictionary().interval_ids_start_lt(secs, nanos)
    }

    /// Ids of DateTimeInterval values with `start > (secs, nanos)`.
    pub fn interval_ids_start_gt(&self, secs: i64, nanos: u32) -> Range<u64> {
        self.value_dictionary().interval_ids_start_gt(secs, nanos)
    }

    /// Ids of DateTimeInterval values with `start >= (secs, nanos)`.
    pub fn interval_ids_start_ge(&self, secs: i64, nanos: u32) -> Range<u64> {
        self.value_dictionary().interval_ids_start_ge(secs, nanos)
    }

    /// Ids of DateTimeInterval values with `start == (secs, nanos)`.
    pub fn interval_ids_start_eq(&self, secs: i64, nanos: u32) -> Range<u64> {
        self.value_dictionary().interval_ids_start_eq(secs, nanos)
    }

    /// Ids of DateTimeInterval values satisfying both a start-id range and
    /// an end predicate over the index. `ids` is typically the output of one
    /// of the `interval_ids_start_*` probes; positions in the index are
    /// segment-relative, so the range is shifted before querying.
    fn interval_ids_end_filtered(
        &self,
        ids: Range<u64>,
        query: impl Fn(&IntervalEndIndex, Range<u64>) -> Vec<u64>,
    ) -> Vec<u64> {
        let Some(seg) = self.value_dictionary().interval_segment() else {
            return Vec::new();
        };
        let pos = ids.start.saturating_sub(seg.start)..ids.end.saturating_sub(seg.start);
        query(self.interval_index(), pos)
            .into_iter()
            .map(|p| p + seg.start)
            .collect()
    }

    /// Ids of DateTimeInterval values with `end <= (secs, nanos)` within `ids`.
    pub fn interval_ids_end_le(&self, ids: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.interval_ids_end_filtered(ids, |i, p| i.positions_end_le(p, secs, nanos))
    }

    /// Ids of DateTimeInterval values with `end < (secs, nanos)` within `ids`.
    pub fn interval_ids_end_lt(&self, ids: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.interval_ids_end_filtered(ids, |i, p| i.positions_end_lt(p, secs, nanos))
    }

    /// Ids of DateTimeInterval values with `end >= (secs, nanos)` within `ids`.
    pub fn interval_ids_end_ge(&self, ids: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.interval_ids_end_filtered(ids, |i, p| i.positions_end_ge(p, secs, nanos))
    }

    /// Ids of DateTimeInterval values with `end > (secs, nanos)` within `ids`.
    pub fn interval_ids_end_gt(&self, ids: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.interval_ids_end_filtered(ids, |i, p| i.positions_end_gt(p, secs, nanos))
    }

    /// Ids of DateTimeInterval values with `end == (secs, nanos)` within `ids`.
    pub fn interval_ids_end_eq(&self, ids: Range<u64>, secs: i64, nanos: u32) -> Vec<u64> {
        self.interval_ids_end_filtered(ids, |i, p| i.positions_end_eq(p, secs, nanos))
    }

    /// Ids of DateTimeInterval values with `end` in `[lo, hi]` within `ids`.
    pub fn interval_ids_end_range(
        &self,
        ids: Range<u64>,
        lo: (i64, u32),
        hi: (i64, u32),
    ) -> Vec<u64> {
        self.interval_ids_end_filtered(ids, |i, p| i.positions_end_range(p, lo, hi))
    }

    pub fn neg_predicate_wavelet_tree(&self) -> Option<&WaveletTree> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_predicate_wavelet_tree),
            Rollup(rollup) => rollup.internal.neg_predicate_wavelet_tree(),
        }
    }

    pub fn pos_subjects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(base) => base.subjects.as_ref(),
            Child(child) => Some(&child.pos_subjects),
            Rollup(rollup) => rollup.internal.pos_subjects(),
        }
    }

    pub fn pos_objects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(base) => base.objects.as_ref(),
            Child(child) => Some(&child.pos_objects),
            Rollup(rollup) => rollup.internal.pos_objects(),
        }
    }

    pub fn neg_subjects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_subjects),
            Rollup(rollup) => rollup.internal.neg_subjects(),
        }
    }

    pub fn neg_objects(&self) -> Option<&MonotonicLogArray> {
        match self {
            Base(_) => None,
            Child(child) => Some(&child.neg_objects),
            Rollup(rollup) => rollup.internal.neg_objects(),
        }
    }

    /// Returns the byte size of this layer's own backing data (not parents).
    pub fn stored_size(&self) -> usize {
        match self {
            Base(base) => base.stored_size,
            Child(child) => child.stored_size,
            Rollup(rollup) => rollup.internal.stored_size(),
        }
    }

    pub fn predicate_dict_get(&self, id: usize) -> Option<String> {
        self.predicate_dictionary().get(id)
    }

    pub fn predicate_dict_len(&self) -> usize {
        self.predicate_dictionary().num_entries()
    }

    pub fn predicate_dict_id(&self, predicate: &str) -> IdLookupResult {
        self.predicate_dictionary().id(&predicate)
    }

    pub fn node_dict_id(&self, subject: &str) -> IdLookupResult {
        self.node_dictionary().id(&subject)
    }

    pub fn node_dict_get(&self, id: usize) -> Option<String> {
        self.node_dictionary().get(id)
    }

    pub fn node_dict_len(&self) -> usize {
        self.node_dictionary().num_entries()
    }

    pub fn value_dict_id(&self, value: &TypedDictEntry) -> IdLookupResult {
        self.value_dictionary().id_entry(value)
    }

    pub fn value_dict_len(&self) -> usize {
        self.value_dictionary().num_entries()
    }

    pub fn value_dict_get(&self, id: usize) -> Option<TypedDictEntry> {
        self.value_dictionary().entry(id)
    }

    pub fn internal_triple_addition_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> bool {
        layer_triple_exists(
            self.pos_subjects(),
            self.pos_s_p_adjacency_list(),
            self.pos_sp_o_adjacency_list(),
            subject,
            predicate,
            object,
        )
    }

    pub fn internal_triple_removal_exists(
        &self,
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> bool {
        match (
            self.neg_subjects(),
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (neg_subject, Some(neg_s_p_adjacency_list), Some(neg_sp_o_adjacency_list)) => {
                layer_triple_exists(
                    neg_subject,
                    neg_s_p_adjacency_list,
                    neg_sp_o_adjacency_list,
                    subject,
                    predicate,
                    object,
                )
            }
            _ => false,
        }
    }

    pub fn internal_triple_additions(&self) -> OptInternalLayerTripleSubjectIterator {
        OptInternalLayerTripleSubjectIterator(Some(InternalLayerTripleSubjectIterator::new(
            self.pos_subjects().cloned(),
            self.pos_s_p_adjacency_list().clone(),
            self.pos_sp_o_adjacency_list().clone(),
        )))
    }

    pub fn internal_triple_removals(&self) -> OptInternalLayerTripleSubjectIterator {
        OptInternalLayerTripleSubjectIterator(
            match (
                self.neg_subjects(),
                self.neg_s_p_adjacency_list(),
                self.neg_sp_o_adjacency_list(),
            ) {
                (neg_subjects, Some(neg_s_p_adjacency_list), Some(neg_sp_o_adjacency_list)) => {
                    Some(InternalLayerTripleSubjectIterator::new(
                        neg_subjects.cloned(),
                        neg_s_p_adjacency_list.clone(),
                        neg_sp_o_adjacency_list.clone(),
                    ))
                }
                _ => None,
            },
        )
    }

    pub fn internal_triple_additions_s(
        &self,
        subject: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    pub fn internal_triple_removals_s(
        &self,
        subject: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    pub fn internal_triple_additions_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions()
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.predicate == predicate && t.subject == subject),
        )
    }

    pub fn internal_triple_removals_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals()
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.predicate == predicate && t.subject == subject),
        )
    }

    pub fn internal_triple_additions_p(
        &self,
        predicate: u64,
    ) -> OptInternalLayerTriplePredicateIterator {
        match self.pos_predicate_wavelet_tree().lookup(predicate) {
            Some(lookup) => OptInternalLayerTriplePredicateIterator(Some(
                InternalLayerTriplePredicateIterator::new(
                    lookup,
                    self.pos_subjects().cloned(),
                    self.pos_s_p_adjacency_list().clone(),
                    self.pos_sp_o_adjacency_list().clone(),
                ),
            )),
            None => OptInternalLayerTriplePredicateIterator(None),
        }
    }

    pub fn internal_triple_removals_p(
        &self,
        predicate: u64,
    ) -> OptInternalLayerTriplePredicateIterator {
        match (
            self.neg_predicate_wavelet_tree()
                .and_then(|t| t.lookup(predicate)),
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
        ) {
            (Some(lookup), Some(s_p_adjacency_list), Some(sp_o_adjacency_list)) => {
                OptInternalLayerTriplePredicateIterator(Some(
                    InternalLayerTriplePredicateIterator::new(
                        lookup,
                        self.neg_subjects().cloned(),
                        s_p_adjacency_list.clone(),
                        sp_o_adjacency_list.clone(),
                    ),
                ))
            }
            _ => OptInternalLayerTriplePredicateIterator(None),
        }
    }

    pub fn internal_triple_additions_o(
        &self,
        object: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_additions_by_object()
                .seek_object(object)
                .stop_at_boundary(true),
        )
    }

    pub fn internal_triple_additions_by_object(&self) -> OptInternalLayerTripleObjectIterator {
        OptInternalLayerTripleObjectIterator(Some(InternalLayerTripleObjectIterator::new(
            self.pos_subjects().cloned(),
            self.pos_objects().cloned(),
            self.pos_o_ps_adjacency_list().clone(),
            self.pos_s_p_adjacency_list().clone(),
            false,
        )))
    }

    pub fn internal_triple_removals_o(
        &self,
        object: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            self.internal_triple_removals_by_object()
                .seek_object(object)
                .stop_at_boundary(true),
        )
    }

    pub fn internal_triple_removals_by_object(&self) -> OptInternalLayerTripleObjectIterator {
        OptInternalLayerTripleObjectIterator(
            match (
                self.neg_subjects(),
                self.neg_objects(),
                self.neg_o_ps_adjacency_list(),
                self.neg_s_p_adjacency_list(),
            ) {
                (
                    neg_subjects,
                    neg_objects,
                    Some(neg_o_ps_adjacency_list),
                    Some(neg_s_p_adjacency_list),
                ) => Some(InternalLayerTripleObjectIterator::new(
                    neg_subjects.cloned(),
                    neg_objects.cloned(),
                    neg_o_ps_adjacency_list.clone(),
                    neg_s_p_adjacency_list.clone(),
                    false,
                )),
                _ => None,
            },
        )
    }

    pub fn internal_triple_layer_addition_count(&self) -> usize {
        self.pos_sp_o_adjacency_list().right_count()
            - self
                .pos_predicate_wavelet_tree()
                .lookup(0)
                .map(|l| l.len())
                .unwrap_or(0)
    }

    pub fn internal_triple_layer_removal_count(&self) -> usize {
        match self.neg_sp_o_adjacency_list() {
            None => 0,
            Some(adjacency_list) => adjacency_list.right_count()
                - self.neg_predicate_wavelet_tree().expect("negative wavelet tree should exist when negative sp_o adjacency list exists")
                .lookup(0).map(|l|l.len()).unwrap_or(0)
        }
    }

    /// Count triple additions in this layer's own positive index that match the
    /// given partially-bound pattern, without enumerating them.
    pub fn internal_triple_pattern_addition_count(
        &self,
        subject: Option<u64>,
        predicate: Option<u64>,
        object: Option<u64>,
    ) -> usize {
        internal_layer_triple_count(
            LayerCountIndexes {
                subjects: self.pos_subjects(),
                objects: self.pos_objects(),
                s_p_adjacency_list: self.pos_s_p_adjacency_list(),
                sp_o_adjacency_list: self.pos_sp_o_adjacency_list(),
                o_ps_adjacency_list: self.pos_o_ps_adjacency_list(),
                predicate_wavelet_tree: Some(self.pos_predicate_wavelet_tree()),
            },
            subject,
            predicate,
            object,
        )
    }

    /// Count triple removals in this layer's own negative index that match the
    /// given partially-bound pattern, without enumerating them.
    pub fn internal_triple_pattern_removal_count(
        &self,
        subject: Option<u64>,
        predicate: Option<u64>,
        object: Option<u64>,
    ) -> usize {
        match (
            self.neg_subjects(),
            self.neg_objects(),
            self.neg_s_p_adjacency_list(),
            self.neg_sp_o_adjacency_list(),
            self.neg_o_ps_adjacency_list(),
            self.neg_predicate_wavelet_tree(),
        ) {
            (subjects, objects, Some(s_p), Some(sp_o), Some(o_ps), wavelet) => {
                internal_layer_triple_count(
                    LayerCountIndexes {
                        subjects,
                        objects,
                        s_p_adjacency_list: s_p,
                        sp_o_adjacency_list: sp_o,
                        o_ps_adjacency_list: o_ps,
                        predicate_wavelet_tree: wavelet,
                    },
                    subject,
                    predicate,
                    object,
                )
            }
            _ => 0,
        }
    }

    pub fn immediate_layers(&self) -> Vec<&InternalLayer> {
        let mut layer = Some(self);
        let mut result = Vec::new();

        while let Some(l) = layer {
            result.push(l);

            layer = l.immediate_parent();
        }

        result.reverse();

        result
    }

    /// Collects the global object ids whose value falls in the half-open range
    /// [low, high), across this layer and all its parents.
    ///
    /// Returns an empty vec if the bounds have different datatypes or the range
    /// is empty.
    pub fn value_range_object_ids(&self, low: &TypedDictEntry, high: &TypedDictEntry) -> Vec<u64> {
        self.value_range_object_ids_inner(low, high, false)
    }

    /// Object ids whose value falls in `low..high`, resolving the typed
    /// bounds per layer into a value-dictionary position range. `rev`
    /// reverses the position order within each layer (layer order is kept).
    fn value_range_object_ids_inner(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
        rev: bool,
    ) -> Vec<u64> {
        if low.datatype() != high.datatype() {
            return Vec::new();
        }

        let dt = low.datatype();
        let layers = self.immediate_layers();
        let mut object_ids = Vec::new();
        let mut parent_count = 0u64;

        for layer in layers.iter() {
            let node_dict_len = layer.node_dict_len() as u64;
            let value_dict_len = layer.value_dict_len() as u64;

            let value_dict = layer.value_dictionary();
            let type_offset = match value_dict.type_segment(dt) {
                Some((_, offset)) => offset,
                None => {
                    parent_count += node_dict_len + value_dict_len;
                    continue;
                }
            };

            let start = match value_dict.id_entry(low) {
                IdLookupResult::Found(i) => i,
                IdLookupResult::Closest(i) => i + 1,
                IdLookupResult::NotFound => type_offset + 1,
            };

            let end = match value_dict.id_entry(high) {
                IdLookupResult::Found(i) => i,
                IdLookupResult::Closest(i) => i + 1,
                IdLookupResult::NotFound => type_offset + 1,
            };

            let id_map = layer.node_value_id_map();
            let positions: Box<dyn Iterator<Item = u64>> = if rev {
                Box::new((start..end).rev())
            } else {
                Box::new(start..end)
            };
            for pos in positions {
                let inner_id = pos + node_dict_len;
                let outer_id = id_map.inner_to_outer(inner_id);
                let global_id = outer_id + parent_count;
                object_ids.push(global_id);
            }

            parent_count += node_dict_len + value_dict_len;
        }

        object_ids
    }

    pub fn immediate_layers_upto(&self, upto_layer_id: [u32; 5]) -> Vec<&InternalLayer> {
        if self.name() == upto_layer_id {
            panic!("tried to retrieve layers up to a boundary, but boundary was the top layer");
        }

        let mut layer = Some(self);
        let mut result = Vec::new();

        while let Some(l) = layer {
            if l.name() == upto_layer_id {
                break;
            }
            result.push(l);

            layer = l.immediate_parent();
        }

        if layer.is_none() {
            // we went through the whole stack and we did not find the boundary.
            panic!("tried to find all layers up to a boundary, but boundary was not found");
        }

        result.reverse();

        result
    }

    pub fn is_rollup(&self) -> bool {
        matches!(self, Rollup(_))
    }
}

impl Layer for InternalLayer {
    fn name(&self) -> [u32; 5] {
        self.name()
    }

    fn parent_name(&self) -> Option<[u32; 5]> {
        self.parent_name()
    }

    fn node_and_value_count(&self) -> usize {
        self.parent_node_value_count()
            + self.node_dictionary().num_entries()
            + self.value_dictionary().num_entries()
    }

    fn predicate_count(&self) -> usize {
        self.parent_predicate_count() + self.predicate_dictionary().num_entries()
    }

    fn subject_id<'a>(&'a self, subject: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer
                    .node_dict_id(subject)
                    .into_option()
                    .map(|id| layer.node_value_id_map().inner_to_outer(id)),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn predicate_id<'a>(&'a self, predicate: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer
                    .predicate_dict_id(predicate)
                    .into_option()
                    .map(|id| layer.predicate_id_map().inner_to_outer(id)),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| id + parent_option.map_or(0, |p| p.predicate_count() as u64))
    }

    fn object_node_id<'a>(&'a self, object: &str) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer
                    .node_dict_id(object)
                    .into_option()
                    .map(|id| layer.node_value_id_map().inner_to_outer(id)),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn object_value_id<'a>(&'a self, object: &TypedDictEntry) -> Option<u64> {
        let to_result = |layer: &'a InternalLayer| {
            (
                layer.value_dict_id(object).into_option().map(|i| {
                    layer
                        .node_value_id_map()
                        .inner_to_outer(i + layer.node_dict_len() as u64)
                }),
                layer.immediate_parent(),
            )
        };
        let mut result = to_result(self);
        while let (None, Some(layer)) = result {
            result = to_result(layer);
        }
        let (id_option, parent_option) = result;
        id_option.map(|id| id + parent_option.map_or(0, |p| p.node_and_value_count() as u64))
    }

    fn id_subject(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id;
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;
                if corrected_id > parent_count {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            return current_layer.node_dict_get(
                current_layer
                    .node_value_id_map()
                    .outer_to_inner(corrected_id)
                    .try_into()
                    .unwrap(),
            );
        }

        None
    }

    fn subjects_with_prefix(&self, prefix: &str) -> Box<dyn Iterator<Item = (u64, String)> + Send> {
        let mut bands = Vec::new();
        let mut parent_count = 0u64;
        let prefix = prefix.to_string();
        for (rank, layer) in self.immediate_layers().into_iter().enumerate() {
            let node_dict_len = layer.node_dict_len() as u64;
            let value_dict_len = layer.value_dict_len() as u64;

            let node_dict = layer.node_dictionary();
            let start = dict_lower_bound(node_dict, node_dict_len, &prefix);

            let cursor = BandPrefixCursor {
                layer: layer.clone(),
                base: parent_count,
                pos: start,
                len: node_dict_len,
                prefix: prefix.clone(),
            };
            bands.push((
                rank,
                (Box::new(cursor) as Box<dyn Iterator<Item = (u64, String)> + Send>).peekable(),
            ));
            parent_count += node_dict_len + value_dict_len;
        }

        Box::new(PrefixSubjects::new(bands))
    }

    fn count_subjects_with_prefix(&self, prefix: &str) -> usize {
        // Band dictionaries contain only names introduced in that band, so a
        // subject name occurs in exactly one band. The per-band prefix ranges
        // are therefore disjoint and the count is the sum of range lengths.
        let end_key = prefix_successor(prefix);
        let prefix_owned = prefix.to_string();
        let mut count = 0usize;
        for layer in self.immediate_layers() {
            let node_dict = layer.node_dictionary();
            let len = layer.node_dict_len() as u64;
            let start = dict_lower_bound(node_dict, len, &prefix_owned);
            let end = match end_key.as_ref() {
                Some(end_key) => dict_lower_bound(node_dict, len, end_key),
                None => len + 1,
            };
            if end > start {
                count += (end - start) as usize;
            }
        }
        count
    }

    fn count_triple_subjects_with_prefix(
        &self,
        prefix: &str,
        predicate: u64,
        objects: &std::collections::HashSet<u64>,
    ) -> usize {
        let layers = self.immediate_layers();
        let bands = prefix_band_ranges(&layers, prefix);
        bands
            .iter()
            .enumerate()
            .filter(|(_, a)| a.lo < a.hi)
            .map(|(a_idx, _)| {
                BandMembers::new(member_slices(&layers, &bands, a_idx, predicate, objects)).count()
            })
            .sum()
    }

    fn triple_subjects_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
        predicate: u64,
        objects: &'a std::collections::HashSet<u64>,
    ) -> Box<dyn Iterator<Item = (u64, String)> + Send + 'a> {
        let layers = self.immediate_layers();
        let bands = prefix_band_ranges(&layers, prefix);
        let mut sources = Vec::new();
        for (a_idx, a) in bands.iter().enumerate() {
            if a.lo >= a.hi {
                continue;
            }
            let members =
                BandMembers::new(member_slices(&layers, &bands, a_idx, predicate, objects));
            let layer = layers[a_idx].clone();
            let base = a.base;
            let source: Box<dyn Iterator<Item = (u64, String)> + Send> = if a.identity {
                // outer = base + dict position, so members arrive in
                // name order and the name is a direct dict lookup.
                Box::new(members.filter_map(move |(subject, _net)| {
                    layer
                        .node_dict_get((subject - base) as usize)
                        .map(|name| (subject, name))
                }))
            } else {
                // shortcut: wavelet bands permute outer ids, so members
                // arrive out of name order — buffer and sort this band
                // (bounded by members in the band; upgrade to an
                // external merge if rollup bands get huge).
                let map = layer.node_value_id_map().clone();
                let mut items: Vec<(u64, String)> = members
                    .filter_map(|(subject, _net)| {
                        let inner = map.outer_to_inner(subject - base);
                        layer
                            .node_dict_get(inner as usize)
                            .map(|name| (subject, name))
                    })
                    .collect();
                items.sort_by(|x, y| x.1.cmp(&y.1));
                Box::new(items.into_iter())
            };
            sources.push((a_idx, source.peekable()));
        }
        Box::new(PrefixSubjects::new(sources))
    }

    fn id_predicate(&self, id: u64) -> Option<String> {
        if id == 0 {
            return None;
        }
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.predicate_count() as u64;
        while let Some(current_layer) = current_option {
            let mut corrected_id = id;
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count -= current_layer.predicate_dict_len() as u64;
                if corrected_id > parent_count {
                    // subject, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            return current_layer.predicate_dict_get(
                current_layer
                    .predicate_id_map()
                    .outer_to_inner(corrected_id)
                    .try_into()
                    .unwrap(),
            );
        }

        None
    }

    fn id_object(&self, id: u64) -> Option<ObjectType> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id;
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;

                if corrected_id > parent_count {
                    // object, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            corrected_id = current_layer
                .node_value_id_map()
                .outer_to_inner(corrected_id);

            if corrected_id > current_layer.node_dict_len() as u64 {
                // object, if it exists, must be a value
                corrected_id -= current_layer.node_dict_len() as u64;
                return current_layer
                    .value_dict_get(corrected_id.try_into().unwrap())
                    .map(ObjectType::Value);
            } else {
                return current_layer
                    .node_dict_get(corrected_id.try_into().unwrap())
                    .map(ObjectType::Node);
            }
        }

        None
    }

    fn id_object_is_node(&self, id: u64) -> Option<bool> {
        if id == 0 {
            return None;
        }
        let mut corrected_id = id;
        let mut current_option: Option<&InternalLayer> = Some(self);
        let mut parent_count = self.node_and_value_count() as u64;
        while let Some(current_layer) = current_option {
            if let Some(parent) = current_layer.immediate_parent() {
                parent_count = parent_count
                    - current_layer.node_dict_len() as u64
                    - current_layer.value_dict_len() as u64;

                if corrected_id > parent_count {
                    // object, if it exists, is in this layer
                    corrected_id -= parent_count;
                } else {
                    current_option = Some(parent);
                    continue;
                }
            }

            corrected_id = current_layer
                .node_value_id_map()
                .outer_to_inner(corrected_id);

            if corrected_id
                > (current_layer.node_dict_len() + current_layer.value_dict_len()) as u64
            {
                return None;
            }
            if corrected_id > current_layer.node_dict_len() as u64 {
                // object, if it exists, must be a value
                return Some(false);
            }

            return Some(true);
        }

        None
    }

    fn clone_boxed(&self) -> Box<dyn Layer> {
        Box::new(self.clone())
    }

    fn triple_addition_count(&self) -> usize {
        let mut additions = self.internal_triple_layer_addition_count();

        let mut parent = self.immediate_parent();
        while parent.is_some() {
            additions += parent.unwrap().internal_triple_layer_addition_count();

            parent = parent.unwrap().immediate_parent();
        }

        additions
    }

    fn triple_removal_count(&self) -> usize {
        let mut removals = self.internal_triple_layer_removal_count();

        let mut parent = self.immediate_parent();
        while parent.is_some() {
            removals += parent.unwrap().internal_triple_layer_removal_count();

            parent = parent.unwrap().immediate_parent();
        }

        removals
    }

    fn all_counts(&self) -> LayerCounts {
        let mut node_count = self.node_dict_len();
        let mut predicate_count = self.predicate_dict_len();
        let mut value_count = self.value_dict_len();
        let mut parent_option = self.immediate_parent();
        while let Some(parent) = parent_option {
            node_count += parent.node_dict_len();
            predicate_count += parent.predicate_dict_len();
            value_count += parent.value_dict_len();
            parent_option = parent.immediate_parent();
        }
        LayerCounts {
            node_count,
            predicate_count,
            value_count,
        }
    }

    fn triple_exists(&self, subject: u64, predicate: u64, object: u64) -> bool {
        if subject == 0 || predicate == 0 || object == 0 {
            return false;
        }

        if self.internal_triple_addition_exists(subject, predicate, object) {
            true
        } else if self.internal_triple_removal_exists(subject, predicate, object) {
            false
        } else {
            let mut parent_opt = self.immediate_parent();
            while parent_opt.is_some() {
                let parent = parent_opt.unwrap();
                if parent.internal_triple_addition_exists(subject, predicate, object) {
                    return true;
                } else if parent.internal_triple_removal_exists(subject, predicate, object) {
                    return false;
                }

                parent_opt = parent.immediate_parent();
            }

            false
        }
    }

    fn triples(&self) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(InternalTripleSubjectIterator::from_layer(self))
    }

    fn triples_s(&self, subject: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            InternalTripleSubjectIterator::from_layer(self)
                .seek_subject(subject)
                .take_while(move |t| t.subject == subject),
        )
    }

    fn triples_sp(
        &self,
        subject: u64,
        predicate: u64,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            InternalTripleSubjectIterator::from_layer(self)
                .seek_subject_predicate(subject, predicate)
                .take_while(move |t| t.subject == subject && t.predicate == predicate),
        )
    }

    fn triples_p(&self, predicate: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(InternalTriplePredicateIterator::from_layer(self, predicate))
    }

    fn triples_o(&self, object: u64) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        Box::new(
            InternalTripleObjectIterator::from_layer(self)
                .seek_object(object)
                .take_while(move |t| t.object == object),
        )
    }

    fn triples_value_range(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let object_ids = self.value_range_object_ids(low, high);
        let layer_clone = self.clone();
        Box::new(object_ids.into_iter().flat_map(move |oid| {
            InternalTripleObjectIterator::from_layer(&layer_clone)
                .seek_object(oid)
                .take_while(move |t| t.object == oid)
        }))
    }

    // Counts the distinct id-triples matching the pattern that are VISIBLE at
    // this layer. There is no "changed" triple state: layers are immutable
    // deltas, so a change is a removal of one id-triple plus an addition of
    // another. For a given id-triple, add/remove events alternate down the
    // stack (a layer can only remove what a parent provides, and only add what
    // is absent), so summing per-layer additions minus per-layer removals
    // yields set membership — the same accounting `triple_count()` uses.
    fn count_triples(
        &self,
        subject: Option<u64>,
        predicate: Option<u64>,
        object: Option<u64>,
    ) -> usize {
        let mut additions = 0;
        let mut removals = 0;
        let mut layer_opt = Some(self);
        while let Some(layer) = layer_opt {
            additions += layer.internal_triple_pattern_addition_count(subject, predicate, object);
            removals += layer.internal_triple_pattern_removal_count(subject, predicate, object);
            layer_opt = layer.immediate_parent();
        }

        additions - removals
    }

    fn count_triples_value_range(
        &self,
        subject: Option<u64>,
        predicate: Option<u64>,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> usize {
        // shortcut: materializes the object ids in range and does a stack
        // walk per id — O(#objects-in-range * #layers) index seeks plus an
        // 8-byte-per-object vec. Same order as enumeration with smaller
        // constants; beating it needs an index keyed on (object range,
        // predicate), which the store doesn't have. Upgrade when range
        // counts over wide value domains become a hot path.
        self.value_range_object_ids(low, high)
            .into_iter()
            .map(|oid| self.count_triples(subject, predicate, Some(oid)))
            .sum()
    }

    fn triples_value_range_rev(
        &self,
        low: &TypedDictEntry,
        high: &TypedDictEntry,
    ) -> Box<dyn Iterator<Item = IdTriple> + Send> {
        let object_ids = self.value_range_object_ids_inner(low, high, true);
        let layer_clone = self.clone();
        Box::new(object_ids.into_iter().flat_map(move |oid| {
            InternalTripleObjectIterator::from_layer(&layer_clone)
                .seek_object(oid)
                .take_while(move |t| t.object == oid)
        }))
    }

    fn stored_size(&self) -> usize {
        InternalLayer::stored_size(self)
    }

    fn single_triple_sp(&self, subject: u64, predicate: u64) -> Option<IdTriple> {
        // is subject/predicate in the positives? we're in luck
        if let Some(pos) = sp_o_position(
            self.pos_subjects(),
            self.pos_s_p_adjacency_list(),
            self.pos_sp_o_adjacency_list(),
            subject,
            predicate,
        ) {
            return Some(IdTriple {
                subject,
                predicate,
                object: self.pos_sp_o_adjacency_list().num_at_pos(pos),
            });
        }

        // alas, it's not in there.
        let mut exclude = HashSet::new();
        let mut l = self;
        loop {
            if let Some(parent) = l.immediate_parent() {
                if let Some(neg_sp_o_adjacency_list) = l.neg_sp_o_adjacency_list() {
                    if let Some(mut pos) = sp_o_position(
                        l.neg_subjects(),
                        l.neg_s_p_adjacency_list().unwrap(),
                        neg_sp_o_adjacency_list,
                        subject,
                        predicate,
                    ) {
                        loop {
                            exclude.insert(neg_sp_o_adjacency_list.num_at_pos(pos));
                            if neg_sp_o_adjacency_list.bit_at_pos(pos) {
                                break;
                            }
                            pos += 1;
                        }
                    }
                }

                let pos_sp_o_adjacency_list = parent.pos_sp_o_adjacency_list();
                if let Some(mut pos) = sp_o_position(
                    parent.pos_subjects(),
                    parent.pos_s_p_adjacency_list(),
                    pos_sp_o_adjacency_list,
                    subject,
                    predicate,
                ) {
                    // we need to iterate through the positives until we find an element that is not excluded
                    loop {
                        let num = pos_sp_o_adjacency_list.num_at_pos(pos);
                        if !exclude.contains(&num) {
                            return Some(IdTriple {
                                subject,
                                predicate,
                                object: num,
                            });
                        }

                        if pos_sp_o_adjacency_list.bit_at_pos(pos) {
                            break;
                        }
                        pos += 1;
                    }
                }

                l = parent;
            } else {
                return None;
            }
        }
    }
}

impl From<BaseLayer> for InternalLayer {
    fn from(layer: BaseLayer) -> InternalLayer {
        InternalLayer::Base(layer)
    }
}

impl From<ChildLayer> for InternalLayer {
    fn from(layer: ChildLayer) -> InternalLayer {
        InternalLayer::Child(layer)
    }
}

impl From<RollupLayer> for InternalLayer {
    fn from(layer: RollupLayer) -> InternalLayer {
        InternalLayer::Rollup(layer)
    }
}

fn sp_o_position(
    subjects: Option<&MonotonicLogArray>,
    s_p_adjacency_list: &AdjacencyList,
    sp_o_adjacency_list: &AdjacencyList,
    subject: u64,
    predicate: u64,
) -> Option<u64> {
    if subject == 0 || predicate == 0 {
        return None;
    }

    let s_position = match subjects {
        None => {
            if subject > s_p_adjacency_list.left_count() as u64 {
                return None;
            } else {
                subject - 1
            }
        }
        Some(subjects) => match subjects.index_of(subject) {
            Some(pos) => pos as u64,
            None => return None,
        },
    };

    let mut s_p_position = s_p_adjacency_list.offset_for(s_position + 1);
    loop {
        let bit = s_p_adjacency_list.bit_at_pos(s_p_position);
        if s_p_adjacency_list.num_at_pos(s_p_position) == predicate {
            break;
        }

        if bit {
            // moved past the end for this subject. triple isn't here.
            return None;
        }

        s_p_position += 1;
    }

    Some(sp_o_adjacency_list.offset_for(s_p_position + 1))
}

pub(crate) fn layer_triple_exists(
    subjects: Option<&MonotonicLogArray>,
    s_p_adjacency_list: &AdjacencyList,
    sp_o_adjacency_list: &AdjacencyList,
    subject: u64,
    predicate: u64,
    object: u64,
) -> bool {
    if object == 0 {
        return false;
    }

    let mut sp_o_position = match sp_o_position(
        subjects,
        s_p_adjacency_list,
        sp_o_adjacency_list,
        subject,
        predicate,
    ) {
        Some(p) => p,
        None => return false,
    };

    loop {
        let bit = sp_o_adjacency_list.bit_at_pos(sp_o_position);
        if sp_o_adjacency_list.num_at_pos(sp_o_position) == object {
            // yay we found it
            return true;
        }

        if bit {
            // moved past the end for this subject-predicate pair. triple isn't here.
            break;
        }

        sp_o_position += 1;
    }

    false
}

/// Maps an id to its 0-based left position in an adjacency list.
///
/// `array` is the sparse id map used when a layer does not store every
/// possible id as a left (child layers, or sparse base layers). When it is
/// `None`, lefts are the ids themselves.
fn sparse_position(array: Option<&MonotonicLogArray>, value: u64, left_count: u64) -> Option<u64> {
    if value == 0 {
        return None;
    }

    match array {
        None => {
            if value > left_count {
                None
            } else {
                Some(value - 1)
            }
        }
        Some(array) => array.index_of(value).map(|i| i as u64),
    }
}

/// Returns the [start, end) positions of the segment belonging to the given
/// 0-based left position in an adjacency list.
fn segment_bounds(adjacency_list: &AdjacencyList, left_position: u64) -> (u64, u64) {
    let start = adjacency_list.offset_for(left_position + 1);
    let end = adjacency_list.bits().select1(left_position + 1).unwrap() + 1;

    (start, end)
}

/// Returns the first position in `nums[lo..hi)` whose value is not less than
/// `value`, or `hi` if there is none. Requires the range to be sorted.
fn lower_bound(adjacency_list: &AdjacencyList, value: u64, lo: u64, hi: u64) -> u64 {
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if adjacency_list.num_at_pos(mid) < value {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    lo
}

/// Counts triples matching a partially-bound pattern in a single internal
/// layer's index set, without enumerating the matching triples.
///
/// This works on either the positive or the negative structures of a layer.
/// The s_p list may contain placeholder positions (predicate 0), each of which
/// pairs with exactly one sp_o entry; these are subtracted out where the
/// counted range can contain them.
/// Borrows of the index structures of a single internal layer, either the
/// positive or the negative set.
struct LayerCountIndexes<'a> {
    subjects: Option<&'a MonotonicLogArray>,
    objects: Option<&'a MonotonicLogArray>,
    s_p_adjacency_list: &'a AdjacencyList,
    sp_o_adjacency_list: &'a AdjacencyList,
    o_ps_adjacency_list: &'a AdjacencyList,
    predicate_wavelet_tree: Option<&'a WaveletTree>,
}

fn internal_layer_triple_count(
    indexes: LayerCountIndexes,
    subject: Option<u64>,
    predicate: Option<u64>,
    object: Option<u64>,
) -> usize {
    let LayerCountIndexes {
        subjects,
        objects,
        s_p_adjacency_list,
        sp_o_adjacency_list,
        o_ps_adjacency_list,
        predicate_wavelet_tree,
    } = indexes;
    match (subject, predicate, object) {
        (None, None, None) => {
            sp_o_adjacency_list.right_count()
                - predicate_wavelet_tree
                    .and_then(|t| t.lookup(0))
                    .map(|l| l.len())
                    .unwrap_or(0)
        }
        (Some(s), p, o) => {
            let s_position =
                match sparse_position(subjects, s, s_p_adjacency_list.left_count() as u64) {
                    Some(pos) => pos,
                    None => return 0,
                };
            let (sp_start, sp_end) = segment_bounds(s_p_adjacency_list, s_position);
            match p {
                Some(p) => {
                    if p == 0 {
                        return 0;
                    }
                    // predicates within a subject segment are sorted
                    let sp_pos = lower_bound(s_p_adjacency_list, p, sp_start, sp_end);
                    if sp_pos == sp_end || s_p_adjacency_list.num_at_pos(sp_pos) != p {
                        return 0;
                    }
                    let (o_start, o_end) = segment_bounds(sp_o_adjacency_list, sp_pos);
                    match o {
                        None => (o_end - o_start) as usize,
                        Some(o) => {
                            let pos = lower_bound(sp_o_adjacency_list, o, o_start, o_end);
                            (pos < o_end && sp_o_adjacency_list.num_at_pos(pos) == o) as usize
                        }
                    }
                }
                None => {
                    let o_start = sp_o_adjacency_list.offset_for(sp_start + 1);
                    let o_end = sp_o_adjacency_list
                        .bits()
                        .select1(sp_end)
                        .map(|e| e + 1)
                        .unwrap_or(o_start);
                    match o {
                        None => {
                            let total = (o_end - o_start) as usize;
                            let zeros = predicate_wavelet_tree
                                .and_then(|t| t.lookup(0))
                                .map(|l| {
                                    l.iter()
                                        .take_while(|&pos| pos < sp_end)
                                        .filter(|pos| *pos >= sp_start)
                                        .count()
                                })
                                .unwrap_or(0);
                            total - zeros
                        }
                        Some(o) => (o_start..o_end)
                            .filter(|&pos| sp_o_adjacency_list.num_at_pos(pos) == o)
                            .count(),
                    }
                }
            }
        }
        (None, None, Some(o)) => {
            let o_position =
                match sparse_position(objects, o, o_ps_adjacency_list.left_count() as u64) {
                    Some(pos) => pos,
                    None => return 0,
                };
            let (ps_start, ps_end) = segment_bounds(o_ps_adjacency_list, o_position);
            // shortcut: nums are sorted, so any zero padding sits at the
            // segment start and is skipped by counting from the first
            // nonzero position
            let start = lower_bound(o_ps_adjacency_list, 1, ps_start, ps_end);
            (ps_end - start) as usize
        }
        (None, Some(p), Some(o)) => {
            if p == 0 {
                return 0;
            }
            let o_position =
                match sparse_position(objects, o, o_ps_adjacency_list.left_count() as u64) {
                    Some(pos) => pos,
                    None => return 0,
                };
            let (ps_start, ps_end) = segment_bounds(o_ps_adjacency_list, o_position);
            (ps_start..ps_end)
                .filter(|&pos| {
                    let sp_pair_num = o_ps_adjacency_list.num_at_pos(pos);
                    sp_pair_num != 0 && s_p_adjacency_list.pair_at_pos(sp_pair_num - 1).1 == p
                })
                .count()
        }
        (None, Some(p), None) => {
            if p == 0 {
                return 0;
            }
            // count the sp_o segment for every s_p position tagged with p
            match predicate_wavelet_tree.and_then(|t| t.lookup(p)) {
                None => 0,
                Some(lookup) => (0..lookup.len())
                    .map(|i| {
                        let sp_pos = lookup.entry(i);
                        let (o_start, o_end) = segment_bounds(sp_o_adjacency_list, sp_pos);
                        (o_end - o_start) as usize
                    })
                    .sum(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::open_directory_store;
    use crate::store::sync::*;

    fn create_base_layer(store: &SyncStore) -> SyncStoreLayer {
        let builder = store.create_base_layer().unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();

        builder.commit().unwrap()
    }

    #[test]
    fn base_layer_addition_count() {
        let store = open_sync_memory_store();

        let layer = create_base_layer(&store);

        assert_eq!(3, layer.triple_layer_addition_count().unwrap());
    }

    #[test]
    fn child_layer_addition_removal_count() {
        let store = open_sync_memory_store();
        let base_layer = create_base_layer(&store);
        let builder = base_layer.open_write().unwrap();

        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("horse", "says", "neigh"))
            .unwrap();

        let layer = builder.commit().unwrap();

        assert_eq!(1, layer.triple_layer_addition_count().unwrap());
        assert_eq!(1, layer.triple_layer_removal_count().unwrap());
    }

    use crate::layer::base::base_tests::*;
    #[tokio::test]
    async fn base_layer_with_gaps_addition_count() {
        let files = base_layer_files();

        let nodes = vec!["aaaaa", "baa", "bbbbb", "ccccc", "mooo"];
        let predicates = vec!["abcde", "fghij", "klmno", "lll"];
        let values = vec!["chicken", "cow", "dog", "pig", "zebra"];

        let mut builder = BaseLayerFileBuilder::from_files(&files).await.unwrap();
        builder.add_nodes(nodes.into_iter().map(|s| s.to_string()));
        builder.add_predicates(predicates.into_iter().map(|s| s.to_string()));
        builder.add_values(values.into_iter().map(|s| String::make_entry(&s)));
        let mut builder = builder.into_phase2().await.unwrap();
        builder.add_triple(3, 3, 3).await.unwrap();
        builder.finalize().await.unwrap();

        let layer = BaseLayer::load_from_files([1, 2, 3, 4, 5], &files)
            .await
            .unwrap();

        assert_eq!(1, layer.internal_triple_layer_addition_count());
    }

    fn create_counting_base_layer(store: &SyncStore) -> SyncStoreLayer {
        let builder = store.create_base_layer().unwrap();

        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("cow", "says", "sniff"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("cow", "likes", "duck"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("duck", "says", "quack"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "cow"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("pig", "says", "oink"))
            .unwrap();

        builder.commit().unwrap()
    }

    #[test]
    fn count_triples_base_layer() {
        let store = open_sync_memory_store();
        let layer = create_counting_base_layer(&store);

        let cow = layer.subject_id("cow").unwrap();
        let duck = layer.subject_id("duck").unwrap();
        let pig = layer.subject_id("pig").unwrap();
        let says = layer.predicate_id("says").unwrap();
        let likes = layer.predicate_id("likes").unwrap();
        let moo = layer
            .object_value_id(&String::make_entry(&"moo".to_string()))
            .unwrap();
        let quack = layer
            .object_value_id(&String::make_entry(&"quack".to_string()))
            .unwrap();

        assert_eq!(6, layer.count_triples(None, None, None));
        assert_eq!(3, layer.count_triples(Some(cow), None, None));
        assert_eq!(2, layer.count_triples(Some(cow), Some(says), None));
        assert_eq!(1, layer.count_triples(Some(cow), Some(likes), None));
        assert_eq!(1, layer.count_triples(Some(cow), Some(says), Some(moo)));
        assert_eq!(0, layer.count_triples(Some(cow), Some(says), Some(quack)));
        assert_eq!(1, layer.count_triples(Some(cow), None, Some(duck)));
        assert_eq!(1, layer.count_triples(Some(cow), None, Some(moo)));
        assert_eq!(4, layer.count_triples(None, Some(says), None));
        assert_eq!(2, layer.count_triples(None, Some(likes), None));
        assert_eq!(1, layer.count_triples(None, Some(says), Some(moo)));
        assert_eq!(1, layer.count_triples(None, Some(likes), Some(cow)));
        assert_eq!(1, layer.count_triples(None, None, Some(duck)));
        assert_eq!(1, layer.count_triples(None, None, Some(quack)));
        // "pig" is a dictionary node but never an object: its o_ps segment
        // is zero padding, which must not count
        let pig_as_object = layer.object_node_id("pig").unwrap();
        assert_eq!(0, layer.count_triples(None, None, Some(pig_as_object)));
        assert_eq!(0, layer.count_triples(Some(9999), None, None));
        assert_eq!(0, layer.count_triples(None, Some(9999), None));
        assert_eq!(0, layer.count_triples(None, None, Some(9999)));
        assert_eq!(0, layer.count_triples(Some(pig), Some(likes), None));
    }

    #[test]
    fn count_triples_child_layer() {
        let store = open_sync_memory_store();
        let base = create_counting_base_layer(&store);
        let builder = base.open_write().unwrap();

        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("horse", "says", "neigh"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("duck", "likes", "pig"))
            .unwrap();

        let layer = builder.commit().unwrap();

        let cow = layer.subject_id("cow").unwrap();
        let duck = layer.subject_id("duck").unwrap();
        let pig = layer.subject_id("pig").unwrap();
        let says = layer.predicate_id("says").unwrap();
        let likes = layer.predicate_id("likes").unwrap();
        let moo = layer
            .object_value_id(&String::make_entry(&"moo".to_string()))
            .unwrap();
        let neigh = layer
            .object_value_id(&String::make_entry(&"neigh".to_string()))
            .unwrap();

        // 6 base - 1 removal + 2 additions
        assert_eq!(7, layer.count_triples(None, None, None));
        assert_eq!(2, layer.count_triples(Some(cow), None, None));
        assert_eq!(1, layer.count_triples(Some(cow), Some(says), None));
        assert_eq!(0, layer.count_triples(Some(cow), Some(says), Some(moo)));
        assert_eq!(4, layer.count_triples(None, Some(says), None));
        assert_eq!(0, layer.count_triples(None, Some(says), Some(moo)));
        assert_eq!(1, layer.count_triples(None, Some(says), Some(neigh)));
        assert_eq!(2, layer.count_triples(Some(duck), Some(likes), None));
        assert_eq!(3, layer.count_triples(None, Some(likes), None));
        assert_eq!(1, layer.count_triples(None, Some(likes), Some(pig)));
        assert_eq!(1, layer.count_triples(None, None, Some(pig)));
    }

    #[test]
    fn count_triples_value_range_base_layer() {
        let store = open_sync_memory_store();
        let layer = create_counting_base_layer(&store);

        let cow = layer.subject_id("cow").unwrap();
        let says = layer.predicate_id("says").unwrap();

        let low = String::make_entry(&"a".to_string());
        let high = String::make_entry(&"z".to_string());

        // all string-valued objects fall in the range: moo, sniff, quack, oink
        assert_eq!(4, layer.count_triples_value_range(None, None, &low, &high));
        assert_eq!(
            4,
            layer.count_triples_value_range(None, Some(says), &low, &high)
        );
        assert_eq!(
            2,
            layer.count_triples_value_range(Some(cow), None, &low, &high)
        );
        assert_eq!(
            2,
            layer.count_triples_value_range(Some(cow), Some(says), &low, &high)
        );

        // narrower range: [moo, quack) -> moo, oink
        let low = String::make_entry(&"moo".to_string());
        let high = String::make_entry(&"quack".to_string());
        assert_eq!(2, layer.count_triples_value_range(None, None, &low, &high));
        assert_eq!(
            1,
            layer.count_triples_value_range(Some(cow), None, &low, &high)
        );

        // empty range
        let low = String::make_entry(&"n".to_string());
        let high = String::make_entry(&"n".to_string());
        assert_eq!(0, layer.count_triples_value_range(None, None, &low, &high));
    }

    #[test]
    fn count_triples_value_range_child_layer() {
        let store = open_sync_memory_store();
        let base = create_counting_base_layer(&store);
        let builder = base.open_write().unwrap();

        builder
            .remove_value_triple(ValueTriple::new_string_value("cow", "says", "moo"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("horse", "says", "neigh"))
            .unwrap();

        let layer = builder.commit().unwrap();

        let cow = layer.subject_id("cow").unwrap();
        let says = layer.predicate_id("says").unwrap();

        let low = String::make_entry(&"a".to_string());
        let high = String::make_entry(&"z".to_string());

        // moo removed, neigh added: sniff, quack, oink, neigh
        assert_eq!(4, layer.count_triples_value_range(None, None, &low, &high));
        assert_eq!(
            1,
            layer.count_triples_value_range(Some(cow), None, &low, &high)
        );
        assert_eq!(
            1,
            layer.count_triples_value_range(Some(cow), Some(says), &low, &high)
        );
    }

    fn create_typed_docs_base_layer(store: &SyncStore) -> SyncStoreLayer {
        let builder = store.create_base_layer().unwrap();

        for triple in [
            ValueTriple::new_node("doc/1", "rdf:type", "Type"),
            ValueTriple::new_node("doc/2", "rdf:type", "Type"),
            ValueTriple::new_node("doc/2", "links", "Type"),
            ValueTriple::new_node("doc/3", "rdf:type", "Type"),
            ValueTriple::new_node("other/1", "rdf:type", "Type"),
            ValueTriple::new_node("doc/1", "rdf:type", "Other"),
        ] {
            builder.add_value_triple(triple).unwrap();
        }

        builder.commit().unwrap()
    }

    #[test]
    fn count_typed_prefix_base_layer() {
        let store = open_sync_memory_store();
        let layer = create_typed_docs_base_layer(&store);

        let rdf_type = layer.predicate_id("rdf:type").unwrap();
        let type_obj = layer.object_node_id("Type").unwrap();
        let other_obj = layer.object_node_id("Other").unwrap();

        let types: HashSet<u64> = vec![type_obj].into_iter().collect();
        // doc/1, doc/2, doc/3 have a type edge; other/1 is out of range and
        // doc/2's "links" edge to the same object must not count.
        assert_eq!(
            3,
            layer.count_triple_subjects_with_prefix("doc/", rdf_type, &types)
        );
        assert_eq!(
            1,
            layer.count_triple_subjects_with_prefix("other/", rdf_type, &types)
        );
        assert_eq!(
            0,
            layer.count_triple_subjects_with_prefix("zzz/", rdf_type, &types)
        );

        // doc/1 is a member of both objects but counts once.
        let both: HashSet<u64> = vec![type_obj, other_obj].into_iter().collect();
        assert_eq!(
            3,
            layer.count_triple_subjects_with_prefix("doc/", rdf_type, &both)
        );

        // the streaming form yields members in name order
        let names: Vec<String> = layer
            .triple_subjects_with_prefix("doc/", rdf_type, &types)
            .map(|(_, name)| name)
            .collect();
        assert_eq!(vec!["doc/1", "doc/2", "doc/3"], names);
    }

    #[test]
    fn count_typed_prefix_child_layer() {
        let store = open_sync_memory_store();
        let base = create_typed_docs_base_layer(&store);
        let builder = base.open_write().unwrap();

        for triple in [
            // new names typed in the child band
            ValueTriple::new_node("doc/4", "rdf:type", "Type"),
            ValueTriple::new_node("doc/5", "rdf:type", "Type"),
            // a base-band name typed in the child band
            ValueTriple::new_node("doc/2", "rdf:type", "Other"),
        ] {
            builder.add_value_triple(triple).unwrap();
        }
        builder
            .remove_value_triple(ValueTriple::new_node("doc/3", "rdf:type", "Type"))
            .unwrap();

        let layer = builder.commit().unwrap();

        let rdf_type = layer.predicate_id("rdf:type").unwrap();
        let type_obj = layer.object_node_id("Type").unwrap();
        let other_obj = layer.object_node_id("Other").unwrap();

        let types: HashSet<u64> = vec![type_obj].into_iter().collect();
        // doc/1, doc/2, doc/4, doc/5; doc/3's edge was removed, other/1 is
        // out of range.
        assert_eq!(
            4,
            layer.count_triple_subjects_with_prefix("doc/", rdf_type, &types)
        );
        assert_eq!(
            1,
            layer.count_triple_subjects_with_prefix("other/", rdf_type, &types)
        );
        assert_eq!(
            0,
            layer.count_triple_subjects_with_prefix("zzz/", rdf_type, &types)
        );

        // doc/1 (Type+Other), doc/2 (Type+Other across bands), doc/4, doc/5.
        let both: HashSet<u64> = vec![type_obj, other_obj].into_iter().collect();
        assert_eq!(
            4,
            layer.count_triple_subjects_with_prefix("doc/", rdf_type, &both)
        );

        // the streaming form yields surviving members in name order —
        // doc/3's removal nets it out.
        let names: Vec<String> = layer
            .triple_subjects_with_prefix("doc/", rdf_type, &types)
            .map(|(_, name)| name)
            .collect();
        assert_eq!(vec!["doc/1", "doc/2", "doc/4", "doc/5"], names);
    }

    #[test]
    fn count_typed_prefix_subject_range_skips_unrelated_objects() {
        let store = open_sync_memory_store();
        let layer = create_typed_docs_base_layer(&store);

        let rdf_type = layer.predicate_id("rdf:type").unwrap();
        let other_obj = layer.object_node_id("Other").unwrap();

        // only doc/1 is an instance of Other in range; the Type members do
        // not leak in even though they share the prefix.
        let types: HashSet<u64> = vec![other_obj].into_iter().collect();
        assert_eq!(
            1,
            layer.count_triple_subjects_with_prefix("doc/", rdf_type, &types)
        );
    }

    #[test]
    fn subjects_with_prefix_matches_linear_scan() {
        let store = open_sync_memory_store();
        let builder = store.create_base_layer().unwrap();

        // Four name families dense enough to span several front-coded
        // dict blocks, so prefixes land mid-block.
        let mut all: Vec<String> = Vec::new();
        for fam in ["aa", "ab", "ba", "zz"] {
            for i in 0..50u32 {
                all.push(format!("{fam}/{i:03}"));
            }
        }
        for name in &all {
            builder
                .add_value_triple(ValueTriple::new_node(name, "p", "other/n"))
                .unwrap();
        }
        let layer = builder.commit().unwrap();
        all.push("other/n".to_string());
        all.sort();
        all.dedup();

        let mut prefixes: HashSet<String> = all
            .iter()
            .flat_map(|n| (1..=n.len()).map(move |l| n[..l].to_string()))
            .collect();
        for p in ["", "a", "ac", "a0", "c", "o", "y", "zzz", "aa/999"] {
            prefixes.insert(p.to_string());
        }

        for prefix in prefixes {
            let want: Vec<String> = all
                .iter()
                .filter(|n| n.starts_with(&prefix))
                .cloned()
                .collect();
            let got: Vec<String> = layer
                .subjects_with_prefix(&prefix)
                .map(|(_, name)| name)
                .collect();
            assert_eq!(want, got, "subjects_with_prefix({prefix:?})");
            assert_eq!(
                want.len(),
                layer.count_subjects_with_prefix(&prefix),
                "count_subjects_with_prefix({prefix:?})"
            );
        }
    }

    #[tokio::test]
    async fn object_is_node_in_base_layer() {
        let dir = tempdir().unwrap();
        let store = open_directory_store(dir.path());
        let builder = store.create_base_layer().await.unwrap();
        builder
            .add_value_triple(ValueTriple::new_node("foo", "links_to", "bar"))
            .unwrap();
        builder
            .add_value_triple(ValueTriple::new_string_value("foo", "links_to_data", "wow"))
            .unwrap();
        let layer = builder.commit().await.unwrap();
        assert_eq!(Some(true), layer.id_object_is_node(1));
        assert_eq!(Some(true), layer.id_object_is_node(2));
        assert_eq!(Some(false), layer.id_object_is_node(3));
        assert_eq!(None, layer.id_object_is_node(4));
    }
}

#[cfg(test)]
mod placeholder_probe {
    use crate::layer::internal::base::base_tests::*;

    // Invariant probe: no current builder writes a predicate-0 entry into
    // s_p (add_triple drops 0 on entry), while o_ps DOES contain [0]
    // segments for ids that exist in the object-id space but never appear
    // as objects. The zero-skip in internal_layer_triple_count relies on
    // that distinction. If a future writer starts emitting predicate-0
    // placeholders, this test fails and the subtraction paths must be
    // re-verified.
    #[tokio::test]
    async fn built_layers_have_no_predicate_zero_but_object_padding() {
        let layer = example_base_layer().await;
        assert!(layer.pos_predicate_wavelet_tree().lookup(0).is_none());
        let o_ps = layer.pos_o_ps_adjacency_list();
        let o_zeros = (0..o_ps.right_count() as u64)
            .filter(|&i| o_ps.num_at_pos(i) == 0)
            .count();
        assert!(o_zeros > 0);
    }
}
