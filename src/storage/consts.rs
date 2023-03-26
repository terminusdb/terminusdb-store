use std::collections::HashMap;

use num_derive::FromPrimitive;

pub struct Filenames {
    pub node_dictionary_blocks: &'static str,
    pub node_dictionary_offsets: &'static str,

    pub predicate_dictionary_blocks: &'static str,
    pub predicate_dictionary_offsets: &'static str,

    pub value_dictionary_types_present: &'static str,
    pub value_dictionary_type_offsets: &'static str,
    pub value_dictionary_blocks: &'static str,
    pub value_dictionary_offsets: &'static str,

    pub node_value_idmap_bits: &'static str,
    pub node_value_idmap_bit_index_blocks: &'static str,
    pub node_value_idmap_bit_index_sblocks: &'static str,

    pub predicate_idmap_bits: &'static str,
    pub predicate_idmap_bit_index_blocks: &'static str,
    pub predicate_idmap_bit_index_sblocks: &'static str,

    pub base_subjects: &'static str,
    pub base_objects: &'static str,

    pub base_s_p_adjacency_list_nums: &'static str,
    pub base_s_p_adjacency_list_bits: &'static str,
    pub base_s_p_adjacency_list_bit_index_blocks: &'static str,
    pub base_s_p_adjacency_list_bit_index_sblocks: &'static str,

    pub base_sp_o_adjacency_list_nums: &'static str,
    pub base_sp_o_adjacency_list_bits: &'static str,
    pub base_sp_o_adjacency_list_bit_index_blocks: &'static str,
    pub base_sp_o_adjacency_list_bit_index_sblocks: &'static str,

    pub base_o_ps_adjacency_list_nums: &'static str,
    pub base_o_ps_adjacency_list_bits: &'static str,
    pub base_o_ps_adjacency_list_bit_index_blocks: &'static str,
    pub base_o_ps_adjacency_list_bit_index_sblocks: &'static str,

    pub pos_subjects: &'static str,
    pub pos_objects: &'static str,

    pub pos_s_p_adjacency_list_nums: &'static str,
    pub pos_s_p_adjacency_list_bits: &'static str,
    pub pos_s_p_adjacency_list_bit_index_blocks: &'static str,
    pub pos_s_p_adjacency_list_bit_index_sblocks: &'static str,

    pub pos_sp_o_adjacency_list_nums: &'static str,
    pub pos_sp_o_adjacency_list_bits: &'static str,
    pub pos_sp_o_adjacency_list_bit_index_blocks: &'static str,
    pub pos_sp_o_adjacency_list_bit_index_sblocks: &'static str,

    pub pos_o_ps_adjacency_list_nums: &'static str,
    pub pos_o_ps_adjacency_list_bits: &'static str,
    pub pos_o_ps_adjacency_list_bit_index_blocks: &'static str,
    pub pos_o_ps_adjacency_list_bit_index_sblocks: &'static str,

    pub neg_subjects: &'static str,
    pub neg_objects: &'static str,

    pub neg_s_p_adjacency_list_nums: &'static str,
    pub neg_s_p_adjacency_list_bits: &'static str,
    pub neg_s_p_adjacency_list_bit_index_blocks: &'static str,
    pub neg_s_p_adjacency_list_bit_index_sblocks: &'static str,

    pub neg_sp_o_adjacency_list_nums: &'static str,
    pub neg_sp_o_adjacency_list_bits: &'static str,
    pub neg_sp_o_adjacency_list_bit_index_blocks: &'static str,
    pub neg_sp_o_adjacency_list_bit_index_sblocks: &'static str,

    pub neg_o_ps_adjacency_list_nums: &'static str,
    pub neg_o_ps_adjacency_list_bits: &'static str,
    pub neg_o_ps_adjacency_list_bit_index_blocks: &'static str,
    pub neg_o_ps_adjacency_list_bit_index_sblocks: &'static str,

    pub base_predicate_wavelet_tree_bits: &'static str,
    pub base_predicate_wavelet_tree_bit_index_blocks: &'static str,
    pub base_predicate_wavelet_tree_bit_index_sblocks: &'static str,

    pub pos_predicate_wavelet_tree_bits: &'static str,
    pub pos_predicate_wavelet_tree_bit_index_blocks: &'static str,
    pub pos_predicate_wavelet_tree_bit_index_sblocks: &'static str,

    pub neg_predicate_wavelet_tree_bits: &'static str,
    pub neg_predicate_wavelet_tree_bit_index_blocks: &'static str,
    pub neg_predicate_wavelet_tree_bit_index_sblocks: &'static str,

    pub parent: &'static str,
    pub rollup: &'static str,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, FromPrimitive)]
pub enum LayerFileEnum {
    NodeDictionaryBlocks = 0,
    NodeDictionaryOffsets,
    PredicateDictionaryBlocks,
    PredicateDictionaryOffsets,
    ValueDictionaryTypesPresent,
    ValueDictionaryTypeOffsets,
    ValueDictionaryBlocks,
    ValueDictionaryOffsets,

    NodeValueIdMapBits,
    NodeValueIdMapBitIndexBlocks,
    NodeValueIdMapBitIndexSBlocks,

    PredicateIdMapBits,
    PredicateIdMapBitIndexBlocks,
    PredicateIdMapBitIndexSBlocks,

    PosSubjects,
    PosObjects,
    NegSubjects,
    NegObjects,

    PosSPAdjacencyListNums,
    PosSPAdjacencyListBits,
    PosSPAdjacencyListBitIndexBlocks,
    PosSPAdjacencyListBitIndexSBlocks,

    PosSpOAdjacencyListNums,
    PosSpOAdjacencyListBits,
    PosSpOAdjacencyListBitIndexBlocks,
    PosSpOAdjacencyListBitIndexSBlocks,

    PosOPsAdjacencyListNums,
    PosOPsAdjacencyListBits,
    PosOPsAdjacencyListBitIndexBlocks,
    PosOPsAdjacencyListBitIndexSBlocks,

    PosPredicateWaveletTreeBits,
    PosPredicateWaveletTreeBitIndexBlocks,
    PosPredicateWaveletTreeBitIndexSBlocks,

    NegSPAdjacencyListNums,
    NegSPAdjacencyListBits,
    NegSPAdjacencyListBitIndexBlocks,
    NegSPAdjacencyListBitIndexSBlocks,

    NegSpOAdjacencyListNums,
    NegSpOAdjacencyListBits,
    NegSpOAdjacencyListBitIndexBlocks,
    NegSpOAdjacencyListBitIndexSBlocks,

    NegOPsAdjacencyListNums,
    NegOPsAdjacencyListBits,
    NegOPsAdjacencyListBitIndexBlocks,
    NegOPsAdjacencyListBitIndexSBlocks,

    NegPredicateWaveletTreeBits,
    NegPredicateWaveletTreeBitIndexBlocks,
    NegPredicateWaveletTreeBitIndexSBlocks,

    Parent,
    Rollup,
}

pub const FILENAMES: Filenames = Filenames {
    node_dictionary_blocks: "node_dictionary_blocks.tfc",
    node_dictionary_offsets: "node_dictionary_offsets.logarray",

    predicate_dictionary_blocks: "predicate_dictionary_blocks.tfc",
    predicate_dictionary_offsets: "predicate_dictionary_offsets.logarray",

    value_dictionary_types_present: "value_dictionary_types.logarray",
    value_dictionary_type_offsets: "value_dictionary_type_offsets.logarray",
    value_dictionary_blocks: "value_dictionary_blocks.tfc",
    value_dictionary_offsets: "value_dictionary_offsets.logarray",

    node_value_idmap_bits: "node_value_idmap_bits.bitarray",
    node_value_idmap_bit_index_blocks: "node_value_idmap_bit_index_blocks.bitarray",
    node_value_idmap_bit_index_sblocks: "node_value_idmap_bit_index_sblocks.bitarray",

    predicate_idmap_bits: "predicate_idmap_bits.bitarray",
    predicate_idmap_bit_index_blocks: "predicate_idmap_bit_index_blocks.bitarray",
    predicate_idmap_bit_index_sblocks: "predicate_idmap_bit_index_sblocks.bitarray",

    base_subjects: "base_subjects.logarray",
    base_objects: "base_objects.logarray",

    base_s_p_adjacency_list_nums: "base_s_p_adjacency_list_nums.logarray",
    base_s_p_adjacency_list_bits: "base_s_p_adjacency_list_bits.bitarray",
    base_s_p_adjacency_list_bit_index_blocks: "base_s_p_adjacency_list_bit_index_blocks.logarray",
    base_s_p_adjacency_list_bit_index_sblocks: "base_s_p_adjacency_list_bit_index_sblocks.logarray",

    base_sp_o_adjacency_list_nums: "base_sp_o_adjacency_list_nums.logarray",
    base_sp_o_adjacency_list_bits: "base_sp_o_adjacency_list_bits.bitarray",
    base_sp_o_adjacency_list_bit_index_blocks: "base_sp_o_adjacency_list_bit_index_blocks.logarray",
    base_sp_o_adjacency_list_bit_index_sblocks:
        "base_sp_o_adjacency_list_bit_index_sblocks.logarray",

    base_o_ps_adjacency_list_nums: "base_o_ps_adjacency_list_nums.logarray",
    base_o_ps_adjacency_list_bits: "base_o_ps_adjacency_list_bits.bitarray",
    base_o_ps_adjacency_list_bit_index_blocks: "base_o_ps_adjacency_list_bit_index_blocks.logarray",
    base_o_ps_adjacency_list_bit_index_sblocks:
        "base_o_ps_adjacency_list_bit_index_sblocks.logarray",

    pos_subjects: "child_pos_subjects.logarray",
    pos_objects: "child_pos_objects.logarray",

    pos_s_p_adjacency_list_nums: "pos_s_p_adjacency_list_nums.logarray",
    pos_s_p_adjacency_list_bits: "pos_s_p_adjacency_list_bits.bitarray",
    pos_s_p_adjacency_list_bit_index_blocks: "pos_s_p_adjacency_list_bit_index_blocks.logarray",
    pos_s_p_adjacency_list_bit_index_sblocks: "pos_s_p_adjacency_list_bit_index_sblocks.logarray",

    pos_sp_o_adjacency_list_nums: "pos_sp_o_adjacency_list_nums.logarray",
    pos_sp_o_adjacency_list_bits: "pos_sp_o_adjacency_list_bits.bitarray",
    pos_sp_o_adjacency_list_bit_index_blocks: "pos_sp_o_adjacency_list_bit_index_blocks.logarray",
    pos_sp_o_adjacency_list_bit_index_sblocks: "pos_sp_o_adjacency_list_bit_index_sblocks.logarray",

    pos_o_ps_adjacency_list_nums: "pos_o_ps_adjacency_list_nums.logarray",
    pos_o_ps_adjacency_list_bits: "pos_o_ps_adjacency_list_bits.bitarray",
    pos_o_ps_adjacency_list_bit_index_blocks: "pos_o_ps_adjacency_list_bit_index_blocks.logarray",
    pos_o_ps_adjacency_list_bit_index_sblocks: "pos_o_ps_adjacency_list_bit_index_sblocks.logarray",

    neg_subjects: "child_neg_subjects.logarray",
    neg_objects: "child_neg_objects.logarray",

    neg_s_p_adjacency_list_nums: "neg_s_p_adjacency_list_nums.logarray",
    neg_s_p_adjacency_list_bits: "neg_s_p_adjacency_list_bits.bitarray",
    neg_s_p_adjacency_list_bit_index_blocks: "neg_s_p_adjacency_list_bit_index_blocks.logarray",
    neg_s_p_adjacency_list_bit_index_sblocks: "neg_s_p_adjacency_list_bit_index_sblocks.logarray",

    neg_sp_o_adjacency_list_nums: "neg_sp_o_adjacency_list_nums.logarray",
    neg_sp_o_adjacency_list_bits: "neg_sp_o_adjacency_list_bits.bitarray",
    neg_sp_o_adjacency_list_bit_index_blocks: "neg_sp_o_adjacency_list_bit_index_blocks.logarray",
    neg_sp_o_adjacency_list_bit_index_sblocks: "neg_sp_o_adjacency_list_bit_index_sblocks.logarray",

    neg_o_ps_adjacency_list_nums: "neg_o_ps_adjacency_list_nums.logarray",
    neg_o_ps_adjacency_list_bits: "neg_o_ps_adjacency_list_bits.bitarray",
    neg_o_ps_adjacency_list_bit_index_blocks: "neg_o_ps_adjacency_list_bit_index_blocks.logarray",
    neg_o_ps_adjacency_list_bit_index_sblocks: "neg_o_ps_adjacency_list_bit_index_sblocks.logarray",

    base_predicate_wavelet_tree_bits: "base_predicate_wavelet_tree_bits.bitarray",
    base_predicate_wavelet_tree_bit_index_blocks:
        "base_predicate_wavelet_tree_bit_index_blocks.logarray",
    base_predicate_wavelet_tree_bit_index_sblocks:
        "base_predicate_wavelet_tree_bit_index_sblocks.logarray",

    pos_predicate_wavelet_tree_bits: "pos_predicate_wavelet_tree_bits.bitarray",
    pos_predicate_wavelet_tree_bit_index_blocks:
        "pos_predicate_wavelet_tree_bit_index_blocks.logarray",
    pos_predicate_wavelet_tree_bit_index_sblocks:
        "pos_predicate_wavelet_tree_bit_index_sblocks.logarray",

    neg_predicate_wavelet_tree_bits: "neg_predicate_wavelet_tree_bits.bitarray",
    neg_predicate_wavelet_tree_bit_index_blocks:
        "neg_predicate_wavelet_tree_bit_index_blocks.logarray",
    neg_predicate_wavelet_tree_bit_index_sblocks:
        "neg_predicate_wavelet_tree_bit_index_sblocks.logarray",

    parent: "parent.hex",
    rollup: "rollup.hex",
};

lazy_static! {
    pub static ref FILENAME_ENUM_MAP: HashMap<&'static str, LayerFileEnum> = HashMap::from([
        (
            "node_dictionary_blocks.tfc",
            LayerFileEnum::NodeDictionaryBlocks
        ),
        (
            "node_dictionary_offsets.logarray",
            LayerFileEnum::NodeDictionaryOffsets
        ),
        (
            "predicate_dictionary_blocks.tfc",
            LayerFileEnum::PredicateDictionaryBlocks
        ),
        (
            "predicate_dictionary_offsets.logarray",
            LayerFileEnum::PredicateDictionaryOffsets
        ),
        (
            "value_dictionary_types.logarray",
            LayerFileEnum::ValueDictionaryTypesPresent
        ),
        (
            "value_dictionary_type_offsets.logarray",
            LayerFileEnum::ValueDictionaryTypeOffsets
        ),
        (
            "value_dictionary_blocks.tfc",
            LayerFileEnum::ValueDictionaryBlocks
        ),
        (
            "value_dictionary_offsets.logarray",
            LayerFileEnum::ValueDictionaryOffsets
        ),
        (
            "node_value_idmap_bits.bitarray",
            LayerFileEnum::NodeValueIdMapBits
        ),
        (
            "node_value_idmap_bit_index_blocks.bitarray",
            LayerFileEnum::NodeValueIdMapBitIndexBlocks
        ),
        (
            "node_value_idmap_bit_index_sblocks.bitarray",
            LayerFileEnum::NodeValueIdMapBitIndexSBlocks
        ),
        (
            "predicate_idmap_bits.bitarray",
            LayerFileEnum::PredicateIdMapBits
        ),
        (
            "predicate_idmap_bit_index_blocks.bitarray",
            LayerFileEnum::PredicateIdMapBitIndexBlocks
        ),
        (
            "predicate_idmap_bit_index_sblocks.bitarray",
            LayerFileEnum::PredicateIdMapBitIndexSBlocks
        ),
        ("base_subjects.logarray", LayerFileEnum::PosSubjects),
        ("base_objects.logarray", LayerFileEnum::PosObjects),
        ("child_pos_subjects.logarray", LayerFileEnum::PosSubjects),
        ("child_pos_objects.logarray", LayerFileEnum::PosObjects),
        ("child_neg_subjects.logarray", LayerFileEnum::NegSubjects),
        ("child_neg_objects.logarray", LayerFileEnum::NegObjects),
        (
            "base_s_p_adjacency_list_nums.logarray",
            LayerFileEnum::PosSPAdjacencyListNums
        ),
        (
            "base_s_p_adjacency_list_bits.bitarray",
            LayerFileEnum::PosSPAdjacencyListBits
        ),
        (
            "base_s_p_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::PosSPAdjacencyListBitIndexBlocks
        ),
        (
            "base_s_p_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::PosSPAdjacencyListBitIndexSBlocks
        ),
        (
            "base_sp_o_adjacency_list_nums.logarray",
            LayerFileEnum::PosSpOAdjacencyListNums
        ),
        (
            "base_sp_o_adjacency_list_bits.bitarray",
            LayerFileEnum::PosSpOAdjacencyListBits
        ),
        (
            "base_sp_o_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::PosSpOAdjacencyListBitIndexBlocks
        ),
        (
            "base_sp_o_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::PosSpOAdjacencyListBitIndexSBlocks
        ),
        (
            "base_o_ps_adjacency_list_nums.logarray",
            LayerFileEnum::PosOPsAdjacencyListNums
        ),
        (
            "base_o_ps_adjacency_list_bits.bitarray",
            LayerFileEnum::PosOPsAdjacencyListBits
        ),
        (
            "base_o_ps_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::PosOPsAdjacencyListBitIndexBlocks
        ),
        (
            "base_o_ps_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::PosOPsAdjacencyListBitIndexSBlocks
        ),
        (
            "base_predicate_wavelet_tree_bits.bitarray",
            LayerFileEnum::PosPredicateWaveletTreeBits
        ),
        (
            "base_predicate_wavelet_tree_bit_index_blocks.logarray",
            LayerFileEnum::PosPredicateWaveletTreeBitIndexBlocks
        ),
        (
            "base_predicate_wavelet_tree_bit_index_sblocks.logarray",
            LayerFileEnum::PosPredicateWaveletTreeBitIndexSBlocks
        ),
        (
            "pos_s_p_adjacency_list_nums.logarray",
            LayerFileEnum::PosSPAdjacencyListNums
        ),
        (
            "pos_s_p_adjacency_list_bits.bitarray",
            LayerFileEnum::PosSPAdjacencyListBits
        ),
        (
            "pos_s_p_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::PosSPAdjacencyListBitIndexBlocks
        ),
        (
            "pos_s_p_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::PosSPAdjacencyListBitIndexSBlocks
        ),
        (
            "pos_sp_o_adjacency_list_nums.logarray",
            LayerFileEnum::PosSpOAdjacencyListNums
        ),
        (
            "pos_sp_o_adjacency_list_bits.bitarray",
            LayerFileEnum::PosSpOAdjacencyListBits
        ),
        (
            "pos_sp_o_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::PosSpOAdjacencyListBitIndexBlocks
        ),
        (
            "pos_sp_o_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::PosSpOAdjacencyListBitIndexSBlocks
        ),
        (
            "pos_o_ps_adjacency_list_nums.logarray",
            LayerFileEnum::PosOPsAdjacencyListNums
        ),
        (
            "pos_o_ps_adjacency_list_bits.bitarray",
            LayerFileEnum::PosOPsAdjacencyListBits
        ),
        (
            "pos_o_ps_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::PosOPsAdjacencyListBitIndexBlocks
        ),
        (
            "pos_o_ps_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::PosOPsAdjacencyListBitIndexSBlocks
        ),
        (
            "pos_predicate_wavelet_tree_bits.bitarray",
            LayerFileEnum::PosPredicateWaveletTreeBits
        ),
        (
            "pos_predicate_wavelet_tree_bit_index_blocks.logarray",
            LayerFileEnum::PosPredicateWaveletTreeBitIndexBlocks
        ),
        (
            "pos_predicate_wavelet_tree_bit_index_sblocks.logarray",
            LayerFileEnum::PosPredicateWaveletTreeBitIndexSBlocks
        ),
        (
            "neg_s_p_adjacency_list_nums.logarray",
            LayerFileEnum::NegSPAdjacencyListNums
        ),
        (
            "neg_s_p_adjacency_list_bits.bitarray",
            LayerFileEnum::NegSPAdjacencyListBits
        ),
        (
            "neg_s_p_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::NegSPAdjacencyListBitIndexBlocks
        ),
        (
            "neg_s_p_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::NegSPAdjacencyListBitIndexSBlocks
        ),
        (
            "neg_sp_o_adjacency_list_nums.logarray",
            LayerFileEnum::NegSpOAdjacencyListNums
        ),
        (
            "neg_sp_o_adjacency_list_bits.bitarray",
            LayerFileEnum::NegSpOAdjacencyListBits
        ),
        (
            "neg_sp_o_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::NegSpOAdjacencyListBitIndexBlocks
        ),
        (
            "neg_sp_o_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::NegSpOAdjacencyListBitIndexSBlocks
        ),
        (
            "neg_o_ps_adjacency_list_nums.logarray",
            LayerFileEnum::NegOPsAdjacencyListNums
        ),
        (
            "neg_o_ps_adjacency_list_bits.bitarray",
            LayerFileEnum::NegOPsAdjacencyListBits
        ),
        (
            "neg_o_ps_adjacency_list_bit_index_blocks.logarray",
            LayerFileEnum::NegOPsAdjacencyListBitIndexBlocks
        ),
        (
            "neg_o_ps_adjacency_list_bit_index_sblocks.logarray",
            LayerFileEnum::NegOPsAdjacencyListBitIndexSBlocks
        ),
        (
            "neg_predicate_wavelet_tree_bits.bitarray",
            LayerFileEnum::NegPredicateWaveletTreeBits
        ),
        (
            "neg_predicate_wavelet_tree_bit_index_blocks.logarray",
            LayerFileEnum::NegPredicateWaveletTreeBitIndexBlocks
        ),
        (
            "neg_predicate_wavelet_tree_bit_index_sblocks.logarray",
            LayerFileEnum::NegPredicateWaveletTreeBitIndexSBlocks
        ),
        ("parent.hex", LayerFileEnum::Parent),
        ("rollup.hex", LayerFileEnum::Rollup),
    ]);
}

pub const SHARED_REQUIRED_FILES: [&str; 8] = [
    FILENAMES.node_dictionary_blocks,
    FILENAMES.node_dictionary_offsets,
    FILENAMES.predicate_dictionary_blocks,
    FILENAMES.predicate_dictionary_offsets,
    FILENAMES.value_dictionary_types_present,
    FILENAMES.value_dictionary_type_offsets,
    FILENAMES.value_dictionary_blocks,
    FILENAMES.value_dictionary_offsets,
];

pub const SHARED_OPTIONAL_FILES: [&str; 7] = [
    FILENAMES.node_value_idmap_bits,
    FILENAMES.node_value_idmap_bit_index_blocks,
    FILENAMES.node_value_idmap_bit_index_sblocks,
    FILENAMES.predicate_idmap_bits,
    FILENAMES.predicate_idmap_bit_index_blocks,
    FILENAMES.predicate_idmap_bit_index_sblocks,
    FILENAMES.rollup,
];

pub const BASE_LAYER_REQUIRED_FILES: [&str; 15] = [
    FILENAMES.base_s_p_adjacency_list_nums,
    FILENAMES.base_s_p_adjacency_list_bits,
    FILENAMES.base_s_p_adjacency_list_bit_index_blocks,
    FILENAMES.base_s_p_adjacency_list_bit_index_sblocks,
    FILENAMES.base_sp_o_adjacency_list_nums,
    FILENAMES.base_sp_o_adjacency_list_bits,
    FILENAMES.base_sp_o_adjacency_list_bit_index_blocks,
    FILENAMES.base_sp_o_adjacency_list_bit_index_sblocks,
    FILENAMES.base_o_ps_adjacency_list_nums,
    FILENAMES.base_o_ps_adjacency_list_bits,
    FILENAMES.base_o_ps_adjacency_list_bit_index_blocks,
    FILENAMES.base_o_ps_adjacency_list_bit_index_sblocks,
    FILENAMES.base_predicate_wavelet_tree_bits,
    FILENAMES.base_predicate_wavelet_tree_bit_index_blocks,
    FILENAMES.base_predicate_wavelet_tree_bit_index_sblocks,
];

pub const BASE_LAYER_OPTIONAL_FILES: [&str; 2] = [FILENAMES.base_subjects, FILENAMES.base_objects];

pub const CHILD_LAYER_REQUIRED_FILES: [&str; 31] = [
    FILENAMES.parent,
    FILENAMES.pos_s_p_adjacency_list_nums,
    FILENAMES.pos_s_p_adjacency_list_bits,
    FILENAMES.pos_s_p_adjacency_list_bit_index_blocks,
    FILENAMES.pos_s_p_adjacency_list_bit_index_sblocks,
    FILENAMES.pos_sp_o_adjacency_list_nums,
    FILENAMES.pos_sp_o_adjacency_list_bits,
    FILENAMES.pos_sp_o_adjacency_list_bit_index_blocks,
    FILENAMES.pos_sp_o_adjacency_list_bit_index_sblocks,
    FILENAMES.pos_o_ps_adjacency_list_nums,
    FILENAMES.pos_o_ps_adjacency_list_bits,
    FILENAMES.pos_o_ps_adjacency_list_bit_index_blocks,
    FILENAMES.pos_o_ps_adjacency_list_bit_index_sblocks,
    FILENAMES.pos_predicate_wavelet_tree_bits,
    FILENAMES.pos_predicate_wavelet_tree_bit_index_blocks,
    FILENAMES.pos_predicate_wavelet_tree_bit_index_sblocks,
    FILENAMES.neg_s_p_adjacency_list_nums,
    FILENAMES.neg_s_p_adjacency_list_bits,
    FILENAMES.neg_s_p_adjacency_list_bit_index_blocks,
    FILENAMES.neg_s_p_adjacency_list_bit_index_sblocks,
    FILENAMES.neg_sp_o_adjacency_list_nums,
    FILENAMES.neg_sp_o_adjacency_list_bits,
    FILENAMES.neg_sp_o_adjacency_list_bit_index_blocks,
    FILENAMES.neg_sp_o_adjacency_list_bit_index_sblocks,
    FILENAMES.neg_o_ps_adjacency_list_nums,
    FILENAMES.neg_o_ps_adjacency_list_bits,
    FILENAMES.neg_o_ps_adjacency_list_bit_index_blocks,
    FILENAMES.neg_o_ps_adjacency_list_bit_index_sblocks,
    FILENAMES.neg_predicate_wavelet_tree_bits,
    FILENAMES.neg_predicate_wavelet_tree_bit_index_blocks,
    FILENAMES.neg_predicate_wavelet_tree_bit_index_sblocks,
];

pub const CHILD_LAYER_OPTIONAL_FILES: [&str; 4] = [
    FILENAMES.pos_subjects,
    FILENAMES.pos_objects,
    FILENAMES.neg_subjects,
    FILENAMES.neg_objects,
];
