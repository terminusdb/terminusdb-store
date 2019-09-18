use super::layer::*;
use super::base::*;
use super::child::*;
use crate::structure::storage::*;
use std::collections::BTreeSet;

#[derive(Clone)]
struct SimpleLayerBuilder<F:'static+FileLoad+FileStore> {
    parent: Option<ParentLayer<F::Map>>,
    files: LayerFiles<F>,
    resolved_additions: BTreeSet<IdTriple>,
    unresolved_additions: BTreeSet<StringTriple>,
    removals: BTreeSet<IdTriple>, // always resolved!
}

impl<F:'static+FileLoad+FileStore> SimpleLayerBuilder<F> {
    pub fn new(files: BaseLayerFiles<F>) -> Self {
        Self {
            parent: None,
            files: LayerFiles::Base(files),
            resolved_additions: BTreeSet::new(),
            unresolved_additions: BTreeSet::new(),
            removals: BTreeSet::new()
        }
    }

    pub fn from_parent(files: ChildLayerFiles<F>, parent: ParentLayer<F::Map>) -> Self {
        Self {
            parent: Some(parent),
            files: LayerFiles::Child(files),
            resolved_additions: BTreeSet::new(),
            unresolved_additions: BTreeSet::new(),
            removals: BTreeSet::new()
        }
    }

    pub fn add_string_triple(&mut self, triple: &StringTriple) {
        match self.parent.as_ref()
            .and_then(|parent| parent.string_triple_to_id(triple)) {
                None => self.unresolved_additions.insert(triple.clone()),
                Some(resolved) => self.resolved_additions.insert(resolved)
            };
    }

    pub fn add_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        if self.parent.as_mut()
            .map(|parent|
                 !parent.id_triple_exists(triple)
                 && parent.id_subject(triple.subject).is_some()
                 && parent.id_predicate(triple.predicate).is_some()
                 && parent.id_object(triple.object).is_some())
            .unwrap_or(false) {
                self.resolved_additions.insert(triple);
                Some(())
            }
        else {
            None
        }
    }

    pub fn remove_id_triple(&mut self, triple: IdTriple) -> Option<()> {
        if self.parent.is_none() {
            return None;
        }

        let parent = self.parent.as_ref().unwrap();

        if parent.id_triple_exists(triple) {
            self.removals.insert(triple);

            Some(())
        }
        else {
            None
        }
    }

    pub fn remove_string_triple(&mut self, triple: StringTriple) -> Option<()> {
        self.parent.as_ref().and_then(|p|p.string_triple_to_id(&triple))
            .and_then(|t| self.remove_id_triple(t))
    }

    fn unresolved_strings(&self) -> (Vec<String>, Vec<String>, Vec<String>) {
        let mut node_builder:BTreeSet<String> = BTreeSet::new();
        let mut predicate_builder:BTreeSet<String> = BTreeSet::new();
        let mut value_builder:BTreeSet<String> = BTreeSet::new();
        for StringTriple {subject, predicate, object} in self.unresolved_additions.iter() {
            // todo - should only copy the string if we actually need to insert it
            node_builder.insert(subject.to_owned());
            predicate_builder.insert(predicate.to_owned());
            match object {
                ObjectType::Node(node) => node_builder.insert(node.to_owned()),
                ObjectType::Value(value) => value_builder.insert(value.to_owned())
            };
        }

        (node_builder.into_iter().collect(),
         predicate_builder.into_iter().collect(),
         value_builder.into_iter().collect())

    }

    pub fn finalize(self) {
        match self.parent {
            Some(parent) => {
                let files = self.files.into_child();
                let builder = ChildLayerFileBuilder::new(
                    parent,
                    files.node_dictionary_blocks_file,
                    files.node_dictionary_offsets_file,

                    files.predicate_dictionary_blocks_file,
                    files.predicate_dictionary_offsets_file,

                    files.value_dictionary_blocks_file,
                    files.value_dictionary_offsets_file,

                    files.pos_subjects_file,
                    files.neg_subjects_file,

                    files.pos_s_p_adjacency_list_bits_file,
                    files.pos_s_p_adjacency_list_blocks_file,
                    files.pos_s_p_adjacency_list_sblocks_file,
                    files.pos_s_p_adjacency_list_nums_file,

                    files.pos_sp_o_adjacency_list_bits_file,
                    files.pos_sp_o_adjacency_list_blocks_file,
                    files.pos_sp_o_adjacency_list_sblocks_file,
                    files.pos_sp_o_adjacency_list_nums_file,

                    files.neg_s_p_adjacency_list_bits_file,
                    files.neg_s_p_adjacency_list_blocks_file,
                    files.neg_s_p_adjacency_list_sblocks_file,
                    files.neg_s_p_adjacency_list_nums_file,

                    files.neg_sp_o_adjacency_list_bits_file,
                    files.neg_sp_o_adjacency_list_blocks_file,
                    files.neg_sp_o_adjacency_list_sblocks_file,
                    files.neg_sp_o_adjacency_list_nums_file,
                );
            },
            None => {
                let files = self.files.into_base();
                let builder = BaseLayerFileBuilder::new(
                    files.node_dictionary_blocks_file,
                    files.node_dictionary_offsets_file,

                    files.predicate_dictionary_blocks_file,
                    files.predicate_dictionary_offsets_file,

                    files.value_dictionary_blocks_file,
                    files.value_dictionary_offsets_file,

                    files.s_p_adjacency_list_bits_file,
                    files.s_p_adjacency_list_blocks_file,
                    files.s_p_adjacency_list_sblocks_file,
                    files.s_p_adjacency_list_nums_file,

                    files.sp_o_adjacency_list_bits_file,
                    files.sp_o_adjacency_list_blocks_file,
                    files.sp_o_adjacency_list_sblocks_file,
                    files.sp_o_adjacency_list_nums_file);
            }
        }
    }
}

#[derive(Clone)]
pub enum LayerFiles<F:FileLoad+FileStore> {
    Base(BaseLayerFiles<F>),
    Child(ChildLayerFiles<F>)
}

impl<F:FileLoad+FileStore> LayerFiles<F> {
    fn into_base(self) -> BaseLayerFiles<F> {
        match self {
            Self::Base(b) => b,
            _ => panic!("layer files are not for base")
        }
    }

    fn into_child(self) -> ChildLayerFiles<F> {
        match self {
            Self::Child(c) => c,
            _ => panic!("layer files are not for child")
        }
    }
}
