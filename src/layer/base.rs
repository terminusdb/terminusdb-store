use futures::prelude::*;
use futures::future;

use crate::structure::*;

pub struct BaseLayer<M:AsRef<[u8]>+Clone> {
    node_dictionary: PfcDict<M>,
    predicate_dictionary: PfcDict<M>,
    value_dictionary: PfcDict<M>,
    s_v_adjacency_list: AdjacencyList<M>,
    sv_o_adjacency_list: AdjacencyList<M>
}

pub struct BaseLayerFileBuilder<F:'static+FileLoad+FileStore> {
    node_dictionary_blocks_file: F,
    node_dictionary_offsets_file: F,

    predicate_dictionary_blocks_file: F,
    predicate_dictionary_offsets_file: F,

    value_dictionary_blocks_file: F,
    value_dictionary_offsets_file: F,

    s_v_adjacency_list_bits_file: F,
    s_v_adjacency_list_blocks_file: F,
    s_v_adjacency_list_sblocks_file: F,
    s_v_adjacency_list_nums_file: F,

    sv_o_adjacency_list_bits_file: F,
    sv_o_adjacency_list_blocks_file: F,
    sv_o_adjacency_list_sblocks_file: F,
    sv_o_adjacency_list_nums_file: F,

    node_dictionary_builder: PfcDictFileBuilder<F::Write>,
    predicate_dictionary_builder: PfcDictFileBuilder<F::Write>,
    value_dictionary_builder: PfcDictFileBuilder<F::Write>,
}

impl<F:'static+FileLoad+FileStore> BaseLayerFileBuilder<F> {
    pub fn new(node_dictionary_blocks_file: F,
               node_dictionary_offsets_file: F,

               predicate_dictionary_blocks_file: F,
               predicate_dictionary_offsets_file: F,

               value_dictionary_blocks_file: F,
               value_dictionary_offsets_file: F,

               s_v_adjacency_list_bits_file: F,
               s_v_adjacency_list_blocks_file: F,
               s_v_adjacency_list_sblocks_file: F,
               s_v_adjacency_list_nums_file: F,

               sv_o_adjacency_list_bits_file: F,
               sv_o_adjacency_list_blocks_file: F,
               sv_o_adjacency_list_sblocks_file: F,
               sv_o_adjacency_list_nums_file: F
    ) -> Self {
        let node_dictionary_builder = PfcDictFileBuilder::new(node_dictionary_blocks_file.open_write(), node_dictionary_offsets_file.open_write());
        let predicate_dictionary_builder = PfcDictFileBuilder::new(predicate_dictionary_blocks_file.open_write(), predicate_dictionary_offsets_file.open_write());
        let value_dictionary_builder = PfcDictFileBuilder::new(value_dictionary_blocks_file.open_write(), value_dictionary_offsets_file.open_write());

        BaseLayerFileBuilder {
            node_dictionary_blocks_file,
            node_dictionary_offsets_file,

            predicate_dictionary_blocks_file,
            predicate_dictionary_offsets_file,

            value_dictionary_blocks_file,
            value_dictionary_offsets_file,


            s_v_adjacency_list_bits_file,
            s_v_adjacency_list_blocks_file,
            s_v_adjacency_list_sblocks_file,
            s_v_adjacency_list_nums_file,

            sv_o_adjacency_list_bits_file,
            sv_o_adjacency_list_blocks_file,
            sv_o_adjacency_list_sblocks_file,
            sv_o_adjacency_list_nums_file,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        }
    }
    
    pub fn into_phase2(self) -> impl Future<Item=BaseLayerFileBuilderPhase2<F>,Error=std::io::Error> {
        let BaseLayerFileBuilder {
            node_dictionary_blocks_file,
            node_dictionary_offsets_file,

            predicate_dictionary_blocks_file,
            predicate_dictionary_offsets_file,

            value_dictionary_blocks_file,
            value_dictionary_offsets_file,


            s_v_adjacency_list_bits_file,
            s_v_adjacency_list_blocks_file,
            s_v_adjacency_list_sblocks_file,
            s_v_adjacency_list_nums_file,

            sv_o_adjacency_list_bits_file,
            sv_o_adjacency_list_blocks_file,
            sv_o_adjacency_list_sblocks_file,
            sv_o_adjacency_list_nums_file,

            node_dictionary_builder,
            predicate_dictionary_builder,
            value_dictionary_builder
        } = self;

        let finalize_nodedict = node_dictionary_builder.finalize();
        let finalize_preddict = predicate_dictionary_builder.finalize();
        let finalize_valdict = value_dictionary_builder.finalize();

        future::join_all(vec![finalize_nodedict, finalize_preddict, finalize_valdict])
            .and_then(move |_| {
                let pred_dict_r = PfcDict::parse(predicate_dictionary_blocks_file.map(),
                                                 predicate_dictionary_offsets_file.map());
                if pred_dict_r.is_err() {
                    return future::err(pred_dict_r.err().unwrap().into());
                }
                let pred_dict = pred_dict_r.unwrap();

                let val_dict_r = PfcDict::parse(value_dictionary_blocks_file.map(),
                                                value_dictionary_offsets_file.map());
                if val_dict_r.is_err() {
                    return future::err(val_dict_r.err().unwrap().into());
                }
                let val_dict = val_dict_r.unwrap();


                let num_predicates = pred_dict.len();
                let num_values = val_dict.len();

                future::ok(BaseLayerFileBuilderPhase2::new(s_v_adjacency_list_bits_file,
                                                           s_v_adjacency_list_blocks_file,
                                                           s_v_adjacency_list_sblocks_file,
                                                           s_v_adjacency_list_nums_file,

                                                           sv_o_adjacency_list_bits_file,
                                                           sv_o_adjacency_list_blocks_file,
                                                           sv_o_adjacency_list_sblocks_file,
                                                           sv_o_adjacency_list_nums_file,

                                                           num_predicates,
                                                           num_values))
            })
    }
}


pub struct BaseLayerFileBuilderPhase2<F:'static+FileLoad+FileStore> {
    s_v_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>,
    sv_o_adjacency_list_builder: AdjacencyListBuilder<F, F::Write, F::Write, F::Write>
}

impl<F:'static+FileLoad+FileStore> BaseLayerFileBuilderPhase2<F> {
    fn new(s_v_adjacency_list_bits_file: F,
           s_v_adjacency_list_blocks_file: F,
           s_v_adjacency_list_sblocks_file: F,
           s_v_adjacency_list_nums_file: F,
           
           sv_o_adjacency_list_bits_file: F,
           sv_o_adjacency_list_blocks_file: F,
           sv_o_adjacency_list_sblocks_file: F,
           sv_o_adjacency_list_nums_file: F,

           num_predicates: usize,
           num_objects: usize
    ) -> Self {
        let s_v_width = (num_predicates as f32).log2().ceil() as u8;
        let sv_o_width = (num_objects as f32).log2().ceil() as u8;
        let s_v_adjacency_list_builder = AdjacencyListBuilder::new(s_v_adjacency_list_bits_file,
                                                                   s_v_adjacency_list_blocks_file.open_write(),
                                                                   s_v_adjacency_list_sblocks_file.open_write(),
                                                                   s_v_adjacency_list_nums_file.open_write(),
                                                                   s_v_width);

        let sv_o_adjacency_list_builder = AdjacencyListBuilder::new(sv_o_adjacency_list_bits_file,
                                                                    sv_o_adjacency_list_blocks_file.open_write(),
                                                                    sv_o_adjacency_list_sblocks_file.open_write(),
                                                                    sv_o_adjacency_list_nums_file.open_write(),
                                                                    sv_o_width);

        BaseLayerFileBuilderPhase2 {
            s_v_adjacency_list_builder,
            sv_o_adjacency_list_builder
        }
    }
}
