use std::io;

use super::{BaseLayerFiles, ChildLayerFiles, FileLoad, FileStore, IdMapFiles};

use tdb_succinct::storage::copy_file;

impl<F1: 'static + FileLoad + FileStore> IdMapFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &IdMapFiles<F2>,
    ) -> io::Result<()> {
        self.node_value_idmap_files
            .copy_from(&from.node_value_idmap_files)
            .await?;
        self.predicate_idmap_files
            .copy_from(&from.predicate_idmap_files)
            .await?;

        Ok(())
    }
}

impl<F1: 'static + FileLoad + FileStore> BaseLayerFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &BaseLayerFiles<F2>,
    ) -> io::Result<()> {
        self.node_dictionary_files
            .copy_from(&from.node_dictionary_files)
            .await?;
        self.predicate_dictionary_files
            .copy_from(&from.predicate_dictionary_files)
            .await?;
        self.value_dictionary_files
            .copy_from(&from.value_dictionary_files)
            .await?;
        self.id_map_files.copy_from(&from.id_map_files).await?;
        copy_file(&from.subjects_file, &self.subjects_file).await?;
        copy_file(&from.objects_file, &self.objects_file).await?;
        self.s_p_adjacency_list_files
            .copy_from(&from.s_p_adjacency_list_files)
            .await?;
        self.sp_o_adjacency_list_files
            .copy_from(&from.sp_o_adjacency_list_files)
            .await?;
        self.o_ps_adjacency_list_files
            .copy_from(&from.o_ps_adjacency_list_files)
            .await?;
        self.predicate_wavelet_tree_files
            .copy_from(&from.predicate_wavelet_tree_files)
            .await?;

        Ok(())
    }
}

impl<F1: 'static + FileLoad + FileStore> ChildLayerFiles<F1> {
    pub async fn copy_from<F2: 'static + FileLoad + FileStore>(
        &self,
        from: &ChildLayerFiles<F2>,
    ) -> io::Result<()> {
        self.node_dictionary_files
            .copy_from(&from.node_dictionary_files)
            .await?;
        self.predicate_dictionary_files
            .copy_from(&from.predicate_dictionary_files)
            .await?;
        self.value_dictionary_files
            .copy_from(&from.value_dictionary_files)
            .await?;
        self.id_map_files.copy_from(&from.id_map_files).await?;

        copy_file(&from.pos_subjects_file, &self.pos_subjects_file).await?;
        copy_file(&from.pos_objects_file, &self.pos_objects_file).await?;
        self.pos_s_p_adjacency_list_files
            .copy_from(&from.pos_s_p_adjacency_list_files)
            .await?;
        self.pos_sp_o_adjacency_list_files
            .copy_from(&from.pos_sp_o_adjacency_list_files)
            .await?;
        self.pos_o_ps_adjacency_list_files
            .copy_from(&from.pos_o_ps_adjacency_list_files)
            .await?;
        self.pos_predicate_wavelet_tree_files
            .copy_from(&from.pos_predicate_wavelet_tree_files)
            .await?;

        copy_file(&from.neg_subjects_file, &self.neg_subjects_file).await?;
        copy_file(&from.neg_objects_file, &self.neg_objects_file).await?;
        self.neg_s_p_adjacency_list_files
            .copy_from(&from.neg_s_p_adjacency_list_files)
            .await?;
        self.neg_sp_o_adjacency_list_files
            .copy_from(&from.neg_sp_o_adjacency_list_files)
            .await?;
        self.neg_o_ps_adjacency_list_files
            .copy_from(&from.neg_o_ps_adjacency_list_files)
            .await?;
        self.neg_predicate_wavelet_tree_files
            .copy_from(&from.neg_predicate_wavelet_tree_files)
            .await?;

        Ok(())
    }
}
