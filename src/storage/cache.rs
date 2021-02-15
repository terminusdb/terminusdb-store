use super::layer::*;
use crate::layer::*;
use futures::future::{self, Future};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, RwLock, Weak};

pub trait LayerCache: 'static + Send + Sync {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>>;
    fn cache_layer(&self, layer: Arc<InternalLayer>);

    fn invalidate(&self, name: [u32; 5]);
}

pub struct NoCache;

impl LayerCache for NoCache {
    fn get_layer_from_cache(&self, _name: [u32; 5]) -> Option<Arc<InternalLayer>> {
        None
    }

    fn cache_layer(&self, _layer: Arc<InternalLayer>) {}

    fn invalidate(&self, _name: [u32; 5]) {}
}

lazy_static! {
    pub static ref NOCACHE: Arc<dyn LayerCache> = Arc::new(NoCache);
}

// locking isn't really ideal but the lock window will be relatively small so it shouldn't hurt performance too much except on heavy updates.
// ideally we should be using some concurrent hashmap implementation instead.
// furthermore, there should be some logic to remove stale entries, like a periodic pass. right now, there isn't.
#[derive(Default)]
pub struct LockingHashMapLayerCache {
    cache: RwLock<HashMap<[u32; 5], Weak<InternalLayer>>>,
}

impl LockingHashMapLayerCache {
    pub fn new() -> Self {
        Default::default()
    }
}

impl LayerCache for LockingHashMapLayerCache {
    fn get_layer_from_cache(&self, name: [u32; 5]) -> Option<Arc<InternalLayer>> {
        let cache = self
            .cache
            .read()
            .expect("rwlock read should always succeed");

        let result = cache.get(&name).map(|c| c.to_owned());
        std::mem::drop(cache);

        match result {
            None => None,
            Some(weak) => match weak.upgrade() {
                None => {
                    self.cache
                        .write()
                        .expect("rwlock write should always succeed")
                        .remove(&name);
                    None
                }
                Some(result) => Some(result),
            },
        }
    }

    fn cache_layer(&self, layer: Arc<InternalLayer>) {
        let mut cache = self
            .cache
            .write()
            .expect("rwlock write should always succeed");
        cache.insert(InternalLayerImpl::name(&*layer), Arc::downgrade(&layer));
    }

    fn invalidate(&self, name: [u32; 5]) {
        // the dumb way - we just delete the thing from cache forcing a refresh
        let mut cache = self
            .cache
            .write()
            .expect("rwlock read should always succeed");

        cache.remove(&name);
    }
}

#[derive(Clone)]
pub struct CachedLayerStore {
    inner: Arc<dyn LayerStore>,
    cache: Arc<dyn LayerCache>,
}

impl CachedLayerStore {
    pub fn new<S: LayerStore, C: LayerCache>(inner: S, cache: C) -> CachedLayerStore {
        CachedLayerStore {
            inner: Arc::new(inner),
            cache: Arc::new(cache),
        }
    }

    pub fn invalidate(&self, name: [u32; 5]) {
        self.cache.invalidate(name);
    }
}

impl LayerStore for CachedLayerStore {
    fn layers(&self) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        self.inner.layers()
    }

    fn get_layer(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        self.inner.get_layer_with_cache(name, self.cache.clone())
    }

    fn get_layer_with_cache(
        &self,
        name: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Option<Arc<InternalLayer>>>> + Send>> {
        self.inner.get_layer_with_cache(name, cache)
    }

    fn create_base_layer(
        &self,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.inner.create_base_layer()
    }

    fn create_child_layer(
        &self,
        parent: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.inner
            .create_child_layer_with_cache(parent, self.cache.clone())
    }

    fn create_child_layer_with_cache(
        &self,
        parent: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn LayerBuilder>>> + Send>> {
        self.inner.create_child_layer_with_cache(parent, cache)
    }

    fn perform_rollup(
        &self,
        layer: Arc<InternalLayer>,
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        self.inner.perform_rollup(layer)
    }

    fn perform_rollup_upto_with_cache(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
        cache: Arc<dyn LayerCache>,
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        self.inner
            .perform_rollup_upto_with_cache(layer, upto, cache)
    }

    fn perform_rollup_upto(
        &self,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        self.inner
            .perform_rollup_upto_with_cache(layer, upto, self.cache.clone())
    }

    fn register_rollup(
        &self,
        layer: [u32; 5],
        rollup: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        // when registering a rollup layer, we need to make sure that
        // the cached version is updated as well.
        let cache = self.cache.clone();
        let register_rollup = self.inner.register_rollup(layer, rollup);

        Box::pin(async move {
            register_rollup.await?;
            cache.invalidate(layer);

            Ok(())
        })
    }

    fn rollup_upto(
        self: Arc<Self>,
        layer: Arc<InternalLayer>,
        upto: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<[u32; 5]>> + Send>> {
        let cache = self.cache.clone();
        self.rollup_upto_with_cache(layer, upto, cache)
    }

    fn export_layers(&self, layer_ids: Box<dyn Iterator<Item = [u32; 5]>>) -> Vec<u8> {
        self.inner.export_layers(layer_ids)
    }
    fn import_layers(
        &self,
        pack: &[u8],
        layer_ids: Box<dyn Iterator<Item = [u32; 5]>>,
    ) -> Result<(), io::Error> {
        self.inner.import_layers(pack, layer_ids)
    }

    fn layer_is_ancestor_of(
        &self,
        descendant: [u32; 5],
        ancestor: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        self.inner.layer_is_ancestor_of(descendant, ancestor)
    }

    fn triple_addition_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(
                    cached.internal_triple_addition_exists(subject, predicate, object),
                ));
            }
        }

        self.inner
            .triple_addition_exists(layer, subject, predicate, object)
    }

    fn triple_removal_exists(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(
                    cached.internal_triple_removal_exists(subject, predicate, object),
                ));
            }
        }

        self.inner
            .triple_removal_exists(layer, subject, predicate, object)
    }

    fn triple_additions(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(Box::new(cached.internal_triple_additions())
                    as Box<dyn Iterator<Item = _> + Send>));
            }
        }

        self.inner.triple_additions(layer)
    }

    fn triple_removals(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(Box::new(cached.internal_triple_removals())
                    as Box<dyn Iterator<Item = _> + Send>));
            }
        }

        self.inner.triple_removals(layer)
    }

    fn triple_additions_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(cached.internal_triple_additions_s(subject)));
            }
        }

        self.inner.triple_additions_s(layer, subject)
    }

    fn triple_removals_s(
        &self,
        layer: [u32; 5],
        subject: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(cached.internal_triple_removals_s(subject)));
            }
        }

        self.inner.triple_removals_s(layer, subject)
    }

    fn triple_additions_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(
                    cached.internal_triple_additions_sp(subject, predicate),
                ));
            }
        }

        self.inner.triple_additions_sp(layer, subject, predicate)
    }

    fn triple_removals_sp(
        &self,
        layer: [u32; 5],
        subject: u64,
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(
                    cached.internal_triple_removals_sp(subject, predicate),
                ));
            }
        }

        self.inner.triple_removals_sp(layer, subject, predicate)
    }

    fn triple_additions_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(
                    Box::new(cached.internal_triple_additions_p(predicate))
                        as Box<dyn Iterator<Item = _> + Send>,
                ));
            }
        }

        self.inner.triple_additions_p(layer, predicate)
    }

    fn triple_removals_p(
        &self,
        layer: [u32; 5],
        predicate: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(
                    Box::new(cached.internal_triple_removals_p(predicate))
                        as Box<dyn Iterator<Item = _> + Send>,
                ));
            }
        }

        self.inner.triple_removals_p(layer, predicate)
    }

    fn triple_additions_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(cached.internal_triple_additions_o(object)));
            }
        }

        self.inner.triple_additions_o(layer, object)
    }

    fn triple_removals_o(
        &self,
        layer: [u32; 5],
        object: u64,
    ) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn Iterator<Item = IdTriple> + Send>>> + Send>>
    {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(cached.internal_triple_removals_o(object)));
            }
        }

        self.inner.triple_removals_o(layer, object)
    }

    fn triple_layer_addition_count(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(cached.internal_triple_layer_addition_count()));
            }
        }

        self.inner.triple_layer_addition_count(layer)
    }

    fn triple_layer_removal_count(
        &self,
        layer: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<usize>> + Send>> {
        if let Some(cached) = self.cache.get_layer_from_cache(layer) {
            if !cached.is_rollup() {
                return Box::pin(future::ok(cached.internal_triple_layer_removal_count()));
            }
        }

        self.inner.triple_layer_removal_count(layer)
    }

    fn retrieve_layer_stack_names(
        &self,
        name: [u32; 5],
    ) -> Pin<Box<dyn Future<Output = io::Result<Vec<[u32; 5]>>> + Send>> {
        // Note: Doesn't use cache, but does pointer chasing on disk anyhow
        self.inner.retrieve_layer_stack_names(name)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::storage::directory::*;
    use crate::storage::memory::*;
    use tempfile::tempdir;

    fn cached_layer_eq(layer1: &dyn Layer, layer2: &dyn Layer) -> bool {
        // a trait object consists of two parts, a pointer to the concrete data, followed by a vtable.
        // we consider two layers equal if that first part, the pointer to the concrete data, is equal.
        unsafe {
            let ptr1 = *(layer1 as *const dyn Layer as *const usize);
            let ptr2 = *(layer2 as *const dyn Layer as *const usize);
            ptr1 == ptr2
        }
    }

    #[tokio::test]
    async fn cached_memory_layer_store_returns_same_layer_multiple_times() {
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let layer1 = store.get_layer(child_name).await.unwrap().unwrap();
        let layer2 = store.get_layer(child_name).await.unwrap().unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = store.get_layer(base_name).await.unwrap().unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[tokio::test]
    async fn cached_directory_layer_store_returns_same_layer_multiple_times() {
        let dir = tempdir().unwrap();
        let store = CachedLayerStore::new(
            DirectoryLayerStore::new(dir.path()),
            LockingHashMapLayerCache::new(),
        );
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        builder = store.create_child_layer(base_name).await.unwrap();
        let child_name = builder.name();

        builder.remove_string_triple(StringTriple::new_value("duck", "says", "quack"));
        builder.add_string_triple(StringTriple::new_node("cow", "likes", "pig"));

        builder.commit_boxed().await.unwrap();

        let layer1 = store.get_layer(child_name).await.unwrap().unwrap();
        let layer2 = store.get_layer(child_name).await.unwrap().unwrap();

        let base_layer = store.cache.get_layer_from_cache(base_name).unwrap();
        let base_layer_2 = store.get_layer(base_name).await.unwrap().unwrap();

        assert!(cached_layer_eq(&*layer1, &*layer2));
        assert!(cached_layer_eq(&*base_layer, &*base_layer_2));
    }

    #[tokio::test]
    async fn cached_layer_store_forgets_entries_when_they_are_dropped() {
        let store = CachedLayerStore::new(MemoryLayerStore::new(), LockingHashMapLayerCache::new());
        let mut builder = store.create_base_layer().await.unwrap();
        let base_name = builder.name();

        builder.add_string_triple(StringTriple::new_value("cow", "says", "moo"));
        builder.add_string_triple(StringTriple::new_value("pig", "says", "oink"));
        builder.add_string_triple(StringTriple::new_value("duck", "says", "quack"));

        builder.commit_boxed().await.unwrap();

        let layer = store.get_layer(base_name).await.unwrap().unwrap();
        let weak = Arc::downgrade(&layer);

        // we expect 2 weak pointers, the one we made above and the one stored in cache
        assert_eq!(2, Arc::weak_count(&layer));

        // forget the layers
        std::mem::drop(layer);

        // according to our weak reference, there's no longer any strong reference around
        assert!(weak.upgrade().is_none());

        // retrieving the same layer again works just fine
        let layer = store.get_layer(base_name).await.unwrap().unwrap();

        // and only has one weak pointer pointing to it, the newly cached one
        assert_eq!(1, Arc::weak_count(&layer));
    }

    #[test]
    fn retrieve_layer_stack_names_retrieves_correctly() {
        //let store = CachedLayerStore::new(MemoryLayerStore::new());
        //let builder = store.create_base_layer().wait().unwrap();
    }
}

#[cfg(test)]
mod delta_tests {
    use super::*;
    use crate::storage::directory::DirectoryLayerStore;

    use tempfile::{tempdir, TempDir};

    async fn example_base_layer(
        store: &CachedLayerStore,
        invalidate: bool,
    ) -> io::Result<([u32; 5], HashMap<StringTriple, IdTriple>)> {
        let (name, additions) = crate::storage::layer::tests::example_base_layer(store).await?;

        if invalidate {
            store.invalidate(name);
        }

        Ok((name, additions))
    }

    async fn example_child_layer(
        store: &CachedLayerStore,
        invalidate: bool,
    ) -> io::Result<(
        [u32; 5],
        HashMap<StringTriple, IdTriple>,
        HashMap<StringTriple, IdTriple>,
    )> {
        let (name, additions, removals) =
            crate::storage::layer::tests::example_child_layer(store).await?;

        if invalidate {
            store.invalidate(name);
        }

        Ok((name, additions, removals))
    }

    fn make_store() -> (TempDir, CachedLayerStore) {
        let dir = tempdir().unwrap();
        let store = DirectoryLayerStore::new(dir.path());
        let cache = CachedLayerStore::new(store, LockingHashMapLayerCache::new());

        (dir, cache)
    }

    async fn base_layer_counts(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _) = example_base_layer(store, invalidate).await?;
        assert_eq!(11, store.triple_layer_addition_count(name).await?);
        assert_eq!(0, store.triple_layer_removal_count(name).await?);

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_counts() {
        let (_dir, store) = make_store();
        base_layer_counts(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_counts() {
        let (_dir, store) = make_store();
        base_layer_counts(&store, true).await.unwrap();
    }

    async fn child_layer_counts(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _, _) = example_child_layer(store, invalidate).await?;
        assert_eq!(5, store.triple_layer_addition_count(name).await?);
        assert_eq!(6, store.triple_layer_removal_count(name).await?);

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_counts() {
        let (_dir, store) = make_store();
        child_layer_counts(&store, true).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_counts() {
        let (_dir, store) = make_store();
        child_layer_counts(&store, true).await.unwrap();
    }

    async fn base_layer_addition_exists(
        store: &CachedLayerStore,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, triples) = example_base_layer(store, invalidate).await?;

        for t in triples.values() {
            assert!(
                store
                    .triple_addition_exists(name, t.subject, t.predicate, t.object)
                    .await?
            );
        }

        assert!(!store.triple_addition_exists(name, 42, 42, 42).await?);
        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_addition_exists() {
        let (_dir, store) = make_store();
        base_layer_addition_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_addition_exists() {
        let (_dir, store) = make_store();
        base_layer_addition_exists(&store, true).await.unwrap();
    }

    async fn base_layer_additions(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, triples) = example_base_layer(store, invalidate).await?;
        let mut values: Vec<_> = triples.values().cloned().collect();
        values.sort();

        let additions: Vec<_> = store.triple_additions(name).await?.collect();
        assert_eq!(values, additions);

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_additions() {
        let (_dir, store) = make_store();
        base_layer_additions(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions() {
        let (_dir, store) = make_store();
        base_layer_additions(&store, true).await.unwrap();
    }

    async fn base_layer_additions_s(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(5, triples.len());

        let result: Vec<_> = store
            .triple_additions_s(name, triples[0].subject)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_s(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_additions_s() {
        let (_dir, store) = make_store();
        base_layer_additions_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_s() {
        let (_dir, store) = make_store();
        base_layer_additions_s(&store, true).await.unwrap();
    }

    async fn base_layer_additions_sp(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow" && t.predicate == "says")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_sp(name, triples[0].subject, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store
            .triple_additions_sp(name, 42, 42)
            .await?
            .next()
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_additions_sp() {
        let (_dir, store) = make_store();
        base_layer_additions_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_sp() {
        let (_dir, store) = make_store();
        base_layer_additions_sp(&store, true).await.unwrap();
    }

    async fn base_layer_additions_p(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.predicate == "says")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(4, triples.len());

        let result: Vec<_> = store
            .triple_additions_p(name, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_p(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_additions_p() {
        let (_dir, store) = make_store();
        base_layer_additions_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_p() {
        let (_dir, store) = make_store();
        base_layer_additions_p(&store, true).await.unwrap();
    }

    async fn base_layer_additions_o(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents) = example_base_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.object == ObjectType::Node("cow".to_string()))
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_o(name, triples[0].object)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_additions_o() {
        let (_dir, store) = make_store();
        base_layer_additions_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_additions_o() {
        let (_dir, store) = make_store();
        base_layer_additions_o(&store, true).await.unwrap();
    }

    async fn base_layer_removals(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _) = example_base_layer(store, invalidate).await?;

        assert!(!store.triple_removal_exists(name, 42, 42, 42).await?);
        assert!(store.triple_removals(name).await?.next().is_none());
        assert!(store.triple_removals_s(name, 42).await?.next().is_none());
        assert!(store
            .triple_removals_sp(name, 42, 42)
            .await?
            .next()
            .is_none());
        assert!(store.triple_removals_p(name, 42).await?.next().is_none());
        assert!(store.triple_removals_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_base_layer_removals() {
        let (_dir, store) = make_store();
        base_layer_removals(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_base_layer_removals() {
        let (_dir, store) = make_store();
        base_layer_removals(&store, true).await.unwrap();
    }

    async fn child_layer_addition_exists(
        store: &CachedLayerStore,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, triples, _) = example_child_layer(store, invalidate).await?;

        for t in triples.values() {
            assert!(
                store
                    .triple_addition_exists(name, t.subject, t.predicate, t.object)
                    .await?
            );
        }

        assert!(!store.triple_addition_exists(name, 42, 42, 42).await?);
        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_addition_exists() {
        let (_dir, store) = make_store();
        child_layer_addition_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_addition_exists() {
        let (_dir, store) = make_store();
        child_layer_addition_exists(&store, true).await.unwrap();
    }

    async fn child_layer_additions(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, triples, _) = example_child_layer(store, invalidate).await?;
        let mut values: Vec<_> = triples.values().cloned().collect();
        values.sort();

        let additions: Vec<_> = store.triple_additions(name).await?.collect();
        assert_eq!(values, additions);

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_additions() {
        let (_dir, store) = make_store();
        child_layer_additions(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions() {
        let (_dir, store) = make_store();
        child_layer_additions(&store, true).await.unwrap();
    }

    async fn child_layer_additions_s(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(3, triples.len());

        let result: Vec<_> = store
            .triple_additions_s(name, triples[0].subject)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_s(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_additions_s() {
        let (_dir, store) = make_store();
        child_layer_additions_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_s() {
        let (_dir, store) = make_store();
        child_layer_additions_s(&store, true).await.unwrap();
    }

    async fn child_layer_additions_sp(
        store: &CachedLayerStore,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.subject == "cow" && t.predicate == "says")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_sp(name, triples[0].subject, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store
            .triple_additions_sp(name, 42, 42)
            .await?
            .next()
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_additions_sp() {
        let (_dir, store) = make_store();
        child_layer_additions_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_sp() {
        let (_dir, store) = make_store();
        child_layer_additions_sp(&store, true).await.unwrap();
    }

    async fn child_layer_additions_p(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.predicate == "likes")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_p(name, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_p(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_additions_p() {
        let (_dir, store) = make_store();
        child_layer_additions_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_p() {
        let (_dir, store) = make_store();
        child_layer_additions_p(&store, true).await.unwrap();
    }

    async fn child_layer_additions_o(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, contents, _removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = contents
            .iter()
            .filter(|(t, _)| t.object == ObjectType::Node("platypus".to_string()))
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_additions_o(name, triples[0].object)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_additions_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_additions_o() {
        let (_dir, store) = make_store();
        child_layer_additions_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_additions_o() {
        let (_dir, store) = make_store();
        child_layer_additions_o(&store, true).await.unwrap();
    }

    async fn child_layer_removal_exists(
        store: &CachedLayerStore,
        invalidate: bool,
    ) -> io::Result<()> {
        let (name, _, removals) = example_child_layer(store, invalidate).await?;

        for t in removals.values() {
            assert!(
                store
                    .triple_removal_exists(name, t.subject, t.predicate, t.object)
                    .await?
            );
        }

        assert!(!store.triple_removal_exists(name, 42, 42, 42).await?);
        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_removal_exists() {
        let (_dir, store) = make_store();
        child_layer_removal_exists(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removal_exists() {
        let (_dir, store) = make_store();
        child_layer_removal_exists(&store, true).await.unwrap();
    }

    async fn child_layer_removals(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _, removals) = example_child_layer(store, invalidate).await?;
        let mut values: Vec<_> = removals.values().cloned().collect();
        values.sort();

        let removals: Vec<_> = store.triple_removals(name).await?.collect();
        assert_eq!(values, removals);

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_removals() {
        let (_dir, store) = make_store();
        child_layer_removals(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals() {
        let (_dir, store) = make_store();
        child_layer_removals(&store, true).await.unwrap();
    }

    async fn child_layer_removals_s(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.subject == "duck")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(3, triples.len());

        let result: Vec<_> = store
            .triple_removals_s(name, triples[0].subject)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_removals_s(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_removals_s() {
        let (_dir, store) = make_store();
        child_layer_removals_s(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_s() {
        let (_dir, store) = make_store();
        child_layer_removals_s(&store, true).await.unwrap();
    }

    async fn child_layer_removals_sp(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.subject == "duck" && t.predicate == "hates")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_removals_sp(name, triples[0].subject, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store
            .triple_removals_sp(name, 42, 42)
            .await?
            .next()
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_removals_sp() {
        let (_dir, store) = make_store();
        child_layer_removals_sp(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_sp() {
        let (_dir, store) = make_store();
        child_layer_removals_sp(&store, true).await.unwrap();
    }

    async fn child_layer_removals_p(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.predicate == "hates")
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(3, triples.len());

        let result: Vec<_> = store
            .triple_removals_p(name, triples[0].predicate)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_removals_p(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_removals_p() {
        let (_dir, store) = make_store();
        child_layer_removals_p(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_p() {
        let (_dir, store) = make_store();
        child_layer_removals_p(&store, true).await.unwrap();
    }

    async fn child_layer_removals_o(store: &CachedLayerStore, invalidate: bool) -> io::Result<()> {
        let (name, _additions, removals) = example_child_layer(store, invalidate).await?;

        let mut triples: Vec<_> = removals
            .iter()
            .filter(|(t, _)| t.object == ObjectType::Node("cow".to_string()))
            .map(|(_, t)| t)
            .cloned()
            .collect();
        triples.sort();
        assert_eq!(2, triples.len());

        let result: Vec<_> = store
            .triple_removals_o(name, triples[0].object)
            .await?
            .collect();
        assert_eq!(triples, result);

        assert!(store.triple_removals_o(name, 42).await?.next().is_none());

        Ok(())
    }

    #[tokio::test]
    async fn cached_child_layer_removals_o() {
        let (_dir, store) = make_store();
        child_layer_removals_o(&store, false).await.unwrap();
    }

    #[tokio::test]
    async fn uncached_child_layer_removals_o() {
        let (_dir, store) = make_store();
        child_layer_removals_o(&store, true).await.unwrap();
    }
}
