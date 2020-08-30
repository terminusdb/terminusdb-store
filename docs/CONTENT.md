# Content Hashing

Right now we have various places where we assign a random name. It'd be much better if these names were content hashes instead.

A content hash is a kind of name that is calculated from the content. hashing involves a one-way function that takes an arbitrary string of data and condenses it to a fixed-length number. This number has no obvious relationship back to the original *content*. A good hash function has the property that when you flip one bit in the input data, each bit of the hash has a 50% chance to flip as well. In other words, a good hash appears to be pretty much a random string.

## Advantages of content hashing

A content hash has a few advantages over a random name.

The primary advantage is that hashing gives some assurance against data tampering. It is easy to check that a hash actually matches the content. If we use hashes as names for commits and the like, people can check that nobody tampered with a commit after it was named.

A second advantage is that hashes for names are lot more secure. Our present random ID implementation has a serious vulnerability where one may pre-seed a store with carefully named layers which then interfere with another database. This will not be possible with content hashing.

A third advantage is that if two people independently make the same change, it'd be hashed to the same name. This prevents accidental data duplication.

## Levels of hashing

### Layers

Layers are the base storage method within terminusdb. Layers have the following bits of information:

- an optional parent layer
- added nodes, predicates and values
- added triples
- removed triples

We can't just hash the underlying data files, cause that'd tie us to this particular storage method forever. That'd be bad cause there's loads of optimizations that can still be made. It is therefore best if we come up with a nice serialization format which can remain more fixed and which will serve as the basis for our hash. As an extra advantage, this could be part of our actual export/import mechanism too eventually.

An important detail is that all the content should be serialized in lexical order, so that the same content always gives the same serialized string and therefore the same graph.

By including the parent id, we ensure a hash chain is formed, where each layer hash transitively also hashes all parent layers.

### Commits

Commits are the basic unit of work in terminusdb. This is what people are actually interested in sharing with each other. Commits are made up of the following parts:

- an optional parent commit
- a bit of metadata, consisting of
-- author
-- message
-- timestamp
- a list of graphs, each of which has
-- a name
-- a type
-- a layer id

We don't necessarily know yet what will be part of a commit in the future. Therefore for any serialization format we need to leave some things open. In particular, I think that the metadata should just be json document, so that we're free to change the details on this later. Also, it may be a good idea to have graph type be something arbitrary, rather than just schema, instance or inference.

The entire commit serialization format should probably just be a json document, as there's no weird binary data involved here. Again though, it's important that we have a fixed order of fields, for example alphabetical ordering, so that we can be sure that the same commit always generates the same serialized document and therefore the same hash.

By including the parent commit, we ensure a hash chain is formed, where each commit hash transitively also hashes all parent commits.

### Repositories

Repositories are an entire commit graph. This means that we could probably use the underlying layer id directly as the content hash for the commit graph. However, we may want to consider hashing just the data, while ignoring the layer parent. We generally don't care about the history of the commit graph, since the commit graph itself is used to register history, so maywe we should consider two commit graphs to be the same commit graph as long as their data matches, even if their parent layers do not match.

This consideration can be pushed into the future though, and for now we can easily get away with just considering the layer id to be the content hash of a commit graph.

## Hashing algorithm

There are many hash algorithms we could choose from. It doesn't really matter much which one we choose, as long as there are no known collision or preimage attacks. If anyone has strong opinions on this I'd like to know. I'm currently considering either SHA2 or BLAKE.

## Hash length

For backwards compatibility we could take any hash function that generates a number that is larger than 160 bits, and cut it down to 160 bits, which is our current id length. We may want to consider going with a longer id though, which increases our collision domain (and therefore reduces the chance of collisions, though this is already very low as it is). This would be the moment to do so.
