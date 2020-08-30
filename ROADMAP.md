# Terminus Store Roadmap

Terminus Store is the backend storage layer for TerminusDB. However,
it is designed to be an independently useful library for storing and
searching graphs using the RDF data model.

The library is already quite effective for storing graphs with edges
in the tens of millions compactly. However there are a number of
important features that are lacking which we would like to implement
in the future.

In rough order of priority we've outlined the following features that
we hope to implement. If you've an interest in working on one of these
features that would be fantastic, and you should contact the
maintainer <matthijs@terminusdb.com> and we can try to help you get
started.

* Content addressable hashing

    Layers are currently addressed using a randomly generated
    identifier. It would be much better to generate this from a hash
    of the data. This will simplify much of layer management and avoid
    duplication.

    More on this issue can be found at [here](./docs/CONTENT.md).

* Lexical xsd type storage

    Currently we store all datatypes in a canonical xsd format. The
    lexical ordering is unfortunately not the same as the natural
    ordering of the domain. This will make range based searches fast
    and will significantly reduce storage space.

    More on this issue can be found at [here](./docs/LEXICAL.md).

* Blob-store backend

    We have tried to write the library with the idea of plugging in
    various backends in the future. Something like an S3 backend would
    be desirable.

* Garbage collection

    We need to do cleanup of layers when they are no longer referenced.

    More on this issue can be found at [here](./docs/GARBAGE.md).

* Encrypted Layers

    Many operations do not require access to the underlying content. This means we could use a public-key crypto-system to allow layer storage and transport without revealing layer content.
