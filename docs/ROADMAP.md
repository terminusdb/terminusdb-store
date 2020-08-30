# Terminus Store Roadmap

Terminus Store is the backend storage layer for TerminusDB. However, it is designed to be an independently useful library for storing and searching graphs using the RDF data model.

The library is already quite effective for storing graphs with edges in the tens of millions compactly. However there are a number of important features that are lacking which we would like to implement in the future.

In rough order of priority we've outlined the following features that we hope to implement. If you've an interest in working on one of these features that would be fantastic, and you should contact the maintainer <matthijs@terminusdb.com> and we can try to help you get started.

* Content addressable hashing

    Layers are currently addressed using a randomly generated identifier. It would be much better to generate this from a hash of the data. This will simplify much of layer management and avoid duplication.

    More on this issue can be found at [here](./CONTENT.md).

* Lexical xsd type storage

    Currently we store all datatypes in a canonical xsd format. The lexical ordering is unfortunately not the same as the natural ordering of the domain. This will make range based searches fast and will significantly reduce storage space.

    More on this issue can be found at [here](./LEXICAL.md).

* Blob-store backend

    We have tried to write the library with the idea of plugging in various backends in the future. Something like an S3 backend would be desirable.

* Garbage collection

    We currently are not doing automatic cleanup of layers. We would like this operation to be initiated with a parallel garbage collection process.

    Mor on this issue can be found [here](./GARBAGE.md)
