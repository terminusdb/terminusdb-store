# Garbage Collection

Currently we have no garbage collection, meaning that old layers that aren't referred to aren't ever removed. What follows is a proposal for adding it.

## When is it ok to delete a layer?

A layer should not be deleted when

- A label points at it
- It is loaded in memory
- It is pointed at by a layer that is not to be deleted

Any other layer may be deleted.

This is an inductive definition. A garbage collector will have to scan for all reachable layers, deleting the ones that are unreachable.

## Distributed setting and leases

I try to design terminus-store in a way which allows multiple backends to use the same physical storage without active cooperation between database nodes. No node should have to know about the existence of any other node. Having access to the physical storage should be enough.

This should extend to garbage collection. Unfortunately, that means we cannot do active garbage collection, where we actively find out what graphs are currently in memory, as stores would need to be prepared to handle messages for that, which adds complexity.

I propose we use a lease mechanism, where any time someone loads a stack of layers into memory, the head of the stack is marked in persistent storage with the current timestamp. Then, at a fixed interval (for example, on an hourly basis), it should refresh this timestamp. Likewise, whenever someone manipulates a label, this should mark the layer pointed at by that label similarly.

This way we can find out which layers were used recently, giving us a conservative estimate of the layers that may be in memory.

## Garbage collection in a separate process

Given a particular set of label files and leases, it is possible to find out what layers exist but aren't in use. I propose we delegate this task to a separate process, a garbage collector tool which can either be run interactively against a store, or be scheduled to run periodically. Doing this in a separate process keeps the database nodes more simple.

## Algorithm and Timing

As described above, layers will be 'leased' for a fixed time period, for example an hour. The garbage collection process, knowing this timespan, should be able to derive a timespan after which  garbage collection of a layer is allowed. for example, two hours.

The proposed algorithm is as follows:

1. Mark time
2. Retrieval:
    1. Retrieve a list of all layers
    2. Retrieve a list of all labels
    3. Retrieve a list of all valid leases (eg less than 2 hours ago)
3. Remove all reachable layers from the list of layers retrieved in 2.1.
4. Compare current time with time from step 1. If the whole computation happened in a brief period (eg 15 minutes), we should now have a valid list of layers that may be removed. If not, abort.
5. Delete unreachable layers.

## Layer Saving and Loading robustness

For many reasons, a database node may become unavailable, incapable of reaching the physical storage. In exceptional cases, this would lead to a node thinking some layers still exist, when in fact they do not.

It is therefore necessary, when saving a new layer, to check that all ancestor layers actually exist, and to error when one of the ancestors has disappeared. Likewise, when loading a layer, we should be prepared for the scenario where a layer is missing, and error appropriately.

Such errors should eventually be displayed to the user as a transaction failure or something similar.

Again, it is pretty unlikely that this will happen, especially with conservative time frames for garbage collection. But given the CAP theorem, we can never fully exclude the possibility of a failing node, and in those cases we firmly choose consistency over availability.
