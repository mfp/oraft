
## Implementation of Raft consensus algorithm

oraft consists of:

* a core Raft (purely functional) state machine featuring leader election +
  log replication, log compaction via snapshotting, cluster membership change
  support and efficient linearizable read-only operations. This state machine
  is abstracted over the specifics of peer communication (I/O, message
  serialization), concurrency and timeouts. (`Oraft` module)

* a specialization of the above state machine using the Lwt library for
  concurrency (`Oraft_lwt` module)

* a replicated state machine built atop `Oraft_lwt` (`RSM` module)

* a sample distributed dictionary client/server (`dict.ml`) built atop `RSM`

## Status

The core state machine has been tested using a discrete event simulator
that simulates node failures, network failures, message loss, random delays,
cluster changes (node deployment and decommissioning)...

## Performance

The sample distributed dictionary has been clocked at rates exceeding 70000
ops/s on a 3-node cluster. There is potential for optimization, both in the
core state machine (by decreasing GC pressure via lesser copying) and the
communication layer (faster message serialization, command batching).

## References

In Search of an Understandable Consensus Algorithm. Diego Ongaro and John
Ousterhout. Stanford University. (Draft of October 7, 2013).
https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
