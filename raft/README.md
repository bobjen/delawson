# Shared Raft

This crate owns consensus, member identity/recovery, journals, checkpoints, leases,
and scheduling of committed client actions. KS is its first client.

`runtime::Host` supplies client action application, checkpoint capture, optional
peer setup messages, and client dispatch. `execution::Application` supplies the
applied index. The host contains `Runtime<Application>` and dereferences to it.
KS's implementation is in `ks/src/runtime.rs`.

`Command::Client(Vec<Action>)` is the only client-specific consensus variant.
Each action has an `Op` and serialized fields. Raft iterates the list; the client
matches the action tag. KS encrypts each action's fields independently. Its outer
tag is visible. Services without secret application state can use plaintext.
Use the shared `execution` cursor and lock-budget iterator to avoid replaying a
successful action when execution yields within an entry.

`raft-main` exclusively owns `Node`. Helpers call `Host::on_raft` with owned inputs
and obtain owned results. **Never call `on_raft` from an owner callback**: nested
queue waits would deadlock. Call routines directly with that callback's `node`.
Do not perform network waits, application locking, or client application in owner
callbacks. Journal persistence is still synchronous and must succeed before an
acknowledgment; asynchronous disk submission is not introduced by this refactor.

`raft-application` is a separate thread, with no per-action workers. It receives
coalesced wakeups and runs queued committed work synchronously. The global write
lock can block this worker without blocking consensus. Lock-budget expiry yields
between actions, never midway through one. Application completion is distinct from
commitment and is required before reporting a write finished.

Checkpoint helpers hold the global read lock through client capture and file write.
`checkpoint_actions` can produce a compact restore action list; it need not retain
all historical commands. KS currently preserves its small list of encrypted key
entries with their original index/term bindings. Large-state streaming remains
future work. Encoding and I/O happen outside the consensus loop; journal rotation
and installing the completed checkpoint happen through it.

All persisted and transmitted structures use the existing binary StructWriter /
StructReader format and Op tags. The shared codec includes a depth byte. Journals
retain their length/complement framing and SHA-256 corruption check. No previous
JSON format compatibility is required.

Routine tests use in-memory storage and direct calls, including a blocked client
action while the owner handles a heartbeat and commitment. Filesystem tests are
ignored by default; run them explicitly with:

```
cargo test -p delawson-raft -p delawson-ks --features delawson-common/testing -- --ignored
```

Normal logging includes entry acceptance, commitment, application, and received
heartbeats. Additional logs cover elections, membership changes, checkpoint work,
and failure/recovery transitions. Logs contain metadata, not action contents.

Copied journal ranges and checkpoints require the current primary to confirm the
ending (index, term), with the leader and election term checked again before
installation. Compacted positions can be proved from retained snapshots and
journals. Unavailable history and mismatching terms reject a checkpoint. Bulk
transfer remains secondary-first; the proof is a small metadata request.

Journal headers (DLAWKR05) include the preceding logical index and term. Local
replay checks these plus member-local batch sequences at file boundaries. When
starting from a snapshot, an initial Carry record can restore the omitted suffix,
but must end at the header's predecessor. Peer transfers never compare different
members' physical sequences. These checks do not detect #127's reuse of both term
and index, or repair already-divergent committed histories.
