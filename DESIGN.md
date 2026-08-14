# DeLawson — Design

Status: **draft, pre-implementation.** Nothing here is built. Sections marked
⚠️ are unresolved; they are collected again in §12.

Language: Rust.

---

## 1. Goals

- A single object may hold up to **2^64 records**.
- No single machine can hold such an object. Objects span many machines by
  construction, not as an optimization.
- **Tiny objects are equally expected.** Different users behave differently;
  the same system must hold a 30-record object and a 2^64-record object
  without the small case paying for the large one. See §9.
- **No per-record index anywhere.** An index with 2^64 entries is not
  storable, so record location must be computed from aggregate counts (or key
  ranges) plus a bounded scan.
- Durability by replication while data is being written, by erasure coding
  once it is immutable.
- Sequential read throughput approaching the aggregate sequential bandwidth of
  the disks holding the object.

## 2. Non-goals

- Not a SQL database. No query language, no joins, no planner.
- No cross-object transactions.
- No general secondary indexes.

---

## 3. Two object flavors

The flavor is fixed at creation and determines how records are addressed.

| | **Unkeyed** | **Keyed** |
|---|---|---|
| Address | 64-bit record number | sorted user key |
| Insert | **only at the end of the object** | anywhere, by key |
| Update | yes, count-preserving | yes |
| Delete | tombstone, count-preserving | tombstone |
| Positional access | yes, `O(log)` | no — see §7.4 |

The asymmetry follows from one fact: **record counts are stable under update
and tombstoned delete, but not under insert.** An update replaces a record and
a tombstone leaves the record present-but-dead, so in both cases every
cumulative count in the object is unchanged. An insert in the middle
renumbers every record after it, invalidating every cumulative count above the
insertion point.

Therefore:

- **Unkeyed objects forbid mid-object insert.** Appends land at the end of the
  object, where they touch no existing cumulative count. Record numbers are
  then permanently stable, and the count-based addressing of §7.1 works.
- **Keyed objects abandon counts as addresses.** Insert-in-the-middle is the
  point of them, so record count is unusably unstable and keys are the address
  instead. Counts are still maintained, but only as statistics — for `count`,
  for load balancing, for placement — never for lookup.

---

## 4. The four levels

```
Object
  ├─ metadata (flavor, framing, device class, totals; inline records if tiny — §9)
  └─ streams[]     unkeyed: (stream_id, cumulative records before, cumulative bytes before)
       │           keyed:   (stream_id, first_key)
       Stream                        unit of capacity striping
         └─ extents[]  unkeyed: (extent_id, cumulative records before, cumulative bytes before)
              │        keyed:   (extent_id, first_key)
              Extent                 unit of replication, sealing, and erasure coding
                ├─ base              sealed, immutable
                ├─ log               unsealed, lazily created — updates, tombstones, keyed inserts
                └─ instances[]        replicas, or EC fragments after sealing
                     │
                     Instance        one contiguous block of bytes on one disk
```

Each level answers exactly one question, by binary search over an ordered
array — of cumulative counts for unkeyed objects, of first keys for keyed ones:

| Level | Question | Key |
|---|---|---|
| Object | which stream? | cumulative records / first key |
| Stream | which extent? | cumulative records / first key |
| Extent | roughly where inside? | sparse offset index (§7.3) |
| Instance | which disks hold those bytes? | replica / fragment list |

### 4.1 Streams

A stream exists **purely for capacity striping**. It carries no semantic
distinction — there is no "index stream" or "log stream". An object small
enough to fit in one stream uses exactly one stream; streams are added as it
grows.

A stream is the granularity at which an object is spread across the cluster,
and the granularity at which extent metadata is partitioned. **Streams of 1 PB
and larger are expected** — §11 shows why they have to be that big.

### 4.2 Extents: base plus log

An extent is the unit of replication and of immutability. Logically it is a
**pair**:

- a **base**: sealed, immutable, sorted (keyed) or in record order (unkeyed);
- a **log**: unsealed and appendable, holding updates, tombstones, and — for
  keyed extents — inserts falling in this extent's key range.

Reads merge base and log, newest version winning. **Compaction** periodically
folds the log into a fresh base and drops the old pair (§8).

**The log is created lazily.** An extent that has only ever been appended to
has no log at all, and costs nothing for it. This matters more than it looks:
a 1 PB stream holds ~250,000 extents (§11), and if each eagerly carried an
open, fully-replicated log, the cluster's open-extent count — the expensive,
un-erasure-coded, in-memory-state-bearing kind — would exceed its sealed
extent count. For append-mostly objects almost no logs exist.

⚠️ Even lazily, an update-heavy 1 PB stream can accumulate ~250,000 open logs.
Whether that is acceptable, or whether logs need to be shared across
neighbouring extents once their count crosses a threshold, is open.

Sealing:

- At most one extent per stream is open for **append** at a time (this is the
  object's tail, distinct from logs).
- When it reaches its size limit it is **sealed**: byte length, record count,
  and content fixed forever.
- Sealing is what makes everything else cheap. A sealed base's cumulative
  counts never change, its sparse index never changes, and its bytes can be
  erasure-coded, checksummed, and cached once.
- Extents may be sealed **short**. Nothing requires a full extent, which makes
  sealing available as a response to failure (§10).

### 4.3 Instances

- While an extent is open, each instance is a **full replica** on a distinct
  machine and distinct failure domain. Writes go to all replicas.
- After sealing, the extent is re-encoded to **erasure-coded fragments**;
  instances become fragments, reconstructible from any *k* of *n*. Replicas
  are dropped once encoding is durable and verified.

Cheap durability for the overwhelming majority of data (sealed, cold, EC) and
simple synchronous replication for the small hot tail.

⚠️ EC changes the read unit: what was one 1 MB sequential read against a
replica becomes *k* fragment reads across *k* machines, partly defeating the
device tuning of §7.3. Either align the fragment stripe with the index
granularity, or retain one full replica alongside the fragments for locality.

---

## 5. Size limits

| Bound | Value | Why |
|---|---|---|
| Extent, logical | ≤ **4 GB** | An intra-extent logical offset fits in 32 bits. |
| Extent, physical | limit configurable in **8 MB – 4 GB** | The 8 MB floor keeps seek cost small relative to scan cost. |
| Records per object | ≤ 2^64 | Stated requirement. |
| Stream | ≥ ~1 PB in practice | Forced by metadata sizing, §11. |

The 8 MB floor: an HDD seek plus rotation is ~10 ms, and 8 MB at ~150 MB/s is
~53 ms of scanning, so the seek is under 20% overhead. Below 8 MB the drive
spends more time finding extents than reading them. Above 4 GB the 32-bit
logical offset breaks.

⚠️ Logical ≤ 4 GB and physical ∈ [8 MB, 4 GB] are separate bounds, so the two
can differ. What exactly drives the difference — superseded versions and
tombstones still occupying logical space, compression, or EC expansion — is
not settled, and it determines whether logical offsets are dense.

---

## 6. Writing

### 6.1 Append blocks

The unit of write is an **append block** of 0..n records, applied to the target
extent atomically: either all its records become visible or none do. Block
size is the client's choice within a limit.

An append returns the record number of the first record in the block (unkeyed)
and the extent it landed in.

### 6.2 Record framing

Record boundaries within a block are found by the object's framing, fixed at
creation:

| Framing | Boundary |
|---|---|
| `LengthPrefixed` | varint or fixed-width length precedes each record |
| `Nul` | records terminated by `0x00` |
| `Lf` | records terminated by `\n` |
| `CrLf` | records terminated by `\r\n` |

`LengthPrefixed` is the only framing permitting arbitrary binary content; the
terminator framings require the terminator not appear inside a record, and
exist so that existing line- and NUL-delimited data can be stored and scanned
without a rewrite.

### 6.3 The three mutations

| | Unkeyed | Keyed |
|---|---|---|
| **Insert** | appended to the object's tail extent; allocates the next record number | written to the log of the extent whose key range covers the key |
| **Update** | `(local record number, new bytes)` appended to that extent's log | `(key, new bytes)` appended to the log |
| **Delete** | tombstone for the local record number, appended to the log | tombstone for the key, appended to the log |

Update and delete never touch the base, never change any cumulative count, and
never require an extent to be reopened. Insert is the only operation that
moves counts, which is why unkeyed inserts are confined to the object's end.

---

## 7. Reading

### 7.1 Unkeyed: locating record *N*

1. Binary search the object's stream list on cumulative records → stream *S*
   and a stream-local record number.
2. Binary search *S*'s extent list on cumulative records → extent *E* and an
   extent-local record number.
3. If *E* has a log, check it for a newer version or tombstone of that local
   record number. If found, done.
4. Otherwise binary search *E*'s base sparse index → nearest preceding
   checkpoint `(local record number, byte offset)`.
5. Read one device granule from that offset off any instance, parse forward by
   framing, stop at the target.

Two binary searches, one small log probe, one index search, one read. Costs
are logarithmic in stream and extent count; the forward scan is bounded by the
index granularity — never by the size of the object.

### 7.2 Keyed: locating key *K*

Identical in shape, with `first_key` comparisons replacing cumulative counts,
and the extent's sparse index mapping `key → byte offset`. The log probe is
not optional here: for keyed extents the log holds inserts, so a key absent
from the base may still exist.

### 7.3 Sparse index granularity

One checkpoint approximately every:

- **1 MB** for HDD-backed extents
- **20 KB** for SSD-backed extents

Granularity is set to the device's efficient read size. On an HDD the seek
dominates, a 1 MB read is nearly free relative to finding it, and there is no
point indexing finer than what you would read anyway. On an SSD there is no
seek to amortize, so much finer granularity pays for itself.

Cost, for a 4 GB extent and 8-byte checkpoints (32-bit local record number
plus 32-bit offset):

| Device | Granularity | Checkpoints | Index size | Overhead |
|---|---|---|---|---|
| HDD | 1 MB | ~4,100 | ~33 KB | 0.0008% |
| SSD | 20 KB | ~210,000 | ~1.7 MB | 0.04% |

Both are small enough to hold in memory for open extents and cache
aggressively for sealed ones. Device class is a property of the extent, not
the object, so an object may migrate between tiers extent by extent.

### 7.4 Scans

A full scan ignores the index: walk the stream list in order, the extent list
in order, and read each extent at full device bandwidth, merging its log.
Because streams sit on disjoint machine sets, scans of a large object
parallelize across the cluster naturally.

Keyed objects support ordered scans and range scans by key, but **not**
positional access — "the 10,000,000th record" is not answerable, because
counts are statistics rather than addresses (§3).

---

## 8. Compaction

Compaction reads a base plus its log, writes a fresh base with updates
applied, tombstones dropped, and (for keyed extents) inserts merged in key
order, then atomically swaps the pair in the stream's extent list.

Consequences:

- **Extent membership of a stream changes over time.** Extent *position*
  within a stream is therefore unstable; extent *identity* must be a stable
  unique id that outlives position.
- **Keyed compaction can split.** If the merged result exceeds the extent
  limit, it becomes two extents covering two key subranges, spliced into the
  stream's extent list. This makes keyed objects B-tree-shaped — one log level
  per base, splitting on overflow — rather than LSM-shaped with many levels.
- **Unkeyed compaction never splits or renumbers.** Updates and tombstones
  preserve count, so the new base covers exactly the record-number range the
  old one did. Cumulative counts above it are untouched. Unkeyed compaction is
  purely local.
- Compaction rewrites, so it is also the moment to re-encode EC, change device
  class, or re-checksum.

⚠️ Compaction policy — what triggers it, how much write amplification is
budgeted, and how splits are throttled — is not designed yet.

---

## 9. Small objects

Three regimes, chosen by size:

| Regime | When | Storage |
|---|---|---|
| **Inline** | very small | records live inside the object's metadata record — no stream, no extent, no instance |
| **Packed** | smaller than half a max-sized extent (< 2 GB logical) | the object shares one extent with other objects; the extent holds a directory of the objects in it |
| **Dedicated** | ≥ 2 GB logical | the object's own extents, as §4 |

The half-extent threshold for packing is what guarantees a packed object never
straddles an extent boundary: at 2 GB of a 4 GB extent, at least two objects
fit and any packable object fits whole.

⚠️ Packing raises the questions the uniform design avoids: deleting one packed
object leaves garbage until the shared extent is compacted; erasure coding is
per-extent, so packed objects share an EC fate; and the extent's directory is a
fourth kind of index. The inline threshold is also unset.

---

## 10. Metadata, consistency, and membership

Deliberately thinner than the rest — least determined so far.

- **Durability** on write comes from replicating an append block to all open
  instances before acknowledging. Sealing is the commit point that fixes an
  extent's content.
- **Metadata** (object → streams, stream → extents, extent → instances) is
  itself replicated state that must stay consistent under failure. This wants
  a small consensus-replicated metadata service per stream or per object, not
  one global one — §11 constrains which.
- **Replica failure on an open extent**: seal the extent short and open a new
  one. This removes replica repair from the write path entirely.
- **Repair** of sealed extents is background fragment reconstruction, never
  blocking a write.

⚠️ Open: metadata service per stream, per object, or sharded globally.

---

## 11. Does the metadata fit? (sizing)

The check that decides whether three levels suffice or a fourth is needed.

Take the stated ceiling: 2^64 ≈ 1.845 × 10^19 records. At an average 100 bytes
per record that is ~1.8 ZB — physically unreachable, but addressing must still
work, so use it as the bound.

At 4 GB per extent an extent holds ~4 × 10^7 such records, so a full object
implies ~4.6 × 10^11 extents. Extent metadata at ~64 bytes per entry is ~30 TB
in total. **No single machine holds the extent list** — and it does not have
to, because it is partitioned by stream. That is what streams are for. But it
pins stream size:

| Extents/stream | Stream capacity | Extent list (64 B) | Streams in a full object | Stream list (32 B) |
|---|---|---|---|---|
| 250 | 1 TB | 16 KB | 1.8 × 10^9 | ~59 GB — too big |
| 2.5 × 10^5 | 1 PB | 16 MB | 1.8 × 10^6 | ~59 MB — fits |
| 2.5 × 10^6 | 10 PB | 160 MB | 1.8 × 10^5 | ~5.9 MB — comfortable |

So streams want to be **1 PB or larger**, which is what is expected anyway. At
that size each stream's extent list is 16–160 MB, held by that stream's
metadata replicas, and the object's stream list is a few MB to tens of MB —
holdable on one machine and cacheable at clients.

**Three levels are enough, but only because streams are enormous.** If streams
ever needed to be small — for placement flexibility, say — the object's stream
list would stop fitting on one machine and would need paging or striping of
its own: a fourth level in disguise. Stream sizing is load-bearing, not a
tuning knob.

---

## 12. Open questions

1. **Log count under update-heavy load** (§4.2). Lazy log creation fixes the
   append-mostly case; a 1 PB stream under heavy update can still reach
   ~250,000 open logs. Share logs across neighbouring extents past a
   threshold, or accept it?
2. **Logical vs physical extent size** (§5). What drives the gap, and are
   logical offsets dense?
3. **EC vs read locality** (§4.3). Align the fragment stripe to the index
   granularity, or keep a full replica for locality?
4. **Compaction policy** (§8): triggers, write-amplification budget, split
   throttling.
5. **Packed extents** (§9): deletion garbage, shared EC fate, the directory
   format, and the inline threshold.
6. **Metadata service topology** (§10): per-stream, per-object, or sharded
   global.
7. Is `count` on an object expected to be exact and cheap? Cumulative counts
   make it cheap and — because update and tombstoned delete preserve count —
   exact for unkeyed objects. For keyed objects it is a maintained statistic.
8. `Nul` and `\0` framing were both listed originally and are folded together
   here (§6.2). Confirm no distinction was meant.
9. Does a tombstoned unkeyed record number stay readable as "deleted" forever,
   or can compaction eventually reclaim the number? Reclaiming it would
   renumber, so presumably never — worth stating deliberately.
