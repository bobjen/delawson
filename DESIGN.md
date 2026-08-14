# DeLawson — Design

Status: **draft, pre-implementation.** Nothing here is built. Sections marked
⚠️ are unresolved tensions, collected again at the end.

Language: Rust.

---

## 1. Goals

- A single object may hold up to **2^64 records**.
- No single machine can hold such an object. Objects span many machines by
  construction, not as an optimization.
- **No per-record index anywhere.** An index with 2^64 entries is not
  storable, so record location must be computed from aggregate counts plus a
  bounded scan.
- Durability through replication while data is being written, through erasure
  coding once it is immutable.
- Sequential read throughput should approach the aggregate sequential
  bandwidth of the disks holding the object.

## 2. Non-goals

- Not a SQL database. No query language, no joins, no planner.
- No cross-object transactions.
- No general secondary indexes.

---

## 3. The four levels

```
Object
  ├─ metadata (flavor, framing, device class, record/byte totals)
  └─ streams[]        each entry: (stream_id, cumulative records before, cumulative bytes before)
       │
       Stream                      unit of capacity striping
         └─ extents[]   each entry: (extent_id, cumulative records before, cumulative bytes before)
              │
              Extent               unit of replication, sealing, and erasure coding
                └─ instances[]     replicas, or EC fragments after sealing
                     │
                     Instance      one contiguous block of bytes on one disk
```

Each level answers exactly one question, by binary search over a
cumulative-count array:

| Level | Question | Structure |
|---|---|---|
| Object | which stream holds record *N*? | cumulative records per stream |
| Stream | which extent holds it? | cumulative records per extent |
| Extent | roughly where in the extent? | sparse offset index |
| Instance | which disks have those bytes? | replica / fragment list |

### 3.1 Streams

A stream exists **purely for capacity striping**. It carries no semantic
distinction — there is no "index stream" or "log stream". An object small
enough to fit in one stream uses exactly one stream; streams are added as the
object grows past what one stream should hold.

A stream is the granularity at which an object is spread across the cluster,
and the granularity at which extent metadata is partitioned (see §7 sizing).

### 3.2 Extents

An extent is the unit of replication and the unit of immutability.

- At most one extent per stream is **open** for append at a time.
- When an open extent reaches its size limit it is **sealed**: its byte
  length, record count, and content are fixed forever.
- Sealing is what makes everything else cheap. A sealed extent's cumulative
  counts never change, its sparse index never changes, and its bytes can be
  erasure-coded, cached, and checksummed once.
- Only the tail extent of the tail stream has a volatile record count.

### 3.3 Instances

- While an extent is open, each instance is a **full replica** on a distinct
  machine (and distinct failure domain). Writes go to all replicas.
- After sealing, the extent is re-encoded to **erasure-coded fragments**;
  instances then become fragments, reconstructible from any *k* of *n*.
  Replicas are dropped once encoding is durable and verified.

This gives cheap durability for the overwhelming majority of data (sealed,
cold, EC) and simple synchronous replication for the small hot tail.

⚠️ EC changes the read unit: a read that was one 1 MB sequential disk read
against a replica becomes *k* fragment reads across *k* machines. §5.2
discusses why the sparse-index granularity is tuned to the device, and EC
partly defeats that tuning. Options: encode with a fragment stripe that is a
multiple of the index granularity, or keep one full replica alongside the
fragments for read locality.

---

## 4. Writing

### 4.1 Append blocks

The unit of write is an **append block**, containing 0..n records. A block is
appended to the open extent atomically: either all its records become visible
or none do. Block size is the client's choice within a limit.

An append returns the record number of the first record in the block (for
sequential objects) and the extent it landed in.

### 4.2 Record framing

Record boundaries within a block are found by the object's declared framing.
Framing is fixed at object creation:

| Framing | Boundary |
|---|---|
| `LengthPrefixed` | varint or fixed-width length precedes each record |
| `Nul` | records terminated by `0x00` |
| `Lf` | records terminated by `\n` |
| `CrLf` | records terminated by `\r\n` |

`LengthPrefixed` is the only framing that permits arbitrary binary record
content; the terminator framings require that the terminator not appear inside
a record, and exist so that existing line- and NUL-delimited data can be
stored and scanned without a rewrite.

⚠️ The original sketch listed both "null terminated" and "\0 terminated";
these appear to be the same thing and are folded into `Nul` here. If a
distinction was intended, it needs naming.

### 4.3 Overwrite

Extents are physically append-only, but records are **logically
overwritable**: an overwrite appends a new version of the record, and the read
path resolves to the newest version. Compaction later reclaims superseded
versions by rewriting live records into fresh extents and re-sealing.

⚠️ **This is the sharpest unresolved tension in the design.** For sequential
objects, record location is derived from cumulative record *counts*. If
overwriting record *N* appends a new version at the tail:

- the cumulative counts now describe *positions in the append order*, not
  record numbers, so binary search finds the original version, not the newest;
- appending a version increments counts, so record numbers and positions
  diverge permanently.

The scheme only stays coherent if one of these holds:

1. **Overwrite is in-place-shaped**: only same-length overwrite is allowed, and
   it is applied to the extent by a replicated patch rather than an append.
   Keeps addressing pure; loses append-only immutability of sealed extents.
2. **Overwrite is keyed-only**: sequential objects are strictly append-only
   with no overwrite, and overwrite exists solely for keyed objects, where
   newest-wins resolution is natural. Keeps both properties; restricts the API.
3. **A redirect layer**: overwritten records get an entry in a small
   exceptions table consulted on read, folded away at compaction. Costs a
   second lookup, and the table is unbounded between compactions.

(2) is the cleanest and is the assumption the rest of this document makes;
it needs confirming.

---

## 5. Reading

### 5.1 Sequential objects — locating record *N*

1. Binary search the object's stream list on cumulative records → stream *S*,
   and a stream-local record number.
2. Binary search *S*'s extent list on cumulative records → extent *E*, and an
   extent-local record number.
3. Binary search *E*'s sparse index → the nearest preceding checkpoint
   `(record_number, byte_offset)`.
4. Read one device granule starting at `byte_offset` from any instance, parse
   forward by framing, and stop at the target record.

Three binary searches and one read. Costs are logarithmic in stream count and
extent count, and the forward scan is bounded by the index granularity — never
by the size of the object.

### 5.2 Sparse index granularity

The extent's index holds one checkpoint approximately every:

- **1 MB** for HDD-backed extents
- **20 KB** for SSD-backed extents

The granularity is set to the device's efficient read size: on an HDD the seek
dominates so a 1 MB read is nearly free relative to finding it, and there is
no reason to index more finely than the amount you would read anyway. On an
SSD there is no seek to amortize, so a much finer granularity pays for itself.

Index cost, for a 1 GiB extent and 16-byte checkpoints:

| Device | Granularity | Checkpoints | Index size | Overhead |
|---|---|---|---|---|
| HDD | 1 MB | ~1,024 | ~16 KB | 0.0015% |
| SSD | 20 KB | ~52,400 | ~840 KB | 0.08% |

Both are small enough to hold in memory for open extents and to cache
aggressively for sealed ones.

Device class is a property of the extent, not the object, so an object may
migrate between tiers extent by extent.

### 5.3 Sequential scans

A full scan does not use the index at all: walk the stream list in order, the
extent list in order, and read each extent's instances at full device
bandwidth. Because streams sit on disjoint machine sets, scans of a large
object parallelize across the cluster naturally.

### 5.4 Keyed objects

Keyed objects are ordered by sorted key and permit insertion in the middle.
This does not fit the cumulative-count scheme cleanly:

- An insertion in the middle shifts the record numbers of everything after it,
  so cumulative *record counts* cannot serve as a stable address space.
- Lookup must be by key, so each level needs a **key range** per child
  (`first_key`) in addition to, or instead of, cumulative counts.

The natural resolution: keyed objects keep the same four levels and the same
sealed-extent immutability, but each level's child list is keyed by
`first_key` rather than cumulative record count, and the extent's sparse index
maps `key → byte_offset` rather than `record_number → byte_offset`.
Insertion in the middle then means writing a new extent covering a subrange
and splicing it into the stream's extent list — an LSM/B-tree-shaped problem.
Cumulative record counts are still maintained, but only as statistics
(for `count`, for load balancing), not as addresses.

⚠️ This needs its own design pass. In particular: whether keyed objects merge
(LSM levels) or split (B-tree), and what the write amplification budget is.

---

## 6. Metadata, consistency, and membership

Deliberately sketchy — this is the part least determined by the discussion so
far.

- **Extent-level durability** comes from replicating append blocks to all open
  instances before acknowledging. Sealing is the commit point that makes an
  extent's content agreed.
- **Metadata** (object → streams, stream → extents, extent → instances) is
  itself replicated state that must be consistent under failure. This wants a
  small consensus-replicated metadata service, per-object or per-stream, not a
  single global one.
- **Failure of an open extent's replica**: seal the extent short (extents need
  not be full to be sealed) and open a new one. This is the standard trick and
  it removes replica repair from the write path entirely.
- **Repair** of sealed extents is fragment reconstruction, done in the
  background, never blocking a write.

⚠️ Open: is there one metadata service per stream, per object, or a sharded
global one? The answer is constrained by §7.

---

## 7. Does the metadata actually fit? (sizing)

This is the check that decides whether the three-level scheme is sufficient or
whether a fourth index level is needed.

Take the stated ceiling: 2^64 ≈ 1.845 × 10^19 records. At an average 100 bytes
per record that is ~1.8 ZB — physically unreachable, but the addressing must
still work, so use it as the bound.

With 1 GiB extents, an extent holds ~10^7 records, so a full object implies
~1.7 × 10^12 extents. Extent metadata at ~64 bytes per entry is ~110 TB in
total. **No single machine holds the extent list.** That is fine — it is
partitioned by stream, which is exactly what streams are for. But it pins down
how big a stream should be:

| Extents per stream | Stream capacity | Streams in a full object | Object's stream list |
|---|---|---|---|
| 10^3 | 1 TB | 1.7 × 10^9 | ~55 GB — too big |
| 10^5 | 100 TB | 1.7 × 10^7 | ~550 MB — fits, cacheable |
| 10^6 | 1 PB | 1.7 × 10^6 | ~55 MB — comfortable |

So a stream should be sized around **10^5 to 10^6 extents (100 TB – 1 PB)**.
At that size:

- each stream's extent list is 6 – 64 MB, held by that stream's metadata
  replicas;
- the object's stream list is tens to hundreds of MB — large, but holdable on
  one machine and cacheable at clients.

Conclusion: **three levels are enough, but only because streams are large.**
If streams turn out to want to be small (for placement flexibility, say), the
object's stream list becomes too large for one machine and needs to be paged
or striped itself — a fourth level in disguise. Stream sizing is therefore a
load-bearing decision, not a tuning knob.

---

## 8. Open questions

1. **Overwrite vs. cumulative-count addressing** (§4.3). The document assumes
   overwrite is keyed-objects-only and sequential objects are strictly
   append-only. Confirm or pick another resolution.
2. **Keyed objects** (§5.4) need their own pass: keyed-by-`first_key` child
   lists, and LSM-merge vs. B-tree-split.
3. **EC vs. read locality** (§3.3). Align fragment stripe with index
   granularity, or keep a replica for locality?
4. **`Nul` vs `\0` framing** (§4.2) — same thing, or was a distinction meant?
5. **Metadata service topology** (§6) — per-stream, per-object, or sharded
   global.
6. **Stream sizing** (§7) — the table argues for 100 TB – 1 PB per stream.
   Does that conflict with placement or repair goals?
7. Extent size: 1 GiB assumed throughout. Fixed, or per-object?
8. Is `count` on an object expected to be exact and cheap? Cumulative counts
   make it cheap; logical overwrite makes it approximate.
