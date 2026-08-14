# DeLawson — Design

Status: **draft, pre-implementation.** Nothing here is built. Sections marked
⚠️ are unresolved; they are collected again in §14.

Language: Rust.

---

## 1. Goals

- A single object may hold up to **2^64 records**.
- No single machine can hold such an object. Objects span many machines by
  construction, not as an optimization.
- **Tiny objects are equally expected.** Different users behave differently;
  the same system must hold a 30-record object and a 2^64-record object
  without the small case paying for the large one (§10).
- **No per-record index anywhere.** An index with 2^64 entries is not
  storable, so record location must be computed from aggregate counts (or key
  ranges) plus a bounded scan.
- **Compression and encryption are automatic**, not opt-in (§6).
- Durability by replication while writing, by erasure coding once immutable.
- Sequential read throughput approaching the aggregate sequential bandwidth of
  the disks holding the object.
- **Billing is by physical bytes stored** (§11), which makes the compressed,
  replicated footprint a first-class, customer-visible quantity rather than an
  implementation detail.

## 2. Non-goals

- Not a SQL database. No query language, no joins, no planner.
- No cross-object transactions.
- No general secondary indexes.
- No deduplication — per-extent encryption keys make cross-extent identity
  invisible, and billing by physical bytes removes the incentive.

---

## 3. Two object flavors

Fixed at creation; determines how records are addressed.

| | **Unkeyed** | **Keyed** |
|---|---|---|
| Address | 64-bit record number | sorted user key |
| Insert | **only at the end of the object** | anywhere, by key |
| Update | yes, count-preserving | yes |
| Delete | tombstone, count-preserving | tombstone |
| Positional access | yes, `O(log)` | no (§8.4) |

The asymmetry follows from one fact: **record counts are stable under update
and tombstoned delete, but not under insert.** An update replaces a record and
a tombstone leaves it present-but-dead, so every cumulative count in the
object is unchanged. An insert in the middle renumbers everything after it,
invalidating every cumulative count above the insertion point.

Therefore:

- **Unkeyed objects forbid mid-object insert.** Appends land at the end, where
  they touch no existing cumulative count. Record numbers are permanently
  stable and the count-based addressing of §8.1 works.
- **Keyed objects abandon counts as addresses.** Insert-in-the-middle is the
  point of them, so counts are unusably unstable and keys are the address.
  Counts are still maintained, but only as statistics — for `count`, for load
  balancing, for placement — never for lookup.

---

## 4. The four levels

```
Object
  ├─ metadata (flavor, framing, device class, totals; inline records if tiny — §10)
  └─ streams[]     unkeyed: (stream_id, cumulative records, cumulative logical + physical bytes)
       │           keyed:   (stream_id, first_key, cumulative bytes)
       Stream                        unit of capacity striping
         └─ extents[]  unkeyed: (extent_id, cumulative records, cumulative logical + physical bytes)
              │        keyed:   (extent_id, first_key, cumulative bytes)
              Extent                 unit of replication, sealing, compression, encryption, EC
                ├─ base              sealed, immutable
                ├─ log               unsealed, lazily created — updates, tombstones, keyed inserts
                └─ instances[]        replicas, or EC fragments after sealing
                     │
                     Instance        one contiguous block of bytes on one disk
```

Each level answers one question by binary search over an ordered array — of
cumulative counts for unkeyed objects, of first keys for keyed ones:

| Level | Question | Key |
|---|---|---|
| Object | which stream? | cumulative records / first key |
| Stream | which extent? | cumulative records / first key |
| Extent | roughly where inside? | sparse index (§8.3) |
| Instance | which disks hold those bytes? | replica / fragment list |

Both logical and physical cumulative byte totals are carried, because logical
drives addressing and physical drives billing (§11) and placement.

### 4.1 Streams

A stream exists **purely for capacity striping** — no semantic distinction, no
"index stream" or "log stream". An object small enough for one stream uses one
stream; streams are added as it grows.

A stream is the granularity at which an object spreads across the cluster and
at which extent metadata is partitioned. **A stream's ceiling is set by keeping
its whole extent list resident in one machine's RAM**: at ~100 B per extent
entry, 256 GB of RAM holds ~2^31 extents, and 2^31 extents of 2^25 bytes each
is **2^56 bytes = 64 PiB per stream**. See §13.

### 4.2 Extents: base plus log

An extent is the unit of replication and immutability. Logically a **pair**:

- a **base**: sealed, immutable, in key order (keyed) or record order (unkeyed);
- a **log**: unsealed and appendable, holding updates, tombstones, and — for
  keyed extents — inserts falling in this extent's key range.

Reads merge base and log, newest version winning. **Compaction** folds the log
into a fresh base and drops the old pair (§9).

**The log is created lazily.** An extent only ever appended to has no log and
costs nothing for one. This matters more than it looks: a full stream holds
~2^31 extents (§13), and if each eagerly carried an open,
fully-replicated log, the cluster's open-extent count — the expensive,
un-erasure-coded, in-memory-state-bearing kind — would dwarf its sealed extent
count. For append-mostly objects almost no logs exist.

⚠️ Even lazily, an update-heavy full stream could want ~2^31 open logs — two
billion replicated, un-erasure-coded extents in one stream. Lazy creation makes
the append-only case free but does nothing for the update-heavy case, so open
logs per stream need a hard bound, with forced compaction when it is hit.
Unlike the extent list, open logs cost replicas and network, not just RAM, so
their bound is far below 2^31.

### 4.3 Instances

- While open, each instance is a **full replica** on a distinct machine and
  failure domain. Writes go to all replicas. Default 4 instances.
- After sealing, the extent is re-encoded to **erasure-coded fragments**,
  reconstructible from any *k* of *n*. Replicas drop once encoding is durable
  and verified.

Because EC operates on the already-compressed, already-encrypted bytes (§6),
re-encoding needs no key access and no recompression.

Sealing and EC are also a **billing event**: 4 replicas of a 32 MB body is
128 MB billed; a 6+3 encoding of the same body is ~48 MB. See §11.

⚠️ EC changes the read unit: what was one sequential read against a replica
becomes *k* fragment reads across *k* machines, partly defeating the device
tuning of §8.3. Either align the fragment stripe with the index granularity, or
retain one full replica alongside the fragments for locality.

---

## 5. Size limits and sealing

An extent seals when it hits **either** limit, whichever comes first:

| Limit | Value | Why |
|---|---|---|
| Logical | **4 GB** | An intra-extent logical offset fits in 32 bits. |
| Physical | **32 MB compressed** (initial guess) | Bounds compaction cost, repair granularity, and open-extent memory. |

Also bounding the physical limit: it must stay **above ~8 MB** so that an HDD
seek is small relative to scanning the extent. A seek plus rotation is ~10 ms;
32 MB at ~150 MB/s is ~213 ms, so the seek is under 5% overhead. At 8 MB it is
~19%. Below that the drive spends more time finding extents than reading them.

**Consequence: extent size is data-dependent.** Which limit binds depends
entirely on compressibility:

| Data | Compression ratio | Binds first | Logical span | Physical |
|---|---|---|---|---|
| Incompressible (already-compressed media) | ~1× | physical | ~32 MB | 32 MB |
| Typical text / structured records | ~3–4× | physical | ~100–130 MB | 32 MB |
| Highly redundant (logs, sparse columns) | ~30× | physical | ~1 GB | 32 MB |
| Pathologically redundant | ≥128× | **logical** | 4 GB | ≤32 MB |

So in practice **the physical limit binds almost always**, and the 4 GB logical
cap is reached only past a 128:1 ratio. The logical cap is best understood not
as a normal sealing trigger but as the guarantee that keeps intra-extent
logical offsets 32 bits wide.

This also means a single object can hold wildly heterogeneous extents — a 32 MB
span next to a 1 GB span — so nothing may assume uniform extent size. All
navigation is by cumulative totals (§4), which is indifferent to this.

---

## 6. The storage pipeline

Every extent body goes through the same pipeline. Order matters and is forced:

```
records → framing → compression → encryption → replication → (seal) → erasure coding
```

- **Compress before encrypt.** Ciphertext is incompressible, so the reverse
  order would give no compression at all — and compression ratio is what the
  customer is billed on (§11).
- **Encrypt before erasure-code.** EC is linear over whatever bytes it is
  given, so encoding the ciphertext lets repair and re-encoding proceed with no
  access to keys — a background repair job never needs to decrypt.

### 6.1 Block alignment

Whole-extent compression and whole-extent encryption would both destroy random
access: you cannot decompress or decrypt from the middle. So both operate on
**blocks**, and the block boundary is the same boundary the sparse index points
at (§8.3). One granule is one compression unit and one encryption unit:

- read the 32 MB physical extent's index, find the granule containing the target,
- read exactly that granule, decrypt it, decompress it, scan it by framing.

This makes granule size a three-way decision — index density, compression
ratio (larger blocks compress better), and read amplification (larger blocks
waste more per point read) — rather than the purely device-driven choice §8.3
describes.

⚠️ The device-tuned granularities of §8.3 (1 MB HDD, 20 KB SSD) were chosen for
seek amortization alone. 20 KB is small enough to hurt compression ratio
noticeably, and ratio is billable. The SSD granularity may need to rise, or
compression blocks may need to span several index granules with the index
recording which block a granule lives in.

### 6.2 Encryption details

- Block-granular mode with a per-block IV/tweak derived from
  `(extent_id, block_index)` so blocks are independently decryptable and
  identical plaintext in two places does not produce identical ciphertext.
- Keys are per-extent. Sealed extents are immutable, so their keys never need
  rotation in place; **rotation happens by rewriting during compaction**, which
  is a rewrite anyway.
- ⚠️ Key custody, the key hierarchy, and whether customers may supply their own
  keys are entirely undesigned.

---

## 7. Writing

### 7.1 Append blocks

The unit of write is an **append block** of 0..n records, applied to the target
extent atomically: all its records become visible or none do.

An append returns the record number of the first record in the block (unkeyed)
and the extent it landed in.

### 7.2 Record framing

Fixed at object creation:

| Framing | Boundary |
|---|---|
| `LengthPrefixed` | varint or fixed-width length precedes each record |
| `Nul` | records terminated by `0x00` |
| `Lf` | records terminated by `\n` |
| `CrLf` | records terminated by `\r\n` |

`LengthPrefixed` is the only framing permitting arbitrary binary content; the
terminator framings require the terminator not appear inside a record, and
exist so existing line- and NUL-delimited data can be stored and scanned
without a rewrite.

### 7.3 The three mutations

| | Unkeyed | Keyed |
|---|---|---|
| **Insert** | appended to the object's tail extent; allocates the next record number | written to the log of the extent whose key range covers the key |
| **Update** | `(local record number, new bytes)` appended to that extent's log | `(key, new bytes)` appended to the log |
| **Delete** | tombstone for the local record number, appended to the log | tombstone for the key, appended to the log |

Update and delete never touch the base, never change any cumulative record
count, and never reopen a sealed extent. Insert is the only operation that
moves counts, which is why unkeyed inserts are confined to the object's end.

---

## 8. Reading

### 8.1 Unkeyed: locating record *N*

1. Binary search the object's stream list on cumulative records → stream *S*
   and a stream-local record number.
2. Binary search *S*'s extent list on cumulative records → extent *E* and an
   extent-local record number.
3. If *E* has a log, check it for a newer version or tombstone. If found, done.
4. Otherwise binary search *E*'s sparse index → nearest preceding checkpoint.
5. Read that granule from any instance, decrypt, decompress, parse forward by
   framing, stop at the target.

Two binary searches, one small log probe, one index search, one granule read.
Logarithmic in stream and extent count; the scan is bounded by granule size,
never by object size.

### 8.2 Keyed: locating key *K*

Identical in shape, with `first_key` comparisons replacing cumulative counts
and the sparse index mapping `key → offset`. The log probe is not optional
here: keyed logs hold inserts, so a key absent from the base may still exist.

### 8.3 The sparse index

One checkpoint approximately every **1 MB** for HDD-backed extents and **20 KB**
for SSD-backed extents, tuned to the device's efficient read size: on an HDD the
seek dominates and there is no point indexing finer than what you would read
anyway; on an SSD there is no seek to amortize, so finer granularity pays.

**Checkpoints must record physical offsets**, i.e. positions in the compressed,
encrypted body — that is what a reader actually seeks to. A checkpoint is
therefore `(local record number or key, physical offset)`, and optionally the
logical offset too if callers need logical byte positions.

Cost, at a 32 MB physical extent and 8-byte checkpoints:

| Device | Granularity | Checkpoints | Index size | Overhead |
|---|---|---|---|---|
| HDD | 1 MB | 32 | 256 B | 0.0008% |
| SSD | 20 KB | ~1,600 | ~13 KB | 0.04% |

Note how small these became once extents shrank from 4 GB to 32 MB physical.
That is not free — see §13, where the same information reappears, multiplied,
in the stream's extent list.

Device class is a property of the extent, so an object may migrate between
tiers extent by extent.

### 8.4 Scans

A full scan ignores the index: walk the stream list in order, the extent list
in order, read each extent at full device bandwidth, merging its log. Because
streams sit on disjoint machine sets, scans of a large object parallelize
across the cluster naturally.

Keyed objects support ordered and range scans by key but **not** positional
access — "the 10,000,000th record" is unanswerable, because counts are
statistics rather than addresses (§3).

---

## 9. Compaction

Compaction reads a base plus its log and writes a fresh base with updates
applied, tombstones dropped, and (keyed) inserts merged in key order, then
atomically swaps the pair in the stream's extent list.

- **Extent membership of a stream changes over time.** Extent *position* within
  a stream is therefore unstable; extent *identity* must be a stable unique id
  that outlives position.
- **Keyed compaction can split.** If the merged result exceeds either limit of
  §5 it becomes two extents covering two key subranges, spliced into the extent
  list. This makes keyed objects B-tree-shaped — one log level per base,
  splitting on overflow — rather than LSM-shaped with many levels.
- **Unkeyed compaction never splits or renumbers.** Update and tombstone
  preserve count, so the new base covers exactly the record-number range the
  old one did and cumulative counts above it are untouched. Purely local.
- Compaction is a rewrite, so it is also where EC re-encoding, device-class
  changes, recompression, and **encryption key rotation** happen.
- Compaction is a **billing event**: it is what reclaims the physical bytes
  held by superseded versions and tombstones (§11).

⚠️ Compaction policy — triggers, write-amplification budget, split throttling —
is not designed. Billing makes it sharper than usual: the schedule directly
determines a customer's bill.

---

## 10. Small objects

Three regimes by size:

| Regime | When | Storage |
|---|---|---|
| **Inline** | very small | records live inside the object's metadata record — no stream, no extent, no instance |
| **Packed** | smaller than half a max-sized extent | shares one extent with other objects; the extent carries a directory of the objects in it |
| **Dedicated** | above that | its own extents, as §4 |

The half-extent threshold guarantees a packed object never straddles an extent
boundary: at most half of a maximum extent, at least two objects fit and any
packable object fits whole.

⚠️ "Half a max-sized extent" is now ambiguous, because §5 gives two maxima.
Half of 4 GB logical and half of 32 MB physical are very different thresholds,
and since the physical limit almost always binds, the packing threshold is
presumably ~16 MB physical. Needs confirming.

⚠️ Packing collides with billing and compression in a way the uniform design
avoids. A packed extent is compressed as a unit, so per-object physical size is
**not additive** — two objects that share substrings compress better together
than apart, and there is no principled way to attribute the saving. Either
compress each packed object independently (losing ratio, and ratio is billable)
or attribute physical bytes proportionally to logical size (approximate, and
customers can notice). Deleting one packed object also leaves garbage until the
shared extent is compacted, and packed objects share an EC fate.

---

## 11. Billing and metering

**Customers are billed by physical bytes stored, summed over instances.** So a
32 MB compressed body held as 4 replicas bills as 128 MB; the same body as a
6+3 erasure code bills as ~48 MB.

This makes several internal choices customer-visible:

- **Compression works for the customer, not just the fleet.** Ratio directly
  divides the bill, which is why compression must precede encryption (§6) and
  why shrinking compression blocks for index density is not a free trade (§6.1).
- **Sealing and EC cut the bill ~2.7×** (4 replicas → 6+3). So the latency
  between sealing and successful encoding is money. That needs an explicit
  target, and a decision about who absorbs the cost of the gap.
- **Compaction reclaims billable bytes.** Superseded versions and tombstones
  occupy physical space until compacted, so the compaction schedule sets part
  of the bill.
- **Metering must be per-object, over time** — physical bytes × instances ×
  duration — which means an accounting subsystem that survives failures and
  double-counts nothing during replica repair, re-encoding, and compaction
  (where old and new bases briefly coexist).

⚠️ Open policy questions, all customer-facing:

1. Do customers pay for **uncompacted garbage** they created by overwriting,
   when DeLawson chooses the compaction schedule?
2. Do they pay for the **transient double footprint** during compaction and
   re-encoding?
3. Do they pay for **metadata**? A stream's extent list can reach hundreds of
   MB (§13) and is itself replicated.
4. How are **packed objects** attributed (§10)?
5. Is the billed figure **compressed size** (which the customer cannot directly
   control) reported back to them, and at what granularity?

---

## 12. Metadata, consistency, and membership

Thinner than the rest — least determined so far.

- **Durability** on write comes from replicating an append block to all open
  instances before acknowledging. Sealing is the commit point fixing content.
- **Metadata** (object → streams, stream → extents, extent → instances) is
  replicated state that must stay consistent under failure. §13.5 settles the
  topology: because a stream's extent list is RAM-resident on one machine,
  there is **one metadata server per stream**.
- **Replica failure on an open extent**: seal short and open a new one. This
  removes replica repair from the write path entirely. Sealing short is cheap
  precisely because §5 makes extent size variable anyway.
- **Repair** of sealed extents is background fragment reconstruction over
  ciphertext, never blocking a write, never needing keys.

⚠️ Open: how a stream's extent list survives losing its machine — rebuilt by
scanning extents, journalled, or consensus-replicated across a group.

---

## 13. Does the metadata fit? (sizing)

The check that decides whether three levels suffice. They do, comfortably —
and the binding constraint turns out to be the width of the record number, not
storage capacity at any level.

The governing rule: **a stream's extent list is RAM-resident on one machine.**
That is what bounds a stream, and everything else follows from it.

### 13.1 One stream

Assume the worst case for capacity, `compressed = physical` (ratio 1×), so an
extent's logical span equals its 2^25-byte (32 MB) physical size.

| Quantity | Value |
|---|---|
| Extent metadata entry | ~100 B — **a guess, to be measured** |
| Extents in 256 GB of RAM | ~2^31 (≈214 GB) |
| Extent logical span | 2^25 B |
| **Stream ceiling** | 2^31 × 2^25 = **2^56 B = 64 PiB** |

Compression only helps: at 4× the same 2^31 extents span 256 PiB logical.

**Sensitivity to the 100 B guess.** The stream ceiling scales inversely with
the entry size, so the number to measure moves the answer — but not the
conclusion:

| Entry size | Extents in 256 GB | Stream ceiling | Streams for 2^64 records |
|---|---|---|---|
| 50 B | 2^32 | 128 PiB | ~12,800 |
| 100 B | 2^31 | 64 PiB | ~25,600 |
| 200 B | 2^30 | 32 PiB | ~51,200 |
| 400 B | 2^29 | 16 PiB | ~102,400 |

Even at 400 B per entry the object's stream list is ~40 MB. The margin in §13.3
is large enough that measuring 100 B as 2× or 4× off changes nothing
structural — it only shifts how many streams a maximal object needs.

### 13.2 The three levels, at full scale

| Level | Entries | Entry | Total | Where it lives |
|---|---|---|---|---|
| Extent sparse index | ~32 (1 MB granule) | 8 B | ~256 B | with the extent |
| Stream's extent list | 2^31 | 100 B | ~214 GB | one machine's RAM |
| Object's stream list | ~25,000 | 100 B | ~2.5 MB | trivially anywhere |

The object's stream list is the striking number. To hold 2^64 records at 100
bytes each — 1.845 ZB — an object needs 1.845 ZB / 64 PiB ≈ **25,600 streams**,
a 2.5 MB list. Three orders of magnitude below anything difficult.

So the shape is lopsided by design: an enormous per-stream list on a dedicated
machine, and a tiny per-object list pointing at a few thousand of those
machines. "The object needs a list of huge streams" is exactly right.

### 13.3 What actually binds

Two independent ceilings:

- **Record number**: 2^64 records, from the 8-byte record number.
- **Capacity**: if the object's stream list is also allowed ~2^31 entries, then
  2^31 streams × 2^56 B = **2^87 bytes**.

These meet at an average record size of 2^87 / 2^64 = 2^23 = **8 MB**. Below 8 MB
average, the record number runs out first; above it, byte capacity does.

Since records are expected to average nearer 100 bytes than 8 MB, **the 8-byte
record number is the binding constraint by a factor of ~10^5** — the object
exhausts its record numbers long before any list approaches its maximum size.
The three-level structure is not close to strained; the address space is.

### 13.4 Correction to an earlier version of this section

An earlier draft argued that 32 MB extents and large streams were in tension,
concluding the physical extent limit had to rise toward 4 GB. That was wrong,
and wrong in an instructive way: it assumed a stream's extent list had to stay
in the tens of megabytes, sized for cheap consensus replication. The real
constraint is one machine's RAM, roughly 10^4 times larger, which dissolves the
problem. The 32 MB physical limit stands, and the tension does not exist.

What survives from that analysis is only the observation that shrinking extents
moves addressing information up from the extent's sparse index into the
stream's extent list. That trade is real; it just lands well inside budget.

### 13.5 Consequences

- **The metadata topology is decided by this section**, not chosen freely: one
  RAM-resident extent list per stream means **one metadata server (or
  replicated group) per stream**. §12's open question is answered.
- **Losing a stream's metadata machine is losing 64 PiB of addressing.** The
  extent list must be durable and recoverable independently of the RAM copy —
  reconstructible by scanning extents, or journalled, or both.
- ⚠️ **The ~100 B entry is tight, and may not fit keyed extents.** For an
  unkeyed extent the entry must hold an extent id, the starting record number,
  cumulative logical and physical bytes, flags, and the instance or fragment
  locations — roughly 4 × 16 B for four instances plus ~40 B of the rest, so
  ~100 B with little slack. A keyed extent additionally needs a `first_key`,
  which is variable-length and user-supplied. Either the entry grows (and the
  stream ceiling falls proportionally), or the list stores a bounded key prefix
  and resolves ambiguity by reading the extent.
- ⚠️ 2^31 extents per stream also means up to 2^31 potential logs (§4.2). RAM
  bounds the extent list; nothing yet bounds the open logs.

---

## 14. Open questions

Roughly in order of how much else depends on them.

1. **Bounding open logs per stream** (§4.2). RAM bounds the extent list at
   ~2^31; nothing yet bounds the logs, and each one costs replicas and network.
   The only unresolved scaling question left in the structure.
2. **Durability of a stream's extent list** (§12, §13.5) — losing that machine
   loses 64 PiB of addressing. Rebuild by scanning, journal, or replicate?
3. **The extent metadata entry: measure it** (§13.1). 100 B is a guess. Also
   whether a keyed extent's variable-length `first_key` fits, or forces a
   bounded prefix (§13.5).
4. **Compression block size vs index granularity** (§6.1). A 20 KB SSD granule
   hurts compression ratio, and ratio is billable.
5. **Keyed objects** generally — the split-on-overflow B-tree shape of §9 needs
   its own pass.
6. **The packing threshold** (§10) — half of *which* maximum? Presumably ~16 MB
   physical.
7. **Packed-object billing attribution** (§10, §11) — per-object physical size
   is not additive under shared-extent compression.
8. **Billing policy** (§11): garbage, transient double footprint, metadata,
   reporting granularity.
9. **Key custody and hierarchy** (§6.2); customer-supplied keys?
10. **EC vs read locality** (§4.3): align the fragment stripe to the granule, or
    keep a full replica?
11. **Compaction policy** (§9) — now partly a pricing decision.
12. Is `count` expected exact and cheap? Cumulative counts make it cheap, and
    because update and tombstoned delete preserve count, exact for unkeyed
    objects. For keyed objects it is a maintained statistic.
13. `Nul` and `\0` framing were both listed originally and are folded together
    (§7.2). Confirm no distinction was meant.
14. Does a tombstoned unkeyed record number stay readable as "deleted" forever?
    Reclaiming it would renumber, so presumably never — worth stating
    deliberately.

**Closed** since earlier drafts: the unkeyed overwrite/addressing contradiction
(§3); logical vs physical extent size, which is compression (§5); metadata
service topology, forced by §13.5; and the supposed 32 MB-vs-large-streams
tension, which was based on a wrong constraint (§13.4).
