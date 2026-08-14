# DeLawson — Design

Status: **draft, pre-implementation.** Nothing here is built. Sections marked
⚠️ are unresolved; they are collected again in §15.

Language: Rust.

---

## 1. Goals

- A single object may hold up to **2^64 records**.
- No single machine can hold such an object. Objects span many machines by
  construction, not as an optimization.
- **Tiny objects are equally expected**, in enormous numbers. Different users
  behave differently; the same system must hold a 30-record object and a
  2^64-record object without the small case paying for the large one (§11).
- **No per-record index anywhere.** An index with 2^64 entries is not
  storable, so record location must be computed from aggregate counts (or key
  ranges) plus a bounded scan.
- **Compression and encryption are automatic**, not opt-in (§7).
- **Billing is by physical bytes stored** (§12), which makes the compressed,
  replicated footprint a customer-visible quantity, not an implementation
  detail.
- Sequential read throughput approaching the aggregate sequential bandwidth of
  the disks holding the object.

### 1.1 Availability and reliability are separate goals

Extents must be both:

- **Available** — readable *now*. Threatened by transient loss: a machine
  rebooting, a rack losing power, a network partition, an overloaded disk.
- **Reliable** — not lost *over time*. Threatened by permanent, correlated
  loss: several drives from one bad batch, one firmware bug, one datacenter.

They are not the same property and they do not have the same remedy.
Availability wants enough live instances reachable right now, and a read path
that will accept any of them. Reliability wants instances whose permanent-loss
events are independent, plus background repair and scrubbing to catch decay
before it accumulates.

Both push the same way on one point, and it is the origin of the node types in
§5: **instances of an extent must sit in places that fail independently, and
the metadata describing an extent must not sit with the extent's own data.** A
failure that destroys the bytes must not also destroy the map to them.

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
| Positional access | yes, `O(log)` | no (§9.4) |

The asymmetry follows from one fact: **record counts are stable under update
and tombstoned delete, but not under insert.** An update replaces a record and
a tombstone leaves it present-but-dead, so every cumulative count in the object
is unchanged. An insert in the middle renumbers everything after it,
invalidating every cumulative count above the insertion point.

Therefore:

- **Unkeyed objects forbid mid-object insert.** Appends land at the end, where
  they touch no existing cumulative count. Record numbers are permanently
  stable and the count-based addressing of §9.1 works.
- **Keyed objects abandon counts as addresses.** Insert-in-the-middle is the
  point of them, so counts are unusably unstable and keys are the address.
  Counts are still maintained, but only as statistics — for `count`, for load
  balancing, for placement — never for lookup.

---

## 4. The logical decomposition: four levels

```
Object
  ├─ metadata (flavor, framing, device class, totals; inline records if tiny — §11)
  └─ streams[]     unkeyed: (stream_id, starting record number, cumulative logical + physical bytes)
       │           keyed:   (stream_id, first_key, cumulative bytes)
       Stream                        unit of capacity striping
         └─ extents[]  unkeyed: (extent_id, starting record number, cumulative bytes)
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
| Object | which stream? | starting record number / first key |
| Stream | which extent? | starting record number / first key |
| Extent | roughly where inside? | sparse index (§9.3) |
| Instance | which disks hold those bytes? | replica / fragment list |

Both logical and physical cumulative byte totals are carried: logical drives
addressing, physical drives billing (§12) and placement.

### 4.1 Streams

A stream exists **purely for capacity striping** — no semantic distinction, no
"index stream" or "log stream". An object small enough for one stream uses one
stream; streams are added as it grows.

A stream is the unit of assignment to a Stream Node (§5.2), and its ceiling is
what one SN can hold — see §14.

### 4.2 Extents: base plus log

An extent is the unit of replication and immutability. Logically a **pair**:

- a **base**: sealed, immutable, in key order (keyed) or record order (unkeyed);
- a **log**: unsealed and appendable, holding updates, tombstones, and — for
  keyed extents — inserts falling in this extent's key range.

Reads merge base and log, newest version winning. **Compaction** folds the log
into a fresh base and drops the old pair (§10).

**The log is created lazily.** An extent only ever appended to has no log and
costs nothing for one. This matters: a full stream holds ~2^31 extents (§14),
and if each eagerly carried an open, fully-replicated log, the cluster's
open-extent count — the expensive, un-erasure-coded, in-memory-state-bearing
kind — would dwarf its sealed extent count. For append-mostly objects almost no
logs exist.

⚠️ Even lazily, an update-heavy full stream could want ~2^31 open logs. Lazy
creation makes the append-only case free and does nothing for the update-heavy
case, so open logs per stream need a hard bound with forced compaction when it
is hit. Open logs cost EN replicas and network, not just SN memory, so their
bound is far below 2^31.

### 4.3 Instances

- While open, each instance is a **full replica** on a distinct EN in a distinct
  failure domain. Writes go to all replicas. Default 4 instances.
- After sealing, the extent is re-encoded to **erasure-coded fragments**,
  reconstructible from any *k* of *n*. Replicas drop once encoding is durable
  and verified.

Because EC operates on already-compressed, already-encrypted bytes (§7),
re-encoding needs no key access and no recompression.

Sealing and EC are also a **billing event**: 4 replicas of a 32 MB body bills as
128 MB; a 6+3 encoding of the same body bills as ~48 MB (§12).

⚠️ EC changes the read unit: what was one sequential read against a replica
becomes *k* fragment reads across *k* ENs, partly defeating the device tuning of
§9.3. Either align the fragment stripe with the index granularity, or retain one
full replica alongside the fragments for locality.

---

## 5. The physical decomposition: three node types

The logical levels of §4 do not all live in the same place. Splitting them is a
reliability requirement, not an optimization (§1.1).

| Node | Holds | Why separate |
|---|---|---|
| **DN** — Directory Node | The directory hierarchy and user object metadata, including each object's stream list, and inline records for tiny objects | User metadata can exceed the data itself; it is a distinct workload with a distinct size |
| **SN** — Stream Node | Per stream: the extent list. Per extent: the instance list | Must not sit with the bytes it maps |
| **EN** — Extent Node | Extent instances: contiguous blocks of bytes on disk | The bytes themselves; the only tier that scales with raw capacity |

```
client ──▶ DN            resolve path → object metadata → which stream?
       ──▶ SN            which extent? → which instances?
       ──▶ EN            the bytes
```

### 5.1 DNs — Directory Nodes

As in GFS, user objects live in a **directory hierarchy**, and DNs track the
directories as well as the objects.

The critical sizing fact: **user metadata cannot be RAM-resident by default.**
Because enormous numbers of tiny objects are expected, and every object carries
identity, path, times, flavor, framing, counts, byte totals, an ACL reference,
and a stream list — well over a hundred bytes before any records — **the
metadata can be larger than the data it describes.** For a 30-byte object it is
larger by several times.

So DN state is **disk-backed, loaded and cached in RAM on demand**. This is the
opposite of the SN arrangement below, and the difference is not incidental: SN
state is bounded per stream by construction, while DN state grows with the
object *count*, which nothing bounds.

⚠️ DN state is a large, sorted, key-addressed collection that must be paged from
disk and cached — which is exactly the keyed object of §3. Whether DeLawson
stores its own directory and object metadata in DeLawson keyed objects (with
some small bootstrap root outside), or in a separate purpose-built store, is
open and consequential. Self-hosting is attractive and is roughly what Colossus
does with BigTable; it also creates a bootstrap ordering problem and a risk of
circular failure.

⚠️ The directory hierarchy brings semantics not yet designed: path resolution,
rename (cheap pointer move, or expensive?), listing a directory with a billion
entries, permissions, and quotas. Billing (§12) also attaches naturally to
directories and accounts rather than to bare objects.

### 5.2 SNs — Stream Nodes

An SN holds, for each stream assigned to it, the **extent list**, and for each
of those extents, the **instance list** — where the copies are.

These two are on the same node type deliberately: there is no known use case
that demands the extent-list-per-stream and the instance-list-per-extent live
on different machines, and keeping them together makes a lookup one hop instead
of two.

What must *not* be co-located is either of them with the instances themselves.
That separation is the reliability requirement of §1.1: an EN failure destroys
bytes, and the map to the surviving copies has to outlive it.

A stream's ceiling is what one SN can hold (§14). A maximal object therefore
spreads its streams over many SNs — necessarily, since no single machine can
hold them all.

⚠️ **Losing an SN makes its streams unaddressable even though every EN is
healthy.** SN state must therefore be durable independently of the serving copy.
The natural mechanism: make every extent instance **self-describing** — each
carries a header with its extent id, stream id, starting record number, and
byte totals — so an SN's extent list is reconstructible by scanning ENs. That
turns SN state into a cache of a truth that lives redundantly on the ENs, at
the cost of a slow rebuild. Whether that is sufficient, or whether SN state also
needs a journal or a consensus group, is open.

### 5.3 ENs — Extent Nodes

ENs store extent instances and nothing else: a contiguous block of compressed,
encrypted bytes per instance. They are the only tier that scales with raw
capacity, and the only tier that erasure coding and repair touch.

Because ENs hold ciphertext (§7), repair, re-encoding, and scrubbing all run
without key access.

### 5.4 Two independent spreading requirements

At different levels, for different reasons:

| Requirement | Level | Reason |
|---|---|---|
| Instances of one extent on ENs in distinct failure domains | extent → instances | Reliability and availability (§1.1) |
| Streams of one object on distinct SNs | object → streams | Capacity: no machine holds them all |

The first is about correlated loss; the second is about arithmetic. Conflating
them would be a mistake — a placement policy good for one is not automatically
good for the other.

---

## 6. Size limits and sealing

An extent seals when it hits **either** limit, whichever comes first:

| Limit | Value | Why |
|---|---|---|
| Logical | **4 GB** | An intra-extent logical offset fits in 32 bits. |
| Physical | **32 MB compressed** (initial guess) | Bounds compaction cost, repair granularity, and open-extent memory. |

Also bounding the physical limit from below: it must stay **above ~8 MB** so an
HDD seek is small relative to scanning the extent. A seek plus rotation is
~10 ms; 32 MB at ~150 MB/s is ~213 ms, so the seek is under 5% overhead. At
8 MB it is ~19%.

**Consequence: extent size is data-dependent.** Which limit binds depends
entirely on compressibility:

| Data | Ratio | Binds first | Logical span | Physical |
|---|---|---|---|---|
| Incompressible (already-compressed media) | ~1× | physical | ~32 MB | 32 MB |
| Typical text / structured records | ~3–4× | physical | ~100–130 MB | 32 MB |
| Highly redundant (logs, sparse columns) | ~30× | physical | ~1 GB | 32 MB |
| Pathologically redundant | ≥128× | **logical** | 4 GB | ≤32 MB |

So **the physical limit binds almost always**, and the 4 GB logical cap is
reached only past a 128:1 ratio. The logical cap is best understood not as a
normal sealing trigger but as the guarantee that keeps intra-extent logical
offsets 32 bits wide.

A single object can therefore hold wildly heterogeneous extents — a 32 MB span
beside a 1 GB span — so nothing may assume uniform extent size. All navigation
is by cumulative totals (§4), which is indifferent to this.

---

## 7. The storage pipeline

Every extent body goes through the same pipeline, and the order is forced:

```
records → framing → compression → encryption → replication → (seal) → erasure coding
```

- **Compress before encrypt.** Ciphertext is incompressible, so the reverse
  order gives no compression at all — and ratio is what the customer is billed
  on (§12).
- **Encrypt before erasure-code.** EC is linear over whatever bytes it is given,
  so encoding ciphertext lets repair and re-encoding proceed on ENs with no
  access to keys.

### 7.1 Block alignment

Whole-extent compression and whole-extent encryption would both destroy random
access: you cannot decompress or decrypt from the middle. So both operate on
**blocks**, and the block boundary is what the sparse index points at (§9.3).
One granule is one compression unit and one encryption unit:

- read the extent's index, find the granule containing the target,
- read exactly that granule from an EN, decrypt it, decompress it, scan it by
  framing.

Granule size is therefore a three-way decision — index density, compression
ratio (larger blocks compress better), and read amplification (larger blocks
waste more per point read) — not the purely device-driven choice §9.3 describes.

⚠️ The device-tuned granularities of §9.3 (1 MB HDD, 20 KB SSD) were chosen for
seek amortization alone. 20 KB is small enough to hurt compression ratio
noticeably, and ratio is billable. The SSD granularity may need to rise, or
compression blocks may need to span several index granules.

### 7.2 Encryption details

- Block-granular mode with a per-block IV/tweak derived from
  `(extent_id, block_index)`, so blocks are independently decryptable and
  identical plaintext in two places does not yield identical ciphertext.
- Keys are per-extent. Sealed extents are immutable, so keys never need rotation
  in place; **rotation happens by rewriting during compaction** (§10), which is
  a rewrite anyway.
- ⚠️ Key custody, the key hierarchy, and whether customers may supply their own
  keys are undesigned.

---

## 8. Writing

### 8.1 Append blocks

The unit of write is an **append block** of 0..n records, applied to the target
extent atomically: all its records become visible or none do.

An append returns the record number of the first record in the block (unkeyed)
and the extent it landed in.

### 8.2 Record framing

Fixed at object creation:

| Framing | Boundary |
|---|---|
| `LengthPrefixed` | varint or fixed-width length precedes each record |
| `Nul` | records terminated by `0x00` |
| `Lf` | records terminated by `\n` |
| `CrLf` | records terminated by `\r\n` |

`LengthPrefixed` is the only framing permitting arbitrary binary content; the
terminator framings require the terminator not appear inside a record, and exist
so existing line- and NUL-delimited data can be stored and scanned without a
rewrite.

### 8.3 The three mutations

| | Unkeyed | Keyed |
|---|---|---|
| **Insert** | appended to the object's tail extent; allocates the next record number | written to the log of the extent whose key range covers the key |
| **Update** | `(local record number, new bytes)` appended to that extent's log | `(key, new bytes)` appended to the log |
| **Delete** | tombstone for the local record number, appended to the log | tombstone for the key, appended to the log |

Update and delete never touch the base, never change any cumulative record
count, and never reopen a sealed extent. Insert is the only operation that moves
counts, which is why unkeyed inserts are confined to the object's end.

---

## 9. Reading

### 9.1 Unkeyed: locating record *N*

1. **DN**: resolve the path, read the object metadata, binary search the stream
   list on starting record number → stream *S* and a stream-local record number.
2. **SN** holding *S*: binary search the extent list → extent *E* and an
   extent-local record number; read *E*'s instance list.
3. If *E* has a log, check it for a newer version or tombstone. If found, done.
4. Otherwise binary search *E*'s sparse index → nearest preceding checkpoint.
5. **EN**: read that granule from any instance, decrypt, decompress, parse
   forward by framing, stop at the target.

Two binary searches, one small log probe, one index search, one granule read —
but spread over three node types, so a cold point read costs **three round
trips**. Sealed extents are immutable, which makes almost everything above
cacheable at the client: the object's stream list changes only when the object
grows, and an extent's instance list changes only on repair or re-encoding. A
warm client goes straight to the EN.

### 9.2 Keyed: locating key *K*

Identical in shape, with `first_key` comparisons replacing starting record
numbers and the sparse index mapping `key → offset`. The log probe is not
optional here: keyed logs hold inserts, so a key absent from the base may still
exist.

### 9.3 The sparse index

One checkpoint approximately every **1 MB** for HDD-backed extents and **20 KB**
for SSD-backed extents, tuned to the device's efficient read size: on an HDD the
seek dominates and there is no point indexing finer than what you would read
anyway; on an SSD there is no seek to amortize, so finer granularity pays.

**Checkpoints record physical offsets** — positions in the compressed, encrypted
body, which is what a reader actually seeks to. A checkpoint is
`(local record number or key, physical offset)`, optionally with the logical
offset if callers need logical positions.

Cost, at a 32 MB physical extent and 8-byte checkpoints:

| Device | Granularity | Checkpoints | Index size | Overhead |
|---|---|---|---|---|
| HDD | 1 MB | 32 | ~256 B | 0.0008% |
| SSD | 20 KB | ~1,600 | ~13 KB | 0.04% |

Device class is a property of the extent, so an object may migrate between tiers
extent by extent.

### 9.4 Scans

A full scan ignores the index: walk the stream list in order, the extent list in
order, read each extent at full device bandwidth, merging its log. Because
streams sit on distinct SNs and their extents on many ENs, scans of a large
object parallelize across the cluster naturally.

Keyed objects support ordered and range scans by key but **not** positional
access — "the 10,000,000th record" is unanswerable, because counts are
statistics rather than addresses (§3).

---

## 10. Compaction

Compaction reads a base plus its log and writes a fresh base with updates
applied, tombstones dropped, and (keyed) inserts merged in key order, then
atomically swaps the pair in the SN's extent list.

- **Extent membership of a stream changes over time.** Extent *position* within
  a stream is unstable; extent *identity* must be a stable unique id that
  outlives position.
- **Keyed compaction can split.** If the merged result exceeds either limit of
  §6 it becomes two extents covering two key subranges, spliced into the extent
  list. This makes keyed objects B-tree-shaped — one log level per base,
  splitting on overflow — rather than LSM-shaped with many levels.
- **Unkeyed compaction never splits or renumbers.** Update and tombstone
  preserve count, so the new base covers exactly the record-number range the old
  one did and everything above it is untouched. Purely local.
- Compaction is a rewrite, so it is also where EC re-encoding, device-class
  changes, recompression, and **encryption key rotation** happen.
- Compaction is a **billing event**: it reclaims the physical bytes held by
  superseded versions and tombstones (§12).

⚠️ Compaction policy — triggers, write-amplification budget, split throttling —
is not designed. Billing makes it sharper than usual: the schedule directly
determines a customer's bill.

---

## 11. Small objects

Three regimes by size, which map cleanly onto the node types of §5:

| Regime | When | Storage | Nodes used |
|---|---|---|---|
| **Inline** | very small | records live inside the object's metadata record | **DN only** |
| **Packed** | below half a max-sized extent | shares one extent with other objects; the extent carries a directory of its occupants | DN + SN + EN |
| **Dedicated** | above that | its own streams and extents | DN + SNs + ENs |

That an inline object touches no SN and no EN at all is the main reason the
three-tier split does not penalize the small case: a 30-record object is one DN
record, with no stream, no extent, and no instance anywhere.

The half-extent threshold guarantees a packed object never straddles an extent
boundary: at most half of a maximum extent, at least two objects fit and any
packable object fits whole.

⚠️ "Half a max-sized extent" is ambiguous now that §6 gives two maxima. Half of
4 GB logical and half of 32 MB physical are very different; since the physical
limit almost always binds, the packing threshold is presumably ~16 MB physical.
Needs confirming.

⚠️ Packing collides with billing and compression. A packed extent is compressed
as a unit, so per-object physical size is **not additive** — two objects that
share substrings compress better together than apart, with no principled way to
attribute the saving. Either compress each packed object independently (losing
ratio, and ratio is billable) or attribute physical bytes proportionally to
logical size (approximate, and customers can notice). Deleting one packed object
also leaves garbage until the shared extent is compacted, and packed objects
share an EC fate.

---

## 12. Billing and metering

**Customers are billed by physical bytes stored, summed over instances.** A
32 MB compressed body held as 4 replicas bills as 128 MB; the same body as a 6+3
erasure code bills as ~48 MB.

This makes several internal choices customer-visible:

- **Compression works for the customer, not just the fleet.** Ratio divides the
  bill, which is why compression must precede encryption (§7) and why shrinking
  compression blocks for index density is not a free trade (§7.1).
- **Sealing and EC cut the bill ~2.7×** (4 replicas → 6+3), so the latency
  between sealing and successful encoding is money. That needs an explicit
  target and a decision about who absorbs the cost of the gap.
- **Compaction reclaims billable bytes**, so the compaction schedule sets part
  of the bill.
- **Metering must be per-object over time** — physical bytes × instances ×
  duration — surviving failures and double-counting nothing during repair,
  re-encoding, and compaction, where old and new bases briefly coexist.
- **DN metadata is not free.** If metadata can exceed data (§5.1), then whether
  it is billed is not a rounding decision.

⚠️ Open policy questions, all customer-facing:

1. Do customers pay for **uncompacted garbage** they created by overwriting,
   when DeLawson chooses the compaction schedule?
2. Do they pay for the **transient double footprint** during compaction and
   re-encoding?
3. Do they pay for **metadata** — DN records, and SN extent lists?
4. How are **packed objects** attributed (§11)?
5. Is the billed figure — compressed size, which the customer cannot directly
   control — reported back to them, and at what granularity?

---

## 13. Consistency, membership, and failure

- **Durability** on write comes from replicating an append block to all open
  instances on their ENs before acknowledging. Sealing is the commit point that
  fixes content.
- **EN failure on an open extent**: seal the extent short and open a new one.
  This removes replica repair from the write path entirely, and is cheap
  precisely because §6 makes extent size variable anyway.
- **EN failure on a sealed extent**: background fragment reconstruction over
  ciphertext, never blocking a write, never needing keys.
- **SN failure**: the streams it served are unaddressable until recovered, even
  with every EN healthy — see §5.2. This is the most serious single-node failure
  in the design.
- **DN failure**: objects under it are unreachable; the directory hierarchy needs
  its own replication story, which depends on the self-hosting question in §5.1.

⚠️ Membership, leader election, and how a stream is reassigned from a dead SN to
a live one are undesigned.

---

## 14. Does the metadata fit? (sizing)

Three tiers, three different answers, and the binding constraint turns out to be
the width of the record number rather than storage at any level.

### 14.1 SN: bounded per stream, RAM-resident

An SN serves a stream's extent list from RAM, and that is what bounds a stream.
Assume the worst case for capacity, `compressed = physical` (ratio 1×), so an
extent's logical span equals its 2^25-byte (32 MB) physical size.

| Quantity | Value |
|---|---|
| Extent metadata entry | ~100 B — **a guess, to be measured** |
| Extents in 256 GB of RAM | ~2^31 (≈214 GB) |
| Extent logical span | 2^25 B |
| **Stream ceiling** | 2^31 × 2^25 = **2^56 B = 64 PiB** |

Compression only helps: at 4× the same 2^31 extents span 256 PiB logical.

Since one full stream fills one SN's memory, **streams and SNs are effectively
one-to-one at full size**, and smaller streams share an SN.

**Sensitivity to the 100 B guess.** The stream ceiling scales inversely with
entry size, so measuring it moves the answer but not the conclusion:

| Entry size | Extents in 256 GB | Stream ceiling | Streams for 2^64 records | Stream list |
|---|---|---|---|---|
| 50 B | 2^32 | 128 PiB | ~12,800 | ~1.3 MB |
| 100 B | 2^31 | 64 PiB | ~25,600 | ~2.5 MB |
| 200 B | 2^30 | 32 PiB | ~51,200 | ~5 MB |
| 400 B | 2^29 | 16 PiB | ~102,400 | ~10 MB |

⚠️ The ~100 B entry is tight. For an unkeyed extent it must hold an extent id,
the starting record number, cumulative logical and physical bytes, flags, and
the instance or fragment locations — roughly 4 × 16 B for four instances plus
~40 B of the rest. A keyed extent additionally needs a variable-length,
user-supplied `first_key`, which may not fit at all: either the entry grows and
the stream ceiling falls proportionally, or the list stores a bounded key prefix
and disambiguates by reading the extent.

### 14.2 DN: unbounded in object count, disk-backed

To hold 2^64 records at 100 bytes each — 1.845 ZB — an object needs
1.845 ZB / 64 PiB ≈ **25,600 streams**, a stream list of a few MB. Trivial.

The DN's problem is the opposite one. Object metadata runs to something like:

| Field | Bytes |
|---|---|
| object id, parent directory id | 32 |
| name | ~32 |
| timestamps | 24 |
| flavor, framing, device class, flags | 8 |
| record count, logical and physical byte totals | 24 |
| ACL reference, stream list pointer | 16 |
| **Total before any records** | **~136 B** |

For a 30-byte object that is **4.5× more metadata than data**. For a 200-byte
object it is still ~68%. Multiply by an unbounded object count and the DN tier
can exceed the EN tier in total size — which is why DN state is disk-backed and
cached, not resident (§5.1).

The asymmetry is worth stating plainly: **SN state is bounded per stream by
construction; DN state is bounded by nothing.** Only the DN tier has to be a
paging, caching store.

### 14.3 What actually binds

Two independent ceilings on one object:

- **Record number**: 2^64 records, from the 8-byte record number.
- **Capacity**: if an object's stream list is allowed ~2^31 entries, then
  2^31 streams × 2^56 B = **2^87 bytes**.

These meet at an average record size of 2^87 / 2^64 = 2^23 = **8 MB**. Below
8 MB average the record number runs out first; above it, byte capacity does.

Since records are expected nearer 100 bytes than 8 MB, **the 8-byte record
number binds by a factor of ~10^5**. An object exhausts its record numbers long
before any list approaches its limit. The structure is not strained; the address
space is.

### 14.4 Correction to earlier drafts

An earlier version argued that 32 MB extents and large streams were in tension,
concluding the physical extent limit had to rise toward 4 GB. That assumed a
stream's extent list had to stay in the tens of megabytes, sized for cheap
consensus replication. The real constraint is one SN's RAM, ~10^4 times larger,
which dissolves the problem: the 32 MB limit stands.

A second draft then over-generalized RAM-residency to *all* metadata. §14.2 is
the correction: it holds for SNs and fails for DNs, because object count is
unbounded while extents per stream is not.

What survives is only the observation that shrinking extents moves addressing
information up from the extent's sparse index into the stream's extent list.
That trade is real; it just lands well inside budget.

---

## 15. Open questions

Roughly in order of how much else depends on them.

1. **Does DeLawson store DN metadata in DeLawson?** (§5.1) Self-hosting keyed
   objects is attractive and creates a bootstrap and circular-failure problem.
   Decides the shape of the whole DN tier.
2. **SN state durability and reassignment** (§5.2, §13). Self-describing extents
   make the extent list rebuildable from ENs; is that enough, or is a journal or
   consensus group needed? How does a stream move to a live SN?
3. **Bounding open logs per stream** (§4.2). RAM bounds the extent list; nothing
   bounds the logs, and each costs EN replicas and network.
4. **Directory semantics** (§5.1): path resolution, rename cost, listing huge
   directories, permissions, quotas.
5. **The extent metadata entry: measure it** (§14.1), and decide whether a keyed
   extent's `first_key` fits or forces a bounded prefix.
6. **Compression block size vs index granularity** (§7.1). A 20 KB SSD granule
   hurts compression ratio, and ratio is billable.
7. **Keyed objects** generally — the split-on-overflow B-tree shape of §10 needs
   its own pass.
8. **The packing threshold** (§11) — half of *which* maximum? Presumably ~16 MB
   physical.
9. **Packed-object billing attribution** (§11, §12) — per-object physical size is
   not additive under shared-extent compression.
10. **Billing policy** (§12): garbage, transient double footprint, metadata,
    reporting granularity.
11. **Key custody and hierarchy** (§7.2); customer-supplied keys?
12. **EC vs read locality** (§4.3): align the fragment stripe to the granule, or
    keep a full replica?
13. **Compaction policy** (§10) — now partly a pricing decision.
14. **Membership and leader election** (§13).
15. Is `count` expected exact and cheap? Cumulative counts make it cheap, and
    because update and tombstoned delete preserve count, exact for unkeyed
    objects. For keyed objects it is a maintained statistic.
16. `Nul` and `\0` framing were both listed originally and are folded together
    (§8.2). Confirm no distinction was meant.
17. Does a tombstoned unkeyed record number stay readable as "deleted" forever?
    Reclaiming it would renumber, so presumably never — worth stating
    deliberately.

**Closed** since earlier drafts: the unkeyed overwrite/addressing contradiction
(§3); logical vs physical extent size, which is compression (§6); the metadata
topology, now three explicit node types (§5); and the supposed 32 MB-vs-large-
streams tension, based on a wrong constraint (§14.4).
