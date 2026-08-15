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

Both push the same way on one point, and it is the origin of the machine types in
§5: **instances of an extent must sit in places that fail independently, and
the metadata describing an extent must not sit with the extent's own data.** A
failure that destroys the bytes must not also destroy the map to them.

### 1.2 One security invariant

**User code runs only on Compute machines, and decryption keys are never on a
machine that runs user code** (§5.9.1). Stated here because it constrains the
machine layout rather than following from it.

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

Each level answers one question, searching on cumulative counts for unkeyed
objects and on first keys for keyed ones:

| Level | Question | Search key |
|---|---|---|
| Object | which stream? | starting record number / first key |
| Stream | which extent? | starting record number / first key |
| Extent | roughly where inside? | sparse index (§9.3) |
| Instance | which disks hold those bytes? | replica / fragment list |

The list in the diagram is the *stored* form; the *served* form is a B-tree
(§4.4). Which of the totals a level carries may be kept exact, and which only
lazily, follows a single rule — §4.5.

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

### 4.4 Stored as lists, served as B-trees

The persistent form and the serving form differ:

| | Stored | Served |
|---|---|---|
| Stream | list of extents | B-tree in memory |
| Object | list of streams | B-tree in memory |

A list is the right thing to *write*: appends are cheap, the format is compact
(§14.1 depends on ~100 B per entry), and it is trivially scannable for the
rebuild-from-ENs path of §5.2. A B-tree is the right thing to *serve*: lookup is
a descent rather than a scan, and — decisively — **insert and split in the
middle are cheap**, which is what keyed objects need (§10) and what a flat
sorted array cannot give.

So an SN loads a stream's extent list from disk into a B-tree on assignment, and
serves from the tree.

Because SNs and DNs are Raft rings (§5), the two forms fall into the standard
consensus arrangement, and the relationship between them is not a new problem:

| Form | Raft role |
|---|---|
| Stored list on disk | **snapshot** |
| Raft log | **journal** of mutations since the snapshot |
| In-memory B-tree | **state machine** |

Recovery is snapshot-then-replay, snapshotting truncates the log, and a new ring
member catches up by shipping the list and replaying. Scanning ENs to rebuild
(§5.2) sits behind all of this as a last resort.

### 4.5 Which summaries may be kept exact

Every level carries totals — record counts, logical bytes, physical bytes — and
they cannot all be maintained the same way. One rule decides it:

> **A summary that only ever grows at the tail may be updated synchronously all
> the way to the root. A summary of something that can change anywhere in the
> tree either should not exist, or is updated only lazily.**

A tail-only update touches just the rightmost spine of the B-tree — O(depth)
nodes, all of them already hot. A change in the middle would dirty an arbitrary
interior path and, worse, invalidate every prefix sum after it.

Applying the rule:

| Summary | Changes where? | Treatment |
|---|---|---|
| Unkeyed record count | tail only — appends only at the end (§3), and update and tombstone preserve count | **synchronous, exact, to the root** |
| Unkeyed logical/physical bytes | anywhere — a log append in a middle extent grows it | **lazy** |
| Keyed record count | anywhere — inserts land mid-tree | **lazy, or absent** |
| Keyed byte totals | anywhere | **lazy** |
| Extent instance list | anywhere — repair, re-encoding | not a summary; maintained directly |

Two consequences worth stating outright:

- **The addressing summary is the one that gets to be exact.** Unkeyed lookup
  needs cumulative *record counts*, which are precisely the tail-only quantity;
  it never needs cumulative bytes. That is not luck, it is why the count-based
  scheme of §3 works at all: the only summary on the critical path is the only
  one that can be maintained synchronously.
- **Byte totals — hence billing — are lazy.** See §12.

Lazy summaries are brought forward by the background loops of §13.1, not by the
write path.

---

## 5. The physical decomposition: machine types

The logical levels of §4 do not all live in the same place. Splitting them is a
reliability requirement, not an optimization (§1.1). Three machine types carry
the storage hierarchy itself; §5.9 adds the four that surround it.

| Node | Holds | Replication | Why separate |
|---|---|---|---|
| **DN** — Directory Node | The directory hierarchy and user object metadata, including each object's stream list, and inline records for tiny objects | **Raft ring** | User metadata can exceed the data itself; a distinct workload with a distinct size |
| **SN** — Stream Node | Per stream: the extent list. Per extent: the instance list | **Raft ring** | Must not sit with the bytes it maps |
| **EN** — Extent Node | Extent instances: contiguous blocks of bytes on disk | none — instance placement *is* its redundancy | The bytes themselves; the only tier that scales with raw capacity |

```
client ──▶ DN            resolve path → object metadata → which stream?
       ──▶ SN            which extent? → which instances?
       ──▶ EN            the bytes
```

The asymmetry in the replication column is the heart of the design. **Metadata is
made reliable by consensus; bulk data is made reliable by placement.** Consensus
is the right tool for small mutable state that must be exactly right, and the
wrong tool for petabytes — so DNs and SNs are Raft rings, while an extent's
redundancy comes from having several independently-placed instances (§4.3) and,
once sealed, erasure coding. Both mechanisms nonetheless need members placed in
independently-failing domains, which is why §5.6 serves both.

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

DN state is a large, sorted, key-addressed collection that must be paged from
disk and cached — which is exactly the keyed object of §3. **It is therefore
stored in DeLawson keyed objects, with a bootstrap root outside.** See §5.5.

⚠️ The directory hierarchy brings semantics only partly designed: path
resolution and rename are addressed by the key choice in §5.5.4, but listing a
directory with a billion entries, permissions, and quotas are not. Billing
(§12) also attaches naturally to directories and accounts rather than to bare
objects.

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

A stream's ceiling is what one SN ring member can hold (§14.1). A maximal object
therefore spreads its streams over many SNs — necessarily, since no single
machine can hold them all.

**An SN is a Raft ring, not a machine** — and only three of its members hold the
extent list in RAM at all (§5.7). Losing one member costs capacity headroom, not
availability; ring members are placed in independently-failing domains by §5.6,
for the same reason extent instances are.

Note what this means for §14.1: a materialized member holds the *whole* extent
list, so the stream ceiling is set by the share of one machine's RAM that this
ring gets — and since a machine hosts many rings (§5.8), that share is a small
fraction, not the whole.

**Self-describing extents remain worth having as a backstop.** Each instance
should carry a header with its extent id, stream id, starting record number, and
byte totals, so that an extent list is reconstructible by scanning ENs. With a
Raft ring this is no longer the primary durability mechanism — it is the recovery
path for losing a whole ring at once, which is exactly the correlated failure
§5.6 exists to make unlikely but cannot make impossible. It also gives an
independent check on the ring's state, which matters most for the metadata tier
(§5.5.3).

### 5.3 ENs — Extent Nodes

ENs store extent instances and nothing else: a contiguous block of compressed,
encrypted bytes per instance. They are the only tier that scales with raw
capacity, and the only tier that erasure coding and repair touch.

Because ENs hold ciphertext (§7), repair, re-encoding, and scrubbing all run
without key access.

### 5.4 Three spreading requirements, of two kinds

| Requirement | Level | Reason |
|---|---|---|
| Instances of one extent on ENs in distinct failure domains | extent → instances | Reliability and availability (§1.1) |
| Members of one Raft ring in distinct failure domains | SN and DN rings | A ring whose members fail together is not a ring |
| Streams of one object on distinct SNs | object → streams | Capacity: no machine holds them all |

The first two are the same problem — place *n* things so they fail
independently — and are solved by one mechanism, §5.6. The third is arithmetic,
not reliability, and conflating it with the others would be a mistake: a
placement policy good for correlated-loss avoidance is not automatically good
for spreading capacity.

### 5.5 The DN tier is self-hosted

**Decided: DN metadata lives in DeLawson keyed objects, with a bootstrap root
outside DeLawson.** Building a separate store for it would duplicate
compaction, replication, erasure coding, compression, and encryption — for a
workload the keyed object already is.

#### 5.5.1 The resolution chain has fixed depth

Self-hosting invites infinite regress: an object's metadata is in an object,
whose metadata is in an object. It must terminate at a fixed, small depth.

| Level | What | Where its own locator lives |
|---|---|---|
| 0 | **Bootstrap root** — tiny, strongly consistent, *outside* DeLawson | replicated by its own means |
| 1 | **Partition map** — a keyed object: key range → metadata object | level 0 |
| 2 | **Metadata objects** — keyed objects: object key → object metadata | level 1 |
| 3 | **User objects** | level 2 |

Three hops, not recursion. This is the same shape, and for the same reason, as
BigTable's Chubby file → root tablet → METADATA tablets → user tablets.

The bootstrap root must contain exactly what a DN would otherwise supply for
level 1: the partition map's object id, its stream list (stream ids plus SN
locations), its totals, and its key identifier. The partition map is small — a
million partitions at ~136 B is ~136 MB, comfortably one stream — so **the root
is a few kilobytes and changes almost never.** That is what makes it cheap to
keep strongly consistent by other means.

Level 2 is partitioned rather than one global object mainly to bound contention
and blast radius; a single keyed object could hold the whole namespace as far as
capacity goes (§14.3).

#### 5.5.2 Damping the write recursion

The hazard: a user write updates metadata, which is a write to a DeLawson
object, which updates *its* metadata, which cascades. A cascade to the root on
every append would be fatal.

Two dampers exist already, and one problem needs a decision.

- **Extent churn does not propagate at all.** An object's extent list lives on
  its SN, not in DN metadata (§5.2). So a metadata object sealing, splitting, or
  compacting extents never touches the level above it. This is the main damper,
  and it is free — a consequence of putting extent lists on SNs rather than in
  object metadata.
- **Stream growth barely propagates.** Level 1 needs updating only when a level-2
  metadata object gains a *stream* — every ~64 PiB (§14.1). Effectively never.
- **The cascade terminates after one level**, by the rule of §4.5. Writing a
  user object's new record count into DN metadata is an *update* to an existing
  level-2 record. An update is count-preserving (§3), so the level-2 metadata
  object's own record count — the only summary maintained synchronously — does
  not change at all. What does change is its byte totals, and those are lazy by
  §4.5. So the propagation stops: one level up, and no further. The feared
  cascade to the root cannot happen.

- ⚠️ **Write amplification into the metadata tier is a separate problem**, and
  it survives. Even without a cascade, updating DN metadata on every user append
  is one metadata write per data write. The remedy is the same as §4.5's
  treatment of byte totals: keep the authoritative live count on the SN, which
  already maintains it exactly and synchronously for addressing, and let the DN
  copy be a lazily-refreshed cache. Then a steady stream of appends touches no
  DN at all.

  Recommended, not decided — but note the recommendation now rests on write
  amplification alone, not on any risk of unbounded recursion.

Nothing else in DN metadata is volatile: the stream list is append-only, because
a stream's starting record number never changes once assigned (§3).

#### 5.5.3 Circular failure

Self-hosting means a DeLawson outage can prevent recovering DeLawson. Three
requirements follow:

- The **bootstrap root must be durable and available independently** of
  DeLawson — it cannot be stored in the thing it bootstraps.
- **Metadata extents must be identifiable on ENs.** §5.2 already wants
  self-describing instance headers so SN state can be rebuilt by scanning ENs;
  those headers should also carry a flag marking metadata extents, so a
  disaster-recovery scan can rebuild the metadata tier **first**, before anything
  that depends on it.
- **Metadata objects want their own placement policy**: more redundancy, maximal
  spread, and possibly replicas retained rather than erasure-coded (§4.3), since
  metadata reads are frequent, small, and latency-critical in a way bulk data is
  not.

⚠️ The key hierarchy has the same circularity: metadata objects are encrypted
(§7), so their keys must be obtainable without reading metadata. The root must
anchor the key hierarchy, not just the object hierarchy.

#### 5.5.4 What the metadata key should be

| Key | Listing a directory | Renaming a directory | Path resolution |
|---|---|---|---|
| Full path | prefix scan | rewrites every descendant key | one lookup |
| `(parent dir id, name)` | prefix scan on parent id | one entry changes | one lookup per component |

**Recommended: `(parent directory id, name)`.** Renaming a directory with a
billion descendants must not be a billion-record rewrite, and prefix scans still
make listing cheap because keyed objects are sorted. The cost is a lookup per
path component, bounded by path depth and highly cacheable.

⚠️ Not decided.

#### 5.5.5 Two dividends

- **Metadata gets compression, erasure coding, and repair for free.** Object
  metadata is highly repetitive — sibling paths, adjacent timestamps, identical
  flavor and framing — so it should compress several-fold, and it is
  erasure-coded once sealed. This materially shrinks the "metadata can exceed
  data" problem of §14.2: true in logical bytes, much less true in physical ones.
- **Billing metadata becomes measurable** rather than estimated, because it is
  physical bytes in real objects (§12).

### 5.6 Failure domains and placement

Reliability (§1.1) requires instances that fail independently. A Raft ring wants
the same thing for a different reason: a ring whose members fail together is not
a ring. **Extent-instance placement and ring-member placement are therefore one
problem**, and one mechanism should serve both.

#### 5.6.1 Slack

To place *n* independent things, use a failure-domain type that has at least
**slack × n** domains. Slack is configurable; the default is **4/3**.

| *n* | Domains required |
|---|---|
| 3 instances | 4 |
| 4 instances | 6 |
| 5-member ring | 7 |
| 7-member ring | 10 |

With exactly *n* domains there is no freedom at all: one domain down — planned
or not — and there is nowhere to put a replacement, so the group stays degraded
until that domain comes back. Slack buys somewhere to re-place, which is what
turns repair from *waiting* into *work*.

#### 5.6.2 Nested, not orthogonal

Domain types must be **nested** — each a refinement of the one above, forming a
single hierarchy. Cross-cutting, orthogonal domain types are **a bug**.

The cost of getting this wrong, using 3 instances with 4 power domains and 4
software-deployment domains:

| | Where instance 3 may go | Fraction of cluster |
|---|---|---|
| **Orthogonal** (a 4×4 grid) | must differ from both predecessors in power *and* software: 2 powers × 2 softwares = 4 of 16 cells | **1/4** |
| **Nested** (software within power) | satisfying the coarse constraint satisfies the fine one automatically: 2 of 4 power domains | **1/2** |

The general shape: with *d* orthogonal types of *k* domains each, the *i*-th
instance may be placed in `((k−i+1)/k)^d` of the cluster; nested, it is
`(k−i+1)/k` **regardless of d**. For the case above the sequence is
1, 0.75, 0.5 nested against 1, 0.5625, 0.25 orthogonal.

**Orthogonality costs an exponent.** That is not merely inelegant: shrinking the
candidate set slows repair, worsens balance, and can make placement impossible
in a cluster that is already partly degraded — exactly when placement matters
most. Nesting keeps the constraint linear no matter how many domain types are
added, which means new domain types (a new firmware tier, a new network fabric)
can be introduced without compounding cost.

#### 5.6.3 What nesting costs

**Planned disruptions must be serialized.** Software deployments and large
planned outages cannot run at the same time.

⚠️ The placement arithmetic above is verified; this operational constraint is
asserted from experience and its precise argument should be written out before
anyone relies on it. The mechanism appears to be that nesting collapses every
domain type onto one hierarchy, so "at most one domain intentionally down"
becomes a single global budget shared by every kind of planned work, rather than
one budget per orthogonal dimension.

#### 5.6.4 A concession that is a known defect

⚠️ Orthogonal software-deployment domains may be supported anyway, because that
is how clusters are built today. This is a compatibility concession and a known
defect, not a design choice, and it should be labelled as such wherever it is
configurable — it costs the exponent in §5.6.2.

### 5.7 Ring composition: consensus membership is not RAM membership

Raft tolerates *n* failures with 2n+1 members. Materializing the served B-tree
(§4.4) on all of them would cost **7× RAM at n=3** — and RAM is the resource that
bounds a stream (§14.1), so that is 7× the most expensive thing in the system.

It is also unnecessary. **Consensus needs 2n+1 voters; only three members need
the state materialized.**

| Role | Count | Cost | Job |
|---|---|---|---|
| Primary | 1 | CPU + RAM | serves reads and writes; holds the B-tree |
| Hot backup | 1 | RAM | always up to date; the designated next primary |
| Checkpoint writer | 1 | RAM | takes write locks to write checkpoints, without stalling the primary |
| Log-only members | 2n+1 − 3 | disk | store the Raft log and vote; materialize nothing |

At n=3 that is **3× RAM instead of 7×**, with four cheap log-only members
supplying the remaining durability and votes.

The third materialized member exists because writing a checkpoint — the on-disk
list form of §4.4 — needs a consistent view of the tree, which means taking write
locks. Doing that on the primary or on the always-current hot backup would stall
serving, so it gets a member of its own.

⚠️ **That third member is removable.** Given transactions and a consistent read
that can be held for ~10 minutes, the single hot backup could write its checkpoint
from a snapshot without taking write locks, bringing the cost to **2× RAM**. The
ten minutes is roughly checkpoint size over write bandwidth: at ~500 MB/s it is a
checkpoint of ~3 × 10^11 bytes, about one machine's full memory. Note that §5.8
shrinks checkpoints by orders of magnitude, which makes this optimization easier
rather than harder.

#### 5.7.1 Primary switchover has three quality tiers

| Case | Client uncertainty | Requires |
|---|---|---|
| Scheduled: known next primary, known time | **milliseconds** | switchover planned in advance |
| Known next primary, unplanned time | one wrong guess, then correct | a designated successor |
| Ordinary Raft election | wait for the winner to catch up | nothing |

The third case is the expensive one, and the role split above is why: **a log-only
member elected primary must materialize the whole B-tree from checkpoint plus log
before it can do anything.** So elections must prefer materialized members
strongly — and the hot backup is exactly that, a designated successor that is
already current. The cheap tiers are not optimizations bolted on; they are the
normal path, and ordinary election is the fallback.

### 5.8 Many rings per machine

A metadata machine should hold members of **many** rings in different roles:
primary for some, hot backup for others, checkpoint writer for a few, log-only for
many more. Two reasons:

- **Cost amortization.** Expensive roles (CPU for primaries, RAM for materialized
  backups) and the cheap one (a log on disk) mix on every machine, so no machine
  is all-expensive and none sits idle.
- **Core utilization.** Write-lock work within a ring is serialized, so one ring
  keeps about one core busy. To use *C* cores a machine needs on the order of *C*
  rings in a primary role. **Ring count per machine is set by core count.**

#### 5.8.1 This shrinks streams by a large factor

Easy to miss, and it corrects §14.1. If a machine is primary for *P* rings then,
by symmetry across the cluster, it is also hot backup for ~*P* and checkpoint
writer for ~*P* — so it materializes ~3*P* B-trees in *R* bytes of RAM. RAM per
ring is **R/3P, not R**, and the stream ceiling falls by the same factor.

At 256 GB per machine, 100 B per extent entry, and 32 MB extents:

| Primary rings/machine | Materialized | RAM/ring | Extents/stream | Stream ceiling | Streams per maximal object | Stream list | Materialize time |
|---|---|---|---|---|---|---|---|
| 1 | 3 | 85 GB | 8.5 × 10^8 | 25 PiB | 64,000 | 6 MB | 171 s |
| 16 | 48 | 5.3 GB | 5.3 × 10^7 | 1.6 PiB | 1.0 × 10^6 | 103 MB | 11 s |
| 64 | 192 | 1.3 GB | 1.3 × 10^7 | 0.4 PiB | 4.1 × 10^6 | 412 MB | 2.7 s |
| 256 | 768 | 333 MB | 3.3 × 10^6 | 0.1 PiB | 1.7 × 10^7 | 1.6 GB | 0.7 s |

(Materialize time assumes 500 MB/s — it is how long an elected log-only member
takes to become useful.)

At a plausible 64 cores, streams are **~400 TB rather than 64 PiB**, and there are
~4 × 10^6 of them in a maximal object rather than ~25,000.

#### 5.8.2 Consequences

Two of them are gifts:

- **Failover becomes fast.** Materializing a 1.3 GB B-tree takes ~3 seconds;
  materializing 214 GB takes ~7 minutes. The same arithmetic that shrinks rings
  shrinks the worst-case election, so §5.7.1's expensive tier stops being
  frightening.
- **Checkpoints become cheap**, which is precisely what the 2×-RAM optimization in
  §5.7 needs. Holding a consistent read for seconds is a far easier promise than
  holding one for ten minutes.

One needs watching:

- **The object's stream list grows proportionally** — ~412 MB at 64 rings per
  machine, and 1.6 GB at 256. That is affordable only because the stream list lives
  in a DN keyed object, which is disk-backed and paged (§14.2). It is no longer
  the trivially small structure §14.2 described, and at the high end it is itself
  a multi-extent object.

⚠️ So *P* is a real design parameter, not a tuning knob: it trades core
utilization and failover speed against stream-list size. It wants choosing
deliberately, and it interacts with the extent-entry size that §14.1 still needs
measured.

### 5.9 The rest of the fleet

Four more machine types surround the storage hierarchy.

Naming convention: **the three storage-hierarchy tiers end in `-N`** — DN, SN, EN
— **and the surrounding services do not**, so FE and KS read as outside the
hierarchy at a glance. Names stay plain and descriptive rather than clever, so
that a wide range of people can work on the code comfortably.

| Type | What it is | Holds keys? | Runs user code? |
|---|---|---|---|
| **KS** — Key Store | A Raft ring holding the daily master keys (§7.3); ~1 MB per century, unlocked by humans and hardware | **yes — and only here** | no |
| **FE** — Front End | Checks user authorization; coordinates clients talking to the other machines | yes, transiently | no |
| **Compute** | Runs user code. Eventually, not initially | **never** | **yes** |
| **`delawson`** — client | Library and CLI, talks to FE over the internet | see §5.9.2 | it *is* user code |

KS is a Raft ring for the same reason SNs and DNs are (§5): keys are small mutable
state that must be exactly right, which is what consensus is for and what bulk
replication is not. Unusually among the rings, KS's state is *tiny* — about a
megabyte per century (§7.3) — so its ring exists purely for reliability and
availability, never for capacity.

#### 5.9.1 The security boundary

> **User code may only run on Compute machines, and decryption keys may never be
> on a machine that runs user code.**

The boundary is drawn at *keys*, not at *bytes*, and that is the right place:
plaintext is one object's worth of exposure, while a key is every extent under it.
The blast radius differs by orders of magnitude, so the two deserve different
containment.

What falls out of the invariant:

- **ENs hold keys, but narrowly scoped ones.** They cache daily SN-master keys
  (§7.3.1), so an EN compromise is not limited to ciphertext — but it is bounded on
  two axes, to the days and the SNs whose keys that EN happens to hold, and it
  never yields a master key. Repair, re-encoding, and scrubbing still need no keys
  at all (§7), so those background loops stay outside the crypto path.
- **Compute machines receive plaintext, never keys.** User code may read what it is
  authorized to read; it may not hold the capability to decrypt anything else.
- **SNs and DNs need keys for their own state, not for user data.** Their
  checkpoints and logs are on disk and so are encrypted, and DN state is itself
  DeLawson keyed objects (§5.5) — but neither needs a user object's data key.

⚠️ **Compute-near-data and decrypt-at-FE pull against each other.** If only a
trusted tier may decrypt, and Compute may not, then every byte Compute reads
traverses a decryption tier — which is exactly the bandwidth funnel that
compute-near-storage exists to avoid. Enforcing the boundary by process or enclave
isolation *on* the Compute machine would fix the bandwidth problem, but the
invariant as stated is machine-level and excludes it. Either the invariant relaxes
to a hardware-isolation boundary, or the decryption tier's bandwidth has to scale
with Compute's input rate. This needs deciding before Compute is built, not after.

#### 5.9.2 Where decryption happens

⚠️ Genuinely open, and consequential:

| Decrypt at | Consequence |
|---|---|
| **FE** | FE sees plaintext for everything passing through it; keys never leave the datacenter; FE bandwidth must carry all bulk data |
| **Client** | End-to-end encryption — the operator cannot read customer data — but keys leave the datacenter and the client must be trusted to hold them |

The stated invariant forbids keys on Compute but says nothing about the client, so
both remain open. They are very different products: the second is a much stronger
privacy claim and a much harder key-management problem.

### 5.10 Node identifiers and bounded fan-out

**DN, SN, and EN identifiers are 32 bits**, so the design scales to 2^32 of each.

That immediately rules out a full mesh. There are only 65,536 ports per IP and a
usable ephemeral range of ~28,000, so a machine can hold on the order of 10^3–10^4
connections. Against 4.3 × 10^9 nodes, **a machine can see roughly one part in
400,000 of the cluster.** Connectivity is therefore deliberately partial:

> Each SN ring talks to only some ENs; each EN talks to only some SNs.

#### 5.10.1 How much fan-out is needed

Let *F* be the ENs an SN can see and *G* the SNs an EN can see. An SN machine's
rings map a fixed quantity of instance bytes, and each visible EN devotes roughly
`capacity/G` of itself to any one SN, so `F/G = instance bytes per SN ÷ EN
capacity`.

The instance bytes per SN machine come straight from §14.1: 256 GB of RAM at ~100 B
per entry across three materialized roles is 8.5 × 10^8 extents, which at 32 MB and
4 instances is **~115 PB of instance bytes**. Note this is **independent of P** —
more rings per machine means proportionally smaller streams, so the total extent
count a machine maps is set by its RAM, not by how that RAM is divided.

| EN capacity | *F/G* | *F* at *G*=1 | *G*=4 | *G*=16 | *G*=64 |
|---|---|---|---|---|---|
| 100 TB | 1145 | 1,145 | 4,581 | 18,325 | 73,301 |
| 1 PB | 115 | 115 | 458 | 1,833 | 7,330 |
| 10 PB | 11 | 11 | 46 | 183 | 733 |

*G* is the free choice, and it is the one that matters: **sharing is what gives
repair and balance somewhere to go.** With *G*=1 each EN answers to a single SN,
which is a static partition — cheap in connections, but a dead EN can only be
replaced from a pool of one SN's making, and a reassigned stream finds no ENs it
may talk to.

The table's real lesson: **EN capacity buys sharing.** At 100 TB per EN, *G*=16
needs 18,000 connections and is out of reach; at 1 PB per EN the same sharing costs
~1,800 connections and is comfortable. Fan-out policy and EN sizing are one
decision, not two.

#### 5.10.2 Neighborhoods must be domain-diverse

§5.6 requires instances in slack × *n* independent failure domains. Those domains
must now exist **within the F ENs an SN can see**, not merely somewhere in the
cluster. A neighborhood of 2,000 ENs that all share a power domain satisfies the
connection budget and violates the reliability requirement.

So fan-out sets are not arbitrary subsets: **each neighborhood must span at least
slack × n domains at every level of the nesting hierarchy** (§5.6.2). Slack is
likewise a neighborhood property, not a cluster property — repair re-places within
the neighborhood, so it is the neighborhood that must retain spare domains after a
loss. This is a materially stronger requirement than the cluster-wide version, and
it is the one that actually has to hold.

#### 5.10.3 Reassignment needs mutable fan-out

⚠️ If a stream moves from one SN ring to another (§13), the new ring must be able
to reach the ENs already holding that stream's extents. Otherwise the move requires
copying every byte. So either reassignment is confined to rings with compatible
neighborhoods, or **fan-out sets must be mutable** — which means connection churn
during recovery, exactly when the system is already busy.

This is the same shape as the key-derivation trap in §7.3.3: a mutable assignment
being used where a stable identity is needed. Both want deciding together.

#### 5.10.4 A dividend: 32-bit ids shrink the extent entry

32-bit node ids are a metadata-density decision as much as an addressing one.
§14.1's ~100 B entry looked tight when an instance reference was assumed to be
~16 B; with a 4-byte EN id plus a 4-byte intra-EN locator it is 8 B, and the whole
entry fits with room to spare:

| Field | Bytes |
|---|---|
| extent id | 8 |
| starting record number | 8 |
| cumulative logical bytes | 8 |
| cumulative physical bytes | 8 |
| day index (§7.3.3) | 2 |
| key generation and flags | 2 |
| 4 × instance reference (4 B EN id + 4 B locator) | 32 |
| **Total** | **68 B** |

That matters more than it looks: §14.1 shows the object's stream list grows as the
*square* of the entry size, so 68 B rather than 136 B is a 4× saving on the largest
metadata structure in the system. It also leaves headroom for the keyed extents'
`first_key`, which §14.1 flagged as the field that might not fit at all.

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
- Keys are per-extent, but **derived, not stored** — see §7.3.
- Sealed extents are immutable, so keys never need rotation in place; **rotation
  happens by rewriting during compaction** (§10), which is a rewrite anyway.

### 7.3 The key hierarchy: daily, derived, and scoped by SN

Per-extent keys are right, but *storing* them is arithmetically impossible. At 32
bytes each:

| Keys for | Count | Stored size |
|---|---|---|
| Extents in a 1 EB physical cluster | 3.0 × 10^10 | **954 GB** |
| Extents in one maximal object | 5.5 × 10^13 | **1.75 PB** |
| Objects, if a trillion tiny ones exist | 10^12 | **32 TB** |

None of those can live in a consensus ring. So the hierarchy stores almost nothing
and derives the rest:

```
daily master key         randomly generated, one per day
                         lives ONLY in KS; on KS disk, encrypted at rest
  → daily SN-master key  = derive(daily master key, SN)
                         cached on ENs
    → extent key         = derive(the correct daily SN-master key, extent)
      → block IV         = f(extent id, block index)        (§7.2)
```

Master keys are **random, not derived** — there is no key above them, which is
what makes the layer below unrecoverable without KS.

**The authoritative key store is kilobytes.** One random key per day:

| Retention | Keys | Size |
|---|---|---|
| 1 year | 365 | 12 KB |
| 10 years | 3,650 | 117 KB |
| 100 years | 36,500 | **1.2 MB** |

A century of master keys is about a megabyte, and — unlike the tenant-keyed scheme
this replaces — the figure does not depend on how many customers or objects exist.
Old daily keys must be kept as long as any extent encrypted under them survives,
which this makes free.

#### 7.3.1 Two-dimensional scoping bounds the blast radius

**ENs cache daily SN-master keys.** This corrects §5.9.1: ENs are not keyless, and
an EN compromise is not limited to ciphertext.

But the exposure is bounded on two axes at once:

| Axis | Bounded by | Effect of a compromise |
|---|---|---|
| Time | daily rotation | only the days whose keys are cached |
| Space | per-SN derivation | only extents of the SNs whose keys are cached |

And critically, **the master keys never leave KS**, so no number of compromised ENs
ever yields the ability to derive a key for a different SN or a different day.
Deriving downward is easy and deriving upward is impossible, which is what makes
caching the middle layer acceptable.

#### 7.3.2 KS is unlocked by humans and hardware

Master keys are written to KS disks encrypted, under an **unlocking key derived
from a combination of operator-entered secrets and hardware devices**. That is the
true root of trust — and it is deliberately not something a machine can produce
alone.

Consequences worth being explicit about:

- **Cold start requires human action.** If every KS machine restarts at once, the
  master keys on disk cannot be decrypted until operators supply their secrets and
  the hardware devices are present. There is no fully automatic recovery from a
  total KS outage, by design. This is a security choice being paid for in
  availability, and it should be an owned decision rather than a discovered one.
- **There are two independent roots.** §5.5.1's bootstrap root anchors the *object*
  hierarchy; the KS unlocking key anchors the *key* hierarchy. A cold start needs
  both, and neither can be stored inside what it unlocks.

#### 7.3.3 What the extent entry must record

Deriving an extent's key needs the *correct* daily SN-master key, so the extent
must say which one. A **day index** is enough and is cheap: 2 bytes covers 179
years, 3 bytes covers far more than the hardware will last.

⚠️ It must also identify the SN scope — and that has a trap. If derivation keys on
*which ring currently serves the stream*, then reassigning a stream to a different
ring (§13) orphans every key for its existing extents. **Derivation must use a
stable identifier**, the stream id rather than the current assignment, or
reassignment must carry the old scope forward explicitly.

⚠️ These bytes land in the ~100 B extent entry that §14.1 already calls tight, and
§14.1 shows the stream list grows as the *square* of that entry size. A day index
is a few bytes and cheap; it is worth counting deliberately rather than discovering
later.

#### 7.3.4 What this hierarchy does not give

⚠️ **There is no cryptographic tenant isolation.** Keys are scoped by day and SN,
not by customer, so two tenants' extents on one SN for one day derive from the same
SN-master key. Isolation rests on FE authorization (§5.9) and on keys never
reaching user code (§1.2) — not on separate key material. That is a coherent
position, but it forecloses customer-supplied keys and weakens any "we cannot read
your data" claim, so it interacts directly with §5.9.2.

⚠️ **Where each pipeline stage runs is now a security question, not just a
performance one.** §7 fixes the *order* — compress, then encrypt, then encode — but
ENs caching keys implies the ENs are doing encryption or decryption, which in turn
implies plaintext crosses the network to reach them. If instead FE or the client
encrypted, ENs would need no keys at all. Which machine performs which stage is
undecided and determines where plaintext exists.

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

0. **FE**: the `delawson` client sends the request; FE authorizes it against the
   object's ACL and coordinates the hops below (§5.9).
1. **DN**: resolve the path, read the object metadata, descend the stream B-tree
   (§4.4) on starting record number → stream *S* and a stream-local record number.
2. **SN** holding *S*: descend the extent B-tree → extent *E* and an
   extent-local record number; read *E*'s instance list.
3. If *E* has a log, check it for a newer version or tombstone. If found, done.
4. Otherwise binary search *E*'s sparse index → nearest preceding checkpoint.
5. **EN**: read that granule from any instance, decrypt, decompress, parse
   forward by framing, stop at the target.

Two B-tree descents, one small log probe, one index search, one granule read —
but spread over four machine types, so a cold point read costs **four round
trips** including the authorization hop. Sealed extents are immutable, which makes almost everything above
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

| Regime | When | Storage | Streams and extents of its own |
|---|---|---|---|
| **Inline** | very small | records live inside the object's metadata record | none |
| **Packed** | below half a max-sized extent | shares one extent with other objects; the extent carries a directory of its occupants | shares |
| **Dedicated** | above that | its own streams and extents | yes |

**Self-hosting (§5.5) changes what "inline" means.** Since DN metadata is itself
stored in DeLawson keyed objects, an inline object's records end up as bytes in a
*metadata* extent on ENs. There is no "DN only" regime — nothing is stored
outside the object hierarchy except the bootstrap root.

The small-object win is real but differently explained: an inline object has no
stream, extent, or instance **of its own**, and instead rides along in metadata
extents shared with thousands of other objects' metadata. So inline is really a
special case of packing, obtained for free — and with better locality than
explicit packing, because an object's metadata and its records land in the same
place.

⚠️ This blurs the boundary between the inline and packed regimes. If inline
already packs via the metadata object, what does the explicit packed regime add
beyond a larger size bound? Possibly nothing, in which case two regimes suffice:
inline-in-metadata, and dedicated. Worth resolving before either is built.

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
- **The billed quantity is a lazy summary.** By §4.5, physical byte totals change
  anywhere in the tree and so are maintained lazily by the background loops of
  §13.1 — the exact, synchronous summary is the unkeyed record count, which
  nobody is billed for. Billing therefore reads an eventually-consistent figure.
  This is tolerable for metering integrated over a month, but it means a
  customer's *instantaneous* usage query is either stale or must be answered by
  summing live extent totals on the SNs on demand. Which of those is the
  contract needs deciding.
- **DN metadata is not free, and is now measurable.** If metadata can exceed
  data in logical bytes (§14.2), whether it is billed is not a rounding
  decision. Self-hosting (§5.5) makes it a real physical-byte figure rather than
  an estimate — and compresses it several-fold on the way.

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
- **SN or DN member failure**: absorbed by the ring (§5.7), but the cost depends on
  *which* member. Losing a log-only member costs a vote. Losing the primary
  triggers switchover, cheap if scheduled or if the hot backup takes over (§5.7.1).
  Losing the hot backup is the one that quietly matters — it removes the cheap
  switchover path until a replacement is materialized, so restoring it should be
  prompt rather than lazy.
- **Loss of a whole SN ring**: the streams it served are unaddressable even with
  every EN healthy. This is now the worst failure in the design, and §5.6 exists
  precisely to make it improbable — a ring is only as independent as its
  placement. The recovery path is scanning ENs for self-describing extents (§5.2).
- **Loss of a whole DN ring**: worse, because of self-hosting (§5.5.3) — recovery
  must start from the bootstrap root and rebuild the metadata tier first.

⚠️ How a stream is reassigned from a failed ring to a fresh one, and how cluster
membership and domain topology are themselves distributed, are undesigned. Note
the topology is not static: §5.6 depends on knowing the domain hierarchy, so that
hierarchy is itself critical configuration that has to live somewhere reliable —
plausibly the bootstrap root (§5.5.1).

### 13.1 Background loops

Everything the write path is not allowed to do synchronously has to happen
somewhere. That somewhere is a set of **background garbage-collection loops**,
and by now the design has accumulated a substantial list of work for them:

| Work | Deferred from | Why it cannot be synchronous |
|---|---|---|
| Bringing lazy summaries forward | §4.5 | Byte totals change anywhere in the tree |
| Compaction: fold logs into bases | §10 | Rewrites an extent; also reclaims garbage |
| Erasure coding after sealing | §4.3 | Encoding a sealed extent takes time and bandwidth |
| Dropping replicas once EC is verified | §4.3 | Must not race the encode |
| Fragment reconstruction after EN loss | §13 | Must never block a write |
| Scrubbing and checksum verification | §1.1 | Catches decay before it accumulates |
| Reclaiming garbage in packed extents | §11 | A deleted packed object leaves a hole |
| Encryption key rotation | §7.2 | Happens by rewriting during compaction |
| Refreshing the DN's cached counters | §5.5.2 | Otherwise one metadata write per data write |
| Re-placing instances and ring members | §5.6 | Restoring domain independence after a loss |
| Raft snapshotting | §4.4 | Truncating the log must not block mutations |
| Rebuilding a ring's state from ENs | §5.2 | Disaster recovery, inherently slow |

Two observations. First, most of these are *the same operation* — compaction
rewrites an extent, and while rewriting it can re-encode, re-key, re-compress,
change device class, and reclaim garbage in one pass. Scheduling them jointly
rather than as independent loops is likely to matter.

Second, the loops are how the design pays for the §4.5 rule. Deferring
mid-tree summary maintenance is only sound if something eventually does it, and
the lag is directly visible in billing (§12) and in any `count` on a keyed
object.

⚠️ Loop scheduling, prioritisation, and rate limiting are undesigned, and they
compete with foreground traffic for exactly the resources — EN disk bandwidth
and SN memory — that the foreground path needs.

---

## 14. Does the metadata fit? (sizing)

Three tiers, three different answers, and the binding constraint turns out to be
the width of the record number rather than storage at any level.

### 14.1 SN: bounded per stream by a *share* of RAM

An SN serves a stream's extent list from RAM, and that is what bounds a stream.
Assume the worst case for capacity, `compressed = physical` (ratio 1×), so an
extent's logical span equals its 2^25-byte (32 MB) physical size.

The naive calculation gives a machine's whole memory to one stream:

| Quantity | Value |
|---|---|
| Extent metadata entry | ~100 B — **a guess, to be measured** |
| Extents in 256 GB of RAM | ~2^31 (≈214 GB) |
| Extent logical span | 2^25 B |
| Ceiling if one stream owned a machine | 2^31 × 2^25 = 2^56 B = 64 PiB |

**But a stream never owns a machine.** §5.8 requires many rings per machine — on
the order of one per core, to use the cores at all — and each machine materializes
roughly three B-trees per primary ring it hosts. So the real budget is `R/3P`, and
at 64 primary rings per machine the ceiling is **~0.4 PiB, not 64 PiB**. The table
in §5.8.1 is the authoritative version; the 64 PiB figure above is only the
degenerate *P*=1 case.

Compression relaxes this: at a 4× ratio the same extent count spans 4× the logical
bytes.

**Sensitivity to the 100 B guess.** The ceiling scales inversely with entry size,
so measuring it moves the answer but not the conclusion. Shown at *P*=64:

| Entry size | Extents/stream | Stream ceiling | Streams per maximal object | Stream list |
|---|---|---|---|---|
| 50 B | 2.7 × 10^7 | ~0.8 PiB | ~2.1 × 10^6 | ~103 MB |
| 100 B | 1.3 × 10^7 | ~0.4 PiB | ~4.1 × 10^6 | ~412 MB |
| 200 B | 6.7 × 10^6 | ~0.2 PiB | ~8.2 × 10^6 | ~1.6 GB |
| 400 B | 3.3 × 10^6 | ~0.1 PiB | ~1.6 × 10^7 | ~6.6 GB |

Note the stream list grows as the *square* of the entry size across this table —
once from more streams, once from fatter entries — so measuring the entry size
matters considerably more than it did when streams were 64 PiB. At 400 B and
*P*=256 the stream list reaches ~26 GB, which is still a paged DN object but no
longer casually affordable.

**The ~100 B entry now looks affordable**, thanks to 32-bit node ids: §5.10.4
itemizes an unkeyed entry at **68 B**, including the day index the key hierarchy
needs (§7.3.3). The earlier worry assumed 16 B instance references; 8 B is enough.

⚠️ A keyed extent additionally needs a variable-length, user-supplied `first_key`.
The 32 B of headroom under the 100 B budget covers a short key and no more, so
either the entry grows and the stream ceiling falls proportionally, or the list
stores a bounded key prefix and disambiguates by reading the extent. This is now
the only field in the entry whose size is not known.

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
can exceed the EN tier in *logical* size — which is why DN state is disk-backed
and cached, not resident (§5.1).

In *physical* bytes it is much less alarming, because self-hosting (§5.5) puts
that metadata through the same pipeline as everything else. Object metadata is
unusually compressible — sibling names share prefixes, timestamps cluster,
flavor and framing repeat across every object in a directory — so at a
conservative 4× and 6+3 erasure coding, ~136 logical bytes becomes ~34 physical
bytes and ~51 billed. Compare a bare 4-replica copy of the same field set at
544 bytes: **self-hosting makes the metadata tier roughly an order of magnitude
cheaper than storing it uncompressed and replicated.**

The asymmetry is still worth stating plainly: **SN state is bounded per stream
by construction; DN state is bounded by nothing.** Only the DN tier has to be a
paging, caching store — and, since §5.5, only the DN tier is stored in DeLawson
itself.

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

1. **The bootstrap root**: what exactly it holds, and how it is made durable and
   strongly consistent *outside* DeLawson (§5.5.1, §5.5.3). It must anchor the
   key hierarchy as well as the object hierarchy.
2. **Where decryption happens — FE or client** (§5.9.2). Two different products:
   operator-readable, or end-to-end encrypted with keys leaving the datacenter.
3. **Compute-near-data vs the key boundary** (§5.9.1). As stated the invariant is
   machine-level, which funnels all of Compute's input through a decryption tier.
   Relax it to hardware isolation, or size the tier's bandwidth accordingly —
   decide before Compute is built.
4. **Ring reassignment, fan-out mutability, and cluster topology** (§13, §5.10.3).
   How a stream moves from a failed ring to a fresh one that may not be able to
   reach its ENs; whether fan-out sets are mutable; and where the failure-domain
   hierarchy that §5.6 depends on is stored — plausibly the bootstrap root, since
   placement decisions cannot be made without it.
5. **Fan-out policy and EN sizing together** (§5.10.1). G, the sharing factor, is
   what gives repair and balance room to move, and EN capacity is what makes G
   affordable. One decision, not two.
6. **Why nesting forces serialized planned disruptions** (§5.6.3) — the
   arithmetic is verified but this operational constraint is asserted, and it
   determines how the cluster is actually operated.
7. **Choosing P, the rings per machine** (§5.8.1). Not a tuning knob: it trades
   core utilization and failover speed against stream-list size, and it multiplies
   with the unmeasured extent-entry size — the stream list grows as the *square*
   of that entry size across the plausible range.
8. **Transactions and long consistent reads** (§5.7). Getting them would drop the
   ring from 3× RAM to 2×, the single largest cost saving identified so far. §5.8
   makes the required read duration much shorter than ten minutes, so this looks
   more attainable than when first raised.
9. **Raft ring sizes and slack** (§5.6.1): how many members for an SN ring, for a
   DN ring, and does the 4/3 default hold for both?
10. **Where the live counters live** (§5.5.2). §4.5 shows the recursion cannot run
    away, so this is now a write-amplification question only: keep the
    authoritative count on the SN and let the DN cache it lazily, or accept one
    metadata write per data write.
11. **Bounding open logs per stream** (§4.2). RAM bounds the extent list; nothing
    bounds the logs, and each costs EN replicas and network.
12. **Background loop scheduling** (§13.1): prioritisation and rate limiting
    against foreground traffic, and whether the rewrite-shaped work — compaction,
    re-encoding, re-keying, garbage reclamation — is scheduled as one pass or many.
13. **Where the live counters live** (§5.5.2). §4.5 shows the recursion cannot run
    away, so this is now a write-amplification question only: keep the
    authoritative count on the SN and let the DN cache it lazily, or accept one
    metadata write per data write.
14. **The metadata key** (§5.5.4): `(parent dir id, name)` recommended over full
    path, so directory rename stays O(1).
15. **Do inline and packed remain distinct regimes?** (§11) Self-hosting makes
    inline objects pack automatically inside metadata extents, which may leave the
    explicit packed regime with nothing to add.
16. **Metadata-tier placement policy** (§5.5.3): more redundancy, maximal spread,
    replicas retained rather than erasure-coded?
17. **Remaining directory semantics** (§5.1): listing huge directories,
    permissions, quotas.
18. **The extent metadata entry: measure it** (§14.1), and decide whether a keyed
    extent's `first_key` fits or forces a bounded prefix.
19. **Compression block size vs index granularity** (§7.1). A 20 KB SSD granule
    hurts compression ratio, and ratio is billable.
20. **Keyed objects** generally — the split-on-overflow B-tree shape of §10 needs
    its own pass.
21. **The packing threshold** (§11) — half of *which* maximum? Presumably ~16 MB
    physical.
22. **Packed-object billing attribution** (§11, §12) — per-object physical size is
    not additive under shared-extent compression.
23. **Billing policy** (§12): garbage, transient double footprint, metadata,
    instantaneous-usage contract, reporting granularity.
24. **Key custody within KS** (§7.3): how the daily master keys are themselves
    protected beyond the unlocking key, and whether customers may supply their own.
25. **EC vs read locality** (§4.3): align the fragment stripe to the granule, or
    keep a full replica?
26. **Compaction policy** (§10) — now partly a pricing decision.
27. **Membership and leader election** (§13).
28. `Nul` and `\0` framing were both listed originally and are folded together
    (§8.2). Confirm no distinction was meant.
29. Does a tombstoned unkeyed record number stay readable as "deleted" forever?
    Reclaiming it would renumber, so presumably never — worth stating
    deliberately.

**Closed** since earlier drafts:

- the unkeyed overwrite/addressing contradiction (§3);
- logical vs physical extent size, which is compression (§6);
- the metadata topology, now three explicit node types (§5);
- whether the DN tier is self-hosted — it is, with a bootstrap root (§5.5);
- whether summary maintenance can run away under self-hosting — it cannot, by
  the tail-only rule (§4.5, §5.5.2);
- whether `count` is exact and cheap — exact and synchronous for unkeyed
  objects, lazy for keyed ones, again by §4.5;
- the supposed 32 MB-vs-large-streams tension, based on a wrong constraint
  (§14.4).
