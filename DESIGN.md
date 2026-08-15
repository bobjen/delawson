# DeLawson — Design

Status: **draft, pre-implementation.** Nothing here is built. Sections marked
⚠️ are unresolved; they are collected again in §16.

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

### 1.2 The cost target

**Ignoring Compute, ~95% of DeLawson's cost should be disk, electricity, and rent.**
Everything else — server hardware, the metadata tiers, the software — fits in the
remaining ~5%.

This is a **working assumption with room to move**, not a hard requirement: overhead
can be larger if it has to be. But it is worth stating as a goal, because it does real
work below and because the numbers it implies are sensitive to it (§5.3.1):

- It forces **tens to hundreds of disks per EN** (§5.3.1), because that is the only
  way a server's own cost becomes negligible against the storage attached to it.
- It keeps **ENs computationally cheap**, which constrains where the storage pipeline
  runs (§7.1).
- It bounds the **metadata tiers** to a small fraction of the fleet, which §14.5
  checks and finds comfortable.

Billing by physical bytes (§12) is the revenue side of the same coin: if cost is
dominated by bytes on disk, the price should be too.

### 1.3 Longevity

**A cluster may run indefinitely, and may be dormant for geological spans and then be
brought back up.** Nothing in any persistent format may assume otherwise.

That rules out a family of designs that are otherwise natural:

- **No absolute end date.** Fixed-epoch counters that wrap into ambiguity are out;
  §4.6.3 compares timestamps modularly instead, so the interpretation can flip
  indefinitely and only extent *age* is bounded.
- **No work proportional to elapsed time.** Anything that marches forward one unit at a
  time must be lazy instead. Generating one key per elapsed day through a billion-year
  dormancy would mean 3.7 × 10^11 keys and 12 TB in a ring meant to hold a megabyte, so
  the key store is indexed by *active* days only (§7.3).
- **No assumption that the reader and the writer are contemporaries.** Persistent
  structures must be self-describing and versioned, because the code reading a record
  may be unrecognizably newer than the code that wrote it. Format evolution across such
  spans is a requirement, not a maintenance concern.

### 1.4 One security invariant

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

⚠️ Even lazily, an update-heavy full stream could want a log per extent. Lazy
creation makes the append-only case free and does nothing for the update-heavy
case, so open logs per stream need a hard bound with forced compaction when it
is hit. Open logs cost EN replicas and network, not just SN memory, so their
bound is far below the extent count.

### 4.3 Instances

- While open, each instance is a **full replica** on a distinct EN in a distinct
  failure domain, and on a distinct **disk** (§5.3.1). Writes go to all replicas.
  **4 instances**, for the reason below.
- After sealing, the extent is re-encoded to **erasure-coded fragments**,
  reconstructible from any *k* of *n*. Replicas drop once encoding is durable
  and verified.

Because EC operates on already-compressed, already-encrypted bytes (§7),
re-encoding needs no key access and no recompression.

#### 4.3.1 Why four instances and not three

Two operating assumptions, per instance:

| State | Fraction of the time |
|---|---|
| unavailable | 1/1,000 |
| gone, and not yet replaced | 1/10,000 |

The target is **at least 10^-14 reliability per average window between an instance being
lost and being replaced.** Data is lost when every instance is gone at once, so with *n*
instances the loss probability per window is (10^-4)^*n*:

| *n* | Loss per window | |
|---|---|---|
| 3 | 10^-12 | **fails the target by 100×** |
| **4** | **10^-16** | meets it with 100× margin |
| 5 | 10^-20 | overshoots; pays 25% more disk for it |

So four is not a round number — three misses by two orders of magnitude and five buys
margin nobody asked for.

⚠️ **That exponent is entirely a claim about independence.** (10^-4)^4 assumes the four
failures are uncorrelated, which is exactly what the placement rules of §5.6 exist to
deliver. A correlated failure mode — one bad firmware revision, one power domain, two
instances on one machine's disks — collapses the exponent toward 10^-4 and the design
misses its target by ten orders of magnitude. The reliability figure is therefore a
statement about placement discipline, not about disks.

**Availability, separately.** A read needs one instance of four, so it is unavailable
(10^-3)^4 = **10^-12** of the time. A write needs three of four applied (§8.1), which is
unavailable ~**6 × 10^-6** of the time — about 190 seconds per year per extent, and six
orders of magnitude worse than reads. That asymmetry is affordable only because a write
that cannot reach a quorum does not block: the extent is sealed short and a new one opened
(§13), which §6 already makes cheap by allowing extents to seal at any size.

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

Lazy summaries are brought forward by the background loops of §13.2, not by the
write path.

### 4.6 References and reference counts

Both downward links in the hierarchy are **node-local 4-byte references to
reference-counted targets**, and they behave symmetrically:

| Reference | Lives in | Width | Population it must name |
|---|---|---|---|
| object → stream | a DN | **4 B** | streams a DN references |
| stream → extent | an SN | **4 B** | 1.7 × 10^9 extents per SN (§14.1) |
| extent → instance | an SN, within its fan-out | 6 B | a **disk** within an EN, plus a slot (§5.3.1) |
| global node id | cluster-wide, control plane | 4 B | 2^32 nodes (§5.10) |

Nothing in the data plane carries a cluster-wide name. Every reference is an index
into a population its holder already knows, sized to that population.

#### 4.6.1 Sharing at both levels

**An extent may be referenced by several streams, and a stream by several objects.**
The working assumption is **one reference each** (§4.7), so the sizing treats both as
privately owned, but the mechanism exists at both levels out of symmetry.

What it buys:

- **Clone and snapshot cost metadata, not data.** Duplicating an object copies its
  stream list and increments stream refcounts — work proportional to the number of
  streams, not bytes. For a 0.4 PiB stream that is the difference between
  milliseconds and days. Sharing at the extent level does the same one level down.
- **Packed extents** (§11), where one extent holds several small objects, are
  naturally multiply-referenced.

What it costs:

- **Deletion becomes decrement**, with the background loops (§13.2) reclaiming at
  zero. Dropping an object frees nothing by itself.
- ⚠️ **Compaction of a shared extent cannot rewrite in place**, because the other
  referrers did not ask for it. It needs copy-on-write: compaction produces a private
  extent for the referrer that triggered it and leaves the shared one alone. So a
  clone's first write costs more than its steady state.
- ⚠️ **Key derivation must fix on the creating stream** (§7.3.3), since a shared
  extent belongs to several streams and "the stream id" is otherwise ambiguous.
- ⚠️ **Billing attribution is a policy question** (§12). Charging a clone and its
  original the full footprint double-bills; charging neither loses revenue; charging
  the creator makes clones free and holds the original hostage.

#### 4.6.2 Sharing forces position-dependent fields into the referrer

Sharing makes a structural rule unavoidable, and it is worth naming because it
explains the layout in §4:

> A shared thing may only carry facts true of it **wherever it appears**. Anything
> that depends on *where* it appears belongs to the reference, not the target.

Two objects sharing a stream will disagree about that stream's starting record
number — it might be the first stream of one object and the thousandth of another.
So the starting record number lives in the object's stream-list entry, never in the
stream record; likewise an extent's starting record number lives in the stream's
extent-list entry, and a keyed `first_key` in the referring entry too. Position-free
facts — physical size, key generation, instance locations — live with the target,
where they are shared for free.

This is why the §4 diagram puts the cumulative counts on the *entries* rather than in
the objects they point at. That was arbitrary-looking before sharing existed; it is
forced now.

#### 4.6.3 Identity: ownership is part of the name

Each level is **owned** by the tier above the bytes, and the owner is part of the
identifier rather than a field beside it:

| Thing | Owner | Full identifier |
|---|---|---|
| Stream | a DN | `(DnId, StreamId)` |
| Extent | an SN | `(SnId, ExtentId, creation second)` |

Extents are not guids. A full extent id is 12 bytes against a guid's 16, and **inside
the owning SN the `SnId` is implicit, so it costs 8** — exactly the extent-id field
already in the record (§5.10.4).

When an extent is shared across DNs, that happens **through two different streams**;
sharing is always mediated by the stream level, so an extent never has two owners.

**Why the creation timestamp.** It makes an `ExtentId` reusable without ambiguity. A
stale reference carrying an old second cannot match a new extent that reused the
number, which is the §5.10.4 naming-lifecycle problem solved by incarnation rather
than by refcounting — and solved with a clock instead of a persisted counter, so it
survives restarts for free and is legible when debugging.

Two safety requirements come with it:

- ⚠️ **An `ExtentId` must not be reused within the same second.** Cycle time is no
  help here — at any plausible creation rate the 2^32 id space takes days to years to
  wrap — so the hazard is purely an allocator handing the same number out twice in one
  second. That argues for a FIFO or least-recently-used free list rather than
  lowest-free allocation.
- ⚠️ **The issued second must never go backward**, or an NTP correction could
  manufacture a collision. The SN should keep the last second it issued in its Raft
  state and refuse to regress.

##### The timestamp is 8 bytes of seconds, compared modularly

The daily SN-master key that encrypted an extent is **selected by its creation
timestamp** (§7.3), so the timestamp is load-bearing for decryption as well as for
identity. That makes its width a longevity decision, not a formatting one.

**8 bytes of seconds.** Inside the owning SN the id field is then `ExtentId` (4) plus
second (8) = **12 B**; the full form is 16 B, the same as a guid but structured and
meaningful.

Read as an unsigned counter from a fixed epoch, 8 bytes spans 585 billion years — 42×
the age of the universe — and then wraps, which would be an absolute doomsday: every
extent in the system becomes ambiguous at the same instant, and no amount of rewriting
beforehand helps the ones written just before it.

**So timestamps are compared by signed difference, not by magnitude.** The same bits are
interpreted as signed or unsigned according to era — equivalently, ordering is decided
by `(a − b)` taken as a signed 64-bit value. This is the technique behind TCP sequence
numbers and Linux's `time_after()`, and it changes the constraint qualitatively:

| | Absolute unsigned epoch | Signed-difference comparison |
|---|---|---|
| Cluster lifetime | 585 billion years, then dead | **unbounded** |
| The real constraint | replace everything before the epoch wraps | **no live extent older than ~292 billion years** (2^63 s) |
| Failure mode | everything breaks at once, unfixably | a single over-age extent, fixed by rewriting it |

That is the reconciliation between wanting 8 bytes and wanting a cluster that may run
essentially forever: the window stops being a doomsday and becomes a **rolling maximum
extent age** of ~292 billion years, 21× the age of the universe, which the age-driven
rewrite sweep (§13.2) satisfies trivially.

**The interpretation flips back and forth indefinitely.** Each flip is not a one-time
extension that eventually runs out; it is a periodic reinterpretation of the same 8
bytes, repeatable without limit. The only standing obligation is that live extents stay
inside one window — a bound on extent *age*, never on cluster age.

Two rules follow, and both are the kind that lie dormant for eons and then fail:

- ⚠️ **Never compare raw timestamps by magnitude.** Any `a < b` on the unsigned value is
  a latent bug. Only signed differences are meaningful.
- ⚠️ **Modular comparison is not a total order**, so a timestamp cannot be used naively
  as a sort key or B-tree key — it is only consistent among values within one window.
  Extents are keyed by `(SnId, ExtentId)` with the timestamp as a disambiguator rather
  than a sort key, so this is currently safe; it would stop being safe the moment
  something wanted to index by creation time.

Two safety rules from before still apply: an `ExtentId` must not be reused within one
second, which argues for FIFO or LRU rather than lowest-free allocation; and the issued
second must never go backward, so the SN keeps the last one it issued in its Raft state
and refuses to regress.

##### The same discipline applies to SCNs

Transaction sequence numbers (§5.7) have the identical problem on a **much shorter
wall-clock window**, because an SCN advances per transaction rather than per second. The
signed half-window, in years:

| Transactions/s | 6-byte | 8-byte | 10-byte |
|---|---|---|---|
| 10^3 | 4,500 | 2.9 × 10^8 | 1.9 × 10^13 |
| 10^6 | **4.5** | 2.9 × 10^5 | 1.9 × 10^10 |
| 10^9 | **0.0045** | 292 | 1.9 × 10^7 |

At a million transactions per second, 6 bytes is 4.5 years and 8 bytes is 290,000 —
where the same widths measured in seconds were geological. So width matters far more
here, and the comparison discipline is not optional.

What rescues it is that **the live SCN range is deliberately tiny**: SCNs are expected
to be valid for only ~10 minutes. Only open consistent reads and retained versions need
to be comparable, so capping validity caps the range, and the window bounds *version
age* rather than cluster age — exactly as with extents.

**SCNs are never persisted in cold data.** They live only in the short-lived replicated
extents — the open base and its log — and are **dropped when those are coalesced into a
sealed, erasure-coded extent** (§10). So an SCN's lifetime is bounded by how long an
extent stays hot, which is short by construction rather than by policy. This is the
strongest of the three reasons for the cap: the others bound the SCN range by choosing
to, while this one makes a long-lived SCN impossible to write down.

It also removes a per-record cost from the tier that holds essentially all the bytes. An
8-byte SCN beside a 100-byte record would be ~8% of logical volume — on the order of 80
PB across an exabyte — before compression; delta-encoding monotonic counters would claw
much of that back, but not storing them is exactly zero and depends on nothing.

⚠️ The API consequence is worth stating plainly: **consistent read has a horizon.** A
snapshot can be held for minutes, but there is no querying an object "as of last
Tuesday", because once coalesced there is one version and no SCN to select by. Any
time-travel feature would need a different mechanism entirely.

That cap is a large part of *why* the 10-minute figure exists. Headroom of a 6-byte SCN:

| SCN validity | at 10^6 tx/s | at 10^9 tx/s |
|---|---|---|
| **10 minutes** | 234,562× | **235×** |
| 1 hour | 39,094× | 39× |
| 1 day | 1,629× | **2× — exhausted** |

At a day of validity and a billion transactions a second, 6 bytes is gone. At ten
minutes it holds comfortably even there. **So the 10-minute cap is what makes a narrow
SCN viable at high transaction rates**, and the width and the cap have to be chosen
together.

Note the 10 minutes is **bracketed from both sides**, which is what makes it look like a
real design point rather than a round number: §5.7 needs a consistent read to last *long
enough* to write a checkpoint, while the SCN range and the coalescing lifetime both need
it *short enough*. The same shape as the extent record bracketed by reference width from
below and fleet size from above (§4.6.4), and the physical extent limit bracketed by seek
amortization and metadata volume (§6).

##### The hot tier absorbs the complexity

SCNs are the fourth thing to land on one side of the same line, which is worth naming as
a through-line rather than a coincidence:

| | Hot: open, replicated, mutable | Cold: sealed, erasure-coded, immutable |
|---|---|---|
| Durability | 4 replicas | *k*-of-*n* fragments |
| Structure | base **plus log** | base only |
| Versioning | **SCNs** | none |
| Mutability | appendable | fixed forever |

Every expensive or intricate mechanism lives in the small hot tier; the tier holding
essentially all the bytes stays simple, cheap, and immutable. Coalescing is the
transition, and it is where each of these is given up at once.

⚠️ SCN width and scope are undesigned. A Raft log index is a natural per-ring SCN and
needs no separate counter, but §2 rules out cross-object transactions while an object
spans many rings, so what an SCN is scoped to is not yet settled.

##### Two questions this settles

- **Who may authorize deleting a stream** (§13.1.5). The `DnId` *is* part of the
  stream's name, so a stream has exactly one owning DN by construction, not by
  convention. That is the safe form recommended there, obtained for free.
- **Key derivation is now stable against reassignment** (§7.3.3). Extent keys derive
  from the daily SN-master key of the SN that *created* the extent, and both the `SnId`
  and the day are immutable parts of the extent's name. Moving a stream to another ring
  cannot orphan its keys. Note the consequence: whoever decrypts must derive from the
  extent's own `SnId`, not from the SN currently serving the stream.
- The creation second also **subsumes the separate day index** §7.3.3 budgeted, since
  the day is the second divided by 86,400. One field, two jobs.

⚠️ Streams have no creation timestamp in this scheme, so `(DnId, StreamId)` is exposed
to exactly the stale-reference hazard the extent timestamp prevents. Stream churn is
far lower, so the pressure is milder — but the hazard is the same shape, and the
asymmetry looks unintentional. Worth confirming.

#### 4.6.4 Reference width brackets the record size from below

The 4-byte extent reference caps a machine at 2^32 extents, and the extent budget of
§14.1 is 171 GB, so:

| Extent record | Extents per SN | Share of 2^32 |
|---|---|---|
| 40 B | 4.27 × 10^9 | **99%** |
| 60 B | 2.84 × 10^9 | 66% |
| 100 B — the budget | 1.71 × 10^9 | 40% |
| 200 B | 0.85 × 10^9 | 20% |

**So the extent record cannot shrink below ~40 B** without a machine holding more
extents than 4 bytes can name. That is a floor, and §14.1.2 gives the ceiling from
fleet size — the record is bracketed from both directions, which is a more
comfortable position than a single guess.

At the 100 B budget there is 2.5× headroom, and the itemized ~60 B (§5.10.4) still
leaves 1.5×. Worth knowing that shrinking records is not free: past a point it trades
metadata density for reference width.

### 4.7 Working budgets

Every number below is **a guess, to be replaced by measurement**. They are collected
here so the sizing arguments have one source, and so that refining one of them is a
single edit rather than an archaeology exercise.

Treat them as **budgets to spend against, not totals to re-derive.** The itemized
content of a record will drift upward as flags, timestamps and checksums accumulate;
that is what the headroom is for, and re-baselining every downstream calculation on
each increment produces numbers obsolete by the next revision.

| Budget | Value | Where it is used |
|---|---|---|
| Extent metadata record | **100 B** | §14.1 — itemized at ~60 B so far (§5.10.4) |
| Stream metadata record | **200 B** | §14.1 |
| User object metadata record | **300 B** | §14.2 |
| References per extent, and per stream | **1** | §4.6.1 — more is supported but expected to be rare |
| Extent / stream reference width | **4 B**, node-local | §4.6 |
| Extent physical size | 32 MB compressed | §6 |
| Extent logical size | ≤ 4 GB | §6 |
| Instances per open extent | 4 | §4.3 |
| SN / DN machine RAM | 256 GB | §14.1 |
| SN RAM split | **1/3 streams, 2/3 extents** | §14.1 — varies wildly by tenant |
| Placement slack | 4/3 | §5.6.1 |
| Fan-out per machine | ~20,000 | §5.10 |
| Compression ratio, when assumed | 4× | §12, §14.2 |

The SN RAM split deserves its own warning: **it will vary wildly between tenants and
in production**, because it encodes an assumed mix of object sizes (§14.1.1). It has
to be a soft allocation that rebalances, not a partition fixed at build time.

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

#### 5.3.1 SNs place on disks, not on ENs

**An SN chooses a disk, not a machine.** It tracks which instances sit on each disk
of each EN it can reach, so an instance reference names `(EN, disk, slot)` rather
than `(EN, offset)`.

That costs nothing, because the disk byte is paid for by the locator shrinking. A
slot index within one disk needs 3 bytes where a byte offset within a whole EN
needed 4:

| Component | Width | Covers |
|---|---|---|
| local EN index | 2 B | ~20,000 ENs in the fan-out set (§5.10) |
| disk index | 1 B | 256 disks — a 1 PB EN of 20 TB disks has 50 |
| slot index | 3 B | 16.7 × 10^6 slots — a 100 TB disk holds 3.0 × 10^6 extents |
| **Instance reference** | **6 B** | unchanged from the previous per-EN encoding |

**The cost target sets the disk count.** Per §1.2, server hardware must be ≤5% of an
EN's total cost. With *S* the server cost and *D* a disk's lifetime cost — the drive
plus its share of electricity and rent — that requires:

> *S* / (*S* + *nD*) ≤ 0.05, i.e. **n ≥ 19·(S/D)**

| *S/D* — server cost in disk-equivalents | Disks required | Within a 1-byte index? |
|---|---|---|
| 3 | 57 | yes |
| 6 | 114 | yes |
| 8 | 152 | yes |
| 13 | 247 | barely |
| 20 | 380 | **no** |

So "tens or hundreds" is right, and which end depends entirely on how a bare storage
server prices against a disk. A cheap server against expensive disks lands in the
tens; anything like a conventional server needs well over a hundred.

**How hard this bites depends on the overhead target**, which §1.2 holds loosely. For
an overhead fraction *f*, the requirement is *n* ≥ ((1−*f*)/*f*)·(*S/D*), and a 1-byte
disk index caps *n* at 256:

| Overhead *f* | Disks needed | Largest *S/D* a 1-byte index allows |
|---|---|---|
| 5% | 19 × (*S/D*) | 13.5 |
| 10% | 9 × (*S/D*) | 28.4 |
| 15% | 5.7 × (*S/D*) | 45.2 |

So this does couple a metadata format decision to the business model — but softly.
At a strict 5% a bare server may cost at most ~13 disk-equivalents before 1 byte of
disk index stops sufficing; letting overhead drift to 10% doubles that headroom to
~28, which is comfortably beyond any plausible server-to-disk ratio. **The 1-byte
index is safe unless both the 5% target is held strictly and servers are expensive
relative to disks.** Worth watching rather than worrying about.

Why bother tracking disks at all: **a disk is the innermost failure domain.** One
disk dying should cost the instances on that disk and nothing else. Without
per-disk tracking, a single failed disk is indistinguishable from a failed EN, and
repair would rebuild the machine's whole complement instead of one disk's worth.
Per-disk placement also stops two instances of one extent landing on two disks of
the same machine, which would satisfy a naive "different disk" rule while sharing a
power supply, a controller, and a kernel.

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
million partitions at ~300 B is ~300 MB, comfortably one stream — so **the root
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

#### 5.6.3 Choosing a disk: weighted random, then reject

Domain independence says where an instance may *not* go. It does not say which of the
survivors to pick, and picking badly fills disks unevenly — which wastes capacity, since
a full disk is unusable even while its neighbours are half empty.

**So placement is proportional to free space.** The mechanism reuses the sync loop that
already exists:

1. The once-a-minute disk-liveness sync (§13.1) also reports **fullness per disk**.
2. A once-a-minute summary builds a **cumulative table of free bytes** over all disks the
   SN can currently reach.
3. To place an instance, pick a **uniformly random byte** in the total free space and
   **binary search** the table for the disk containing it.
4. If the placement rules already exclude that disk's domain, **retry**.

The cost is negligible. An SN reaching 407 ENs of 150 disks tracks ~61,000 disks — a
477 KB table, rebuilt each minute, and 16 comparisons per placement. At *G*=64 it is
~244,000 disks, 1.9 MB, and 18 comparisons.

Retries are few, because the rules only exclude domains already used. With the six domains
slack requires for four instances (§5.6.1):

| Placing instance | Fraction allowed | Expected picks |
|---|---|---|
| 1st | 1.00 | 1.0 |
| 2nd | 0.83 | 1.2 |
| 3rd | 0.67 | 1.5 |
| 4th | 0.50 | 2.0 |

**Rejection sampling is exactly right here, not an approximation.** Sampling from a
free-space-weighted distribution and discarding disallowed draws yields precisely the
free-space-weighted distribution *conditioned on* the allowed set — which is the
distribution wanted. No correction term is needed.

Two further properties worth noting:

- **It self-corrects.** A newly added, empty disk has a large share of free bytes and so
  attracts writes until it matches its neighbours; a nearly full one is chosen almost
  never. Fill levels converge without any explicit rebalancing.
- **It is indifferent to the rule set**, so the same machinery serves erasure coding,
  whose placement rules differ from 4-instance replication (§4.3) — only the rejection
  predicate changes.

⚠️ Weighting and constraints do interact: if one domain is markedly emptier than the rest,
weighted picks concentrate there and are rejected more often. The retry count stays
bounded but rises, and the effect is worst exactly when the cluster is lopsided — after
adding a rack, say. Worth measuring rather than assuming.

#### 5.6.4 What nesting costs

**Planned disruptions must be serialized.** Software deployments and large
planned outages cannot run at the same time.

⚠️ The placement arithmetic above is verified; this operational constraint is
asserted from experience and its precise argument should be written out before
anyone relies on it. The mechanism appears to be that nesting collapses every
domain type onto one hierarchy, so "at most one domain intentionally down"
becomes a single global budget shared by every kind of planned work, rather than
one budget per orthogonal dimension.

#### 5.6.5 A concession that is a known defect

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

The same ten minutes is also an *upper* bound from a different direction — it keeps
the live SCN range small enough for a narrow sequence number (§4.6.3) — so the figure
is squeezed from both sides rather than chosen.

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

#### 5.8.1 Rings are a concurrency unit, not a capacity unit

**A ring serves many streams.** *P* is chosen for cores and cost amortization, and
has nothing to do with how much a stream may hold — the two are independent, and an
earlier draft of this section wrongly tied them together by assuming one stream per
ring (§14.4).

What *P* does bound is **failover cost**, because an elected log-only member must
materialize everything its ring serves before it can act. At 500 MB/s, a ring
holding a 1/*P* share of a machine's 171 GB extent budget takes:

| Primary rings/machine | Ring's share of the extent budget | Materialize time |
|---|---|---|
| 1 | 171 GB | ~5.7 min |
| 16 | 10.7 GB | ~21 s |
| 64 | 2.7 GB | ~5.3 s |
| 256 | 667 MB | ~1.3 s |

So many rings per machine is what makes §5.7.1's expensive tier — an ordinary
election won by an unmaterialized member — tolerable rather than frightening. It
also makes checkpoints cheap, which is exactly what the 2×-RAM optimization in §5.7
needs: holding a consistent read for seconds is a far easier promise than holding
one for ten minutes.

Stream size, separately, is bounded by the machine's whole extent budget — see
§14.1.

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

The instance bytes per SN machine come from §14.1. A machine's extent budget of
171 GB at ~100 B holds **1.7 × 10^9 extent records**, but it is *primary* for only
about a third of the rings it materializes (§5.7) — the other roles do not talk to
ENs — so it drives ~5.7 × 10^8 extents, which at 32 MB and 4 instances is
**~76 PB of EN capacity to reach**. Note this is **independent of P**: more rings per
machine means proportionally smaller streams, so the extent count a machine holds is
set by its RAM, not by how that RAM is divided.

| EN capacity | *F/G* | *F* at *G*=4 | *G*=16 | *G*=64 |
|---|---|---|---|---|
| 100 TB | 764 | 3,054 | 12,217 | 48,867 |
| 1 PB | 76 | 306 | 1,222 | 4,887 |
| 3 PB | 25 | 102 | 407 | 1,629 |

*G* is the free choice, and it is the one that matters: **sharing is what gives
repair and balance somewhere to go.** With *G*=1 each EN answers to a single SN,
which is a static partition — cheap in connections, but a dead EN can only be
replaced from a pool of one SN's making, and a reassigned stream finds no ENs it
may talk to.

The table's real lesson: **EN capacity buys sharing.** At 100 TB per EN, *G*=16
needs 18,000 connections and is out of reach; at 1 PB per EN the same sharing costs
~1,800 connections and is comfortable. Fan-out policy and EN sizing are one
decision, not two.

Helpfully, **the cost target pushes the same way.** §5.3.1 derives tens to hundreds
of disks per EN from the requirement that server hardware be ≤5% of cost; at 150
disks of 20 TB that is a 3 PB EN, where *G*=16 costs only ~407 connections and *G*=64
still only ~1,600. Two independent arguments — reliability wanting sharing, economics
wanting few servers per byte — converge on large ENs.

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

#### 5.10.4 Bounded fan-out means 2-byte references, not 4-byte

The 32-bit identifiers are for **cluster-wide naming** — topology, configuration,
the control plane. They need not appear in per-extent metadata at all.

Because an SN can reach at most ~20,000 ENs, and 2 bytes addresses 65,536, an
instance's EN can be named by a **neighborhood-local index**. The global 32-bit id
appears exactly once, in the SN's fan-out table: 20,000 entries × 4 B = **78 KB**,
once per table rather than once per reference.

| Field | Bytes |
|---|---|
| extent id — `ExtentId` (4) + creation second (8), `SnId` implicit (§4.6.3) | 12 |
| starting record number | 8 |
| cumulative logical bytes | 8 |
| cumulative physical bytes | 8 |
| key generation and flags | 2 |
| 4 × instance reference (2 B local EN index + 1 B disk + 3 B slot) | 24 |
| **Total** | **62 B** |

The day index §7.3.3 needs for key derivation is not a separate field: the creation
second already carries it.

The same argument applies to any reference whose fan-out is bounded: DN and FE
references from an SN are likewise ≤20,000, so they too are 2-byte local indices.
**Global ids are control-plane; local indices are data-plane.**

The intra-EN locator can likely shrink too. If it is a slot index rather than a byte
offset, 3 bytes addresses 2^24 slots, which at 32 MB per extent covers a 512 TiB EN
— bringing the entry to **56 B**. Worth confirming against the EN sizing that §5.10.1
already ties to fan-out.

Two compounding effects, because §14.1 shows the stream list grows as the *square*
of entry size: the 8 bytes saved by local indices frees ~10 GB of RAM on every SN
machine, *and* takes ~22% off the largest metadata structure in the system. Larger
streams also mean fewer of them, which shortens every stream-list descent.

The itemized total is not a new baseline. **§14.1 keeps 100 B as the budget**,
because flags, timestamps and fields not yet identified will consume the headroom;
what the local-index encoding buys is room to add them, not a smaller number to
quote.

⚠️ **The cost is a naming-lifecycle problem.** A local index is only meaningful
relative to a table, so:

- The fan-out table is critical state and must live in the ring's Raft state
  (§4.4), snapshotted and logged like everything else.
- **A local index must not be reused while any extent entry still references it.**
  Retiring an EN and admitting a replacement into the same slot would silently
  re-point every extent that named it. That needs reference counting, or a
  generation number in the slot, or append-only slot allocation with compaction.
- §5.10.3's mutable fan-out becomes harder: reassigning a stream to another ring
  means its entries' local indices refer to the wrong table. The cheap fix is to
  make the table **travel with the stream** — 78 KB is nothing next to the extent
  list — which argues for the index namespace being per-stream rather than per-ring.

⚠️ Per-stream or per-ring index namespace is undecided, and it is the same
stable-identity-versus-mutable-assignment question as §5.10.3 and §7.3.3. All three
now want deciding together.

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

**The cost target constrains where these stages run.** §1.2 requires ENs to be
computationally cheap — a server carrying 150 disks cannot also carry the CPU to
compress at line rate, since that CPU would blow the 5% budget it is supposed to fit
inside. **So compression should not run on ENs.** Encryption is a weaker case, since
AES-NI is cheap enough to plausibly survive the budget, which matters because §7.3.1
has ENs caching keys and therefore doing crypto. That asymmetry — compression
elsewhere, encryption possibly on the EN — is a constraint on §5.9.2's open question,
not an answer to it.

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

| Active days | Keys | Size |
|---|---|---|
| 1 year | 365 | 12 KB |
| 10 years | 3,650 | 117 KB |
| 100 years | 36,500 | **1.2 MB** |

A century of master keys is about a megabyte, and — unlike the tenant-keyed scheme
this replaces — the figure does not depend on how many customers or objects exist.
Old daily keys must be kept as long as any extent encrypted under them survives,
which this makes free.

⚠️ **Keys are generated lazily, and the store is indexed by *active* day, not elapsed
day.** §1.3 allows a cluster to lie dormant for geological spans; marching one key
forward per elapsed day through a billion-year dormancy would mean 3.7 × 10^11 keys and
12 TB in a ring sized for a megabyte. A day with no extent creations needs no key, so
the store must be sparse and populated on demand.

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

Deriving an extent's key needs the *correct* daily SN-master key — the right day, and
the right SN. **The extent's own identifier supplies both, at no extra cost.** A full
extent id is `(SnId, ExtentId, creation second)` (§4.6.3), so:

- the **day** is the creation second divided by 86,400 — no separate day-index field;
- the **SN scope** is the `SnId` of the SN that *created* the extent.

Both are immutable parts of the extent's name, which closes a trap an earlier draft
flagged: if derivation had keyed on *which ring currently serves the stream*, then
reassigning a stream (§13) would orphan every key for its existing extents. Naming the
creating SN makes derivation stable against reassignment by construction.

⚠️ The consequence is that **whoever decrypts must derive from the extent's own `SnId`,
not from the SN serving it.** After a reassignment those differ, and a cache keyed on
the serving SN would fetch the wrong master key.

#### 7.3.4 What this hierarchy does not give

⚠️ **There is no cryptographic tenant isolation.** Keys are scoped by day and SN,
not by customer, so two tenants' extents on one SN for one day derive from the same
SN-master key. Isolation rests on FE authorization (§5.9) and on keys never
reaching user code (§1.4) — not on separate key material. That is a coherent
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

#### 8.1.1 Accepted, applied, durable

An extent has a **primary EN** among the ENs holding its instances, and that primary runs
the append protocol. Three states, and the distinctions matter:

| State | Means | Scope |
|---|---|---|
| **accepted** | a machine has received the append | one machine |
| **applied** | it *and every append before it* are on disk on that machine | one machine |
| **durable** | the primary EN knows **3 of 4** instances have applied it | the extent |

**Applied is a prefix property, not a per-append flag.** An append is applied only once
all its predecessors are also on disk, so each instance's state collapses to a single
watermark and durability is the watermark reached by at least three of the four. That is
what makes the bookkeeping cheap enough for a tier that must stay CPU-poor (§1.2): the
primary tracks four numbers per open extent, not a set of outstanding appends.

**Three of four is a quorum, and that is load-bearing.** Any two 3-subsets of a 4-set
overlap in at least two members, so a new primary that polls any three instances is
guaranteed to see every durable append. Primary handover needs no separate reconciliation
protocol — the intersection does it. Had durability been 2 of 4, two disjoint pairs could
each hold a "durable" append the other had never seen.

#### 8.1.2 The quorum forces broadcast, not a daisy chain

The primary must send each append to all three secondaries **in parallel**. Chain
replication — primary → S1 → S2 → S3 — would defeat the quorum entirely.

The reason is that a quorum only buys straggler tolerance if the writes are *issued*
concurrently. Under broadcast, durability arrives when the second-fastest of three
secondaries applies, so one slow node costs nothing. Under a chain, reaching three of four
means passing *through* the first two secondaries in sequence, so every straggler is on the
critical path and a stall at S1 blocks S2 and S3 as well:

| | Mean latency to durable (iid apply times, mean 1) |
|---|---|
| Broadcast — 2nd fastest of 3 | 1/3 + 1/2 = **0.83** |
| Chain — first two in sequence | 1 + 1 = **2.00** |

The tail matters more than the mean. With 1% of nodes running 10× slow, a chain is hit by a
straggler 2.0% of the time, while broadcast needs *two* of three to be slow and is hit
0.03% of the time — a **67× rarer** slow tail. Given that 3-of-4 exists precisely to avoid
waiting for the slowest instance, pairing it with a topology that serializes them would be
self-defeating.

What broadcast costs is egress *distribution*, not egress total. For a 4 MB append block
both put 12 MB on the wire; the chain spreads it one hop per node while broadcast puts all
12 MB on the primary's uplink. Fleet-wide that evens out, because every EN is primary for
some extents and secondary for others — so the concentration bounds per-extent burst
throughput rather than aggregate capacity.

#### 8.1.3 The SN is not in the write path

Worth stating explicitly, because it is easy to assume otherwise: **the SN places
instances and records where they are, then gets out of the way.** The append protocol runs
between the client or FE and the primary EN, and the SN is not consulted per write.

Two things depend on this:

- **The SN's memory budget survives.** §14.1 has one SN mapping ~76 PB of EN capacity from
  256 GB of RAM, which is only possible because it never touches a byte of it.
- **The cost target survives.** §1.2 needs writes to be handled by the tier that already
  owns the disks, not by an extra hop through a metadata machine.

The SN does learn of the outcome, but lazily: extent size and byte totals are the mid-tree
summaries §4.5 keeps lazy, and the sync loops of §13.1 reconcile the rest.

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
  §13.2 — the exact, synchronous summary is the unkeyed record count, which
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

### 13.1 Sync loops

Reality diverges from metadata, and three loops reconcile it. Their cadences differ
by orders of magnitude, and the principle behind that is worth stating:

> **Sync frequency follows the rate at which reality can diverge, not the importance
> of the data.**

| Loop | Cadence | Why that cadence |
|---|---|---|
| SN → EN, disk liveness | **~1 minute** | The fastest twitch. Disks fail without warning, and until the SN knows, it is placing on and reading from something that is gone. |
| SN → EN, warm instance sync | frequent | Only what changed, so small at any fleet size (§13.1.6). |
| SN → EN, cold instance sync | slow sweep | Full enumeration, to catch what the warm path missed. Divergence comes from real hardware loss and silent corruption, so it must happen — but not urgently, since liveness already caught the loud failures. |
| DN → SN | **quite slow** | No hardware failure can cause a DN's view of a stream to go wrong. Only software bugs and lost messages can, so this is a correctness audit rather than a failure detector. |

All of them are **driven by the upper tier** — the SN asks the EN, the DN asks the SN.
The tier holding the metadata is the one that knows what ought to exist.

The DN→SN case is the instructive one. A DN's reference to a stream does not depend on
any hardware that can independently fail — the stream's existence is guaranteed by its
SN ring (§5.7), not by a disk. So there is nothing for a fast loop to catch. An SN's
reference to an instance, by contrast, depends on a specific spinning disk that can
die between one second and the next.

#### 13.1.1 One message per pair, driven from above

**Sync is per pair — SN/EN, DN/SN — not per disk or per object.** One message covers
the whole pair and is *internally* structured by the substructure below it. That keeps
the message count proportional to the fan-out (§5.10) rather than to fan-out × disks,
which at 150 disks per EN is a 150-fold difference in chatter for no gain.

**The upper tier drives.** The SN asks the EN; the DN asks the SN. The tier holding the
metadata is the one that knows what ought to exist.

#### 13.1.2 Cold sync bucketing, negotiated

A cold sync enumerates everything, and everything is large, so it is split in three
escalating steps. **Both sync pairs use the same mechanism**, with the middle step
instantiated on whatever natural substructure that pair has:

| Step | Split by | SN → EN | DN → SN |
|---|---|---|---|
| 1 | nothing — one message | small pairs | small pairs |
| 2 | the natural substructure | **sets of disks** | **sets of objects** |
| 3 | hash within one unit of it | **hash within a disk** | **hash within an object** |

The parallel is exact: a disk groups an EN's instances the way an object groups a DN's
streams, and in both cases step 3 exists for the case where a single unit of that
grouping is itself too large.

Both need step 3 in practice:

- **SN → EN.** An SN drives ~2.3 × 10^9 instances (§5.10.1); at a 3 PB EN with *G*=16
  that is ~5.6 million per pair, **~112 MB** at 20 B each. One whole 20 TB disk digests
  to ~12 MB, so a pair reaches step 3 only when the SN owns a large fraction of one
  disk — a low sharing factor or unusually large disks. At *G*=16 the per-disk share is
  ~0.75 MB and step 2 suffices.
- **DN → SN.** An SN holds ~4.3 × 10^8 stream records (§14.1). How much lands in one
  pair depends entirely on how widely an SN's streams are spread across DNs:

  | DNs per SN | Streams per pair | Digest |
  |---|---|---|
  | 20,000 | 21,000 | 0.43 MB — one message |
  | 1,000 | 427,000 | 8.5 MB — step 2 |
  | 100 | 4.3 × 10^6 | 85 MB — step 2, well split |

  And step 3 is unavoidable at the top end regardless: **a maximal object has ~4.1 × 10^6
  streams, ~82 MB**, so one object alone exceeds any sane message and must be split by
  hash.

**Bucket count is negotiated, and only upward:**

- The **upper tier estimates** the bucket count from what it knows — it has the count
  already, so the estimate is free and usually right.
- The **lower tier may raise it in its first response** if that estimate is too low, and
  the upper tier retries with the larger count.

The asymmetry is the interesting part, and it is not arbitrary. The lower tier may
legitimately hold *more* than the upper tier knows about — orphans, which are a large
part of what the sync is for (§13.1.5) — so only it can discover the estimate was short.
Allowing increases only makes the negotiation monotonic and terminating: at worst one
extra round trip, with no possibility of two parties oscillating between sizes.

#### 13.1.3 What each sync is actually for

The two loops have different centres of gravity, and it is worth being precise because
an earlier draft of this section had the DN→SN emphasis backwards.

**DN → SN is mostly about confirming deletes.** Its dominant job is assuring that the SN
really did delete the streams the DN told it to. That is the common case and the
expensive one.

**An orphan stream wastes space permanently**, and the leak is far larger than the
record itself. Nothing else in the system will ever notice it:

- no DN asks about it, because no object references it;
- the SN cannot tell it is unreferenced — it just holds a stream record;
- and its extents still carry a reference from it (§4.6.1), so extent reclamation will
  not touch them either.

So an orphan stream **pins everything beneath it forever**. A 200-byte leaked stream
record at the §14.1 stream size holds down ~1.3 × 10^7 extents, four instances each:
**~1.74 PB of disk, pinned by 200 bytes of metadata** — a leverage ratio of ~10^13.
That is the whole justification for the loop existing, and it is why "quite slow" must
still mean "reliably completes".

#### 13.1.4 Why there are exactly two sync loops

Because **only cross-tier references can drift.** Within one tier a Raft ring makes
updates atomic, so its internal references are consistent by construction; across a
tier boundary there is a message that can be lost.

| Reference | Crosses a tier? | Can drift? | Sync loop |
|---|---|---|---|
| object → stream | DN → SN | **yes** | DN → SN |
| stream → extent | inside one SN ring | no — atomic under Raft | none needed |
| extent → instance | SN → EN | **yes** | SN → EN |

That is the complete explanation for the loop inventory: one per tier boundary, and none
in the middle. It also reframes reference counts — **a refcount is a cache of a truth
held by the referrer**, and where the referrer is in another tier, the cache can be
wrong. The sync is what reconciles it.

#### 13.1.5 Divergence runs in both directions

| Divergence | Meaning | Action |
|---|---|---|
| SN holds a stream no DN references | **orphan stream** — a failed delete or abandoned create | reclaim; this is the common case (§13.1.3) |
| EN holds an instance the SN never mentions | **orphan instance** — a failed delete, an abandoned write, or state a ring lost and rebuilt | reclaim the space |
| SN expects an instance the EN lacks | lost to disk failure or corruption | repair — re-replicate or reconstruct |
| DN references a stream the SN lacks | **data loss**, not mere unavailability — the ring was the sole authority | escalate; nothing routine fixes this |

The last row is rare rather than routine: it needs a whole ring to lose state, which
§5.6 exists to prevent. But it is the one divergence with no remedy, so the loop should
raise an alarm rather than attempt a repair.

⚠️ **Deleting on the strength of one DN's silence is dangerous, and sharing makes it
worse.** Streams may be referenced by objects on *different* DNs (§4.6.1), so one DN
failing to mention a stream does not mean nobody references it. A wrong deletion here is
unrecoverable — it destroys live customer data — so the safe forms are:

1. **Each stream records an owning DN**, the single authority for its reference set, and
   only that DN's sync may authorize deletion. Cross-DN sharing then means a shared
   stream still has one home.
2. Or the SN must hear from *every* DN that could reference the stream before deleting,
   which is a quorum problem across an unbounded set.

(1) is much the simpler, and it is the recommendation, but it constrains how stream
sharing works and needs settling alongside §4.6.1.

#### 13.1.6 Warm sync, and why cadence is affordable

Cold sync is the full enumeration above. **Warm sync covers only what changed**, so it
is small regardless of fleet size and can run far more often. The cold sweep exists to
catch what the warm path missed — silent corruption, dropped messages, state lost in a
ring failure — and its cadence is a bandwidth decision:

| Full cold sweep every | Digest bandwidth per SN |
|---|---|
| hour | ~13 MB/s |
| day | ~0.5 MB/s |
| week | ~0.08 MB/s |

Even hourly is affordable in bulk terms. What makes it affordable is that it is a
digest exchange rather than a per-instance protocol: the same sweep as individual RPCs
would be tens of thousands per second.

⚠️ Better still, compare a **hash tree per disk** and descend only where the digests
differ, making steady-state cost proportional to *divergence* rather than to instance
count. Divergence is normally zero, so that is the difference between a full sweep and
almost nothing. Undesigned; this is where it belongs.

The liveness loop has no such problem: one message per EN per minute across the fan-out
is a few hundred messages a second, and each is a small per-disk status vector.

### 13.2 Background loops

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
| **Age-driven rewrite of over-age extents** | §4.6.3 | Compulsory: no live extent may be older than the ~292-billion-year comparison window, or its identity and key selection become ambiguous |
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

### 14.1 SN: a machine's RAM, split between streams and extents

An SN serves stream and extent records from RAM, and that is what bounds both. Using
the budgets of §4.7 — 256 GB per machine, split **1/3 for stream records at 200 B
and 2/3 for extent records at 100 B** — and taking the worst case for capacity,
`compressed = physical`, so an extent's logical span equals its 32 MB physical size:

| | Budget | Record | Capacity |
|---|---|---|---|
| Stream records | 85 GB | 200 B | **4.3 × 10^8 streams** |
| Extent records | 171 GB | 100 B | **1.7 × 10^9 extents** |

A stream is bounded above by the whole extent budget: one stream monopolizing a
machine reaches 1.7 × 10^9 × 32 MB = **51 PiB**. Any lower cap is policy, not a
constraint. Note this is a property of the *machine*, not of the ring — §5.8.1
corrects an earlier draft that tied stream size to rings per machine.

Compression relaxes it further: at a 4× ratio the same extent count spans 4× the
logical bytes.

#### 14.1.1 The split encodes an assumed workload mix

The two rows above imply an average of just **4 extents per stream** — 128 MB of
logical data per stream. That is not a claim about how a large object is striped; it
is a claim about the *population*, and it is consistent with §1's expectation of
enormous numbers of tiny objects, each holding one stream with a handful of extents.

The average and the maximum differ by nine orders of magnitude, and both are real:

| Stream size | Extents | What has this shape |
|---|---|---|
| 128 MB | 4 | the average, dominated by tiny objects |
| 0.4 PiB | 1.3 × 10^7 | a stream of a large object |
| 51 PiB | 1.7 × 10^9 | one stream owning a machine's whole extent budget |

**So the split cannot be a static partition.** A tenant with a billion tiny objects
would exhaust the stream budget while the extent budget sat idle; a tenant with one
enormous object would do the reverse. Bob's own caveat is that the ratio varies
wildly by tenant even in production, so it has to be a soft allocation over one pool,
rebalanced as the mix shifts.

⚠️ Soft allocation raises a question a fixed split does not: what happens when a
machine's mix shifts far enough that it can no longer hold what it has been assigned?
Streams must migrate, which is §16's reassignment problem arriving from a new
direction — driven by workload drift rather than failure.

#### 14.1.2 Sensitivity

The ceiling scales inversely with record size. **The §4.7 budgets stay the planning
figures** — 100 B and 200 B — even though the fields itemized so far reach only ~60 B
(§5.10.4), because flags, timestamps and fields nobody has identified yet will consume
the headroom. Budget and spend deliberately; do not re-baseline on each increment.

| Extent record | Extents per SN | One-stream ceiling | Share of the 4-byte reference (§4.6.4) |
|---|---|---|---|
| 40 B | 4.3 × 10^9 | 128 PiB | 99% — the floor |
| 60 B — itemized so far | 2.8 × 10^9 | 85 PiB | 66% |
| **100 B — the budget** | 1.7 × 10^9 | 51 PiB | 40% |
| 200 B — a long `first_key` inlined | 0.85 × 10^9 | 26 PiB | 20% |

What the extent record controls is **how many machines a given object needs**: at
200 B rather than 100 B every stream costs twice the SN memory, so the fleet doubles
for the same data. It does not affect the object's stream list, which depends on the
stream record and the chosen stream size instead.

⚠️ Inlining a generous keyed `first_key` is therefore a fleet-sizing decision, not a
metadata-format detail. That is the argument for a bounded key prefix with
disambiguation-by-read.

⚠️ The 200 B stream budget is the **stream record on its SN**. The object's *stream-list
entry* on a DN is a different and smaller thing — a 4-byte reference, a starting record
number, cumulative byte totals — and has no budget yet. Sizings that quote a stream-list
size use 200 B as a deliberately loose upper bound; a real figure would shrink them
severalfold.

### 14.2 DN: unbounded in object count, disk-backed

The SN's problem is bounding a single stream. The DN's is the opposite: nothing
bounds the number of objects.

At the §4.7 budget of **300 B per user object**:

| Objects | Logical | At 4× compression |
|---|---|---|
| 10^9 | 300 GB | 75 GB |
| 10^12 | **300 TB** | **75 TB** |

The ratio that matters is against the object's own data. **Metadata equals data at a
payload of 300 B**, so:

| Object payload | Metadata as a multiple of data |
|---|---|
| 30 B | **10×** |
| 300 B | 1× |
| 3 KB | 0.1× |

Below a few hundred bytes an object costs more to describe than to store, and §1
expects those in enormous numbers — so the DN tier can exceed the EN tier in
*logical* size. That is why DN state is disk-backed and cached, not resident (§5.1).

In *physical* bytes it is far less alarming, because self-hosting (§5.5) runs that
metadata through the same pipeline as everything else. Object metadata is unusually
compressible — sibling names share prefixes, timestamps cluster, flavor and framing
repeat across every object in a directory — so at a conservative 4× and 6+3 erasure
coding, 300 logical bytes becomes ~75 physical and ~113 billed. Against a bare
4-replica copy at 1,200 bytes, **self-hosting makes the metadata tier roughly an
order of magnitude cheaper than storing it uncompressed and replicated.**

The asymmetry is still worth stating plainly: **SN state is bounded per stream
by construction; DN state is bounded by nothing.** Only the DN tier has to be a
paging, caching store — and, since §5.5, only the DN tier is stored in DeLawson
itself.

### 14.3 What actually binds

Two independent ceilings on one object:

- **Record number**: 2^64 records, from the 8-byte record number.
- **Capacity**: if an object's stream list is allowed ~2^31 entries at the §14.1
  one-stream ceiling of 51 PiB, then 2^31 × 2^56 B ≈ **2^87 bytes**.

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

A third draft tied stream size to *P*, the rings per machine, by assuming one stream
per ring. Rings serve many streams; *P* is a concurrency and failover parameter and
has nothing to do with stream capacity, which is bounded by the machine's extent
budget (§5.8.1, §14.1).

What survives from all of this is the observation that shrinking extents moves
addressing information up from the extent's sparse index into the stream's extent
list. That trade is real; it just lands well inside budget.

---

### 14.5 Do the metadata tiers fit in the cost budget?

§1.2 wants everything but disk, electricity and rent inside ~5%. The metadata tiers
are the part of that most likely to surprise, so it is worth a check.

Take an EN of 150 disks with a server costing 8 disk-equivalents (§5.3.1): 158
disk-equivalents, holding 3 PB. One SN machine drives ~76 PB of EN capacity (§5.10.1),
so it accounts for ~25 such ENs — call it 1.5 server-costs for the extra RAM, or 12
disk-equivalents.

| | Disk-equivalents |
|---|---|
| 25 ENs | 3,950 |
| 1 SN machine | 12 |
| **SN tier share** | **0.3%** |

Comfortable, with two orders of magnitude of headroom against the 5% budget. The
reason is structural: an SN's memory maps a fixed *count* of extents, and each extent
stands for 32 MB of disk, so the metadata tier scales with the storage it describes
divided by 32 million.

⚠️ The DN tier does not follow that logic. DN cost scales with **object count**, which
nothing bounds (§14.2), so a tenant with a trillion tiny objects buys DN capacity
rather than disk. The 0.2% above is the SN side only, and the DN side cannot be
computed until object-count expectations are pinned. This is the same asymmetry as
§14.2 and §14.4, arriving in the cost model.

---

## 15. Testing at scale

The sizing claims of §14 are about metadata volume — B-trees of 10^9 entries,
checkpoints of hundreds of GB, failover that must materialize them. Provisioning
exabytes of disk to test any of that would be absurd. It is also unnecessary.

### 15.1 Self-concatenation

Because extents are reference-counted (§4.6.1), **an unkeyed stream can be
concatenated to itself.** Each concatenation appends its own extent list to its end,
which doubles both the number of extent references in the list and the reference
count on every original extent. The bytes on disk do not change at all.

So *n* operations produce 2^n references over constant physical data:

| Doublings | Extent references | Logical size | Physical data |
|---|---|---|---|
| 10 | 1,024 | 32 GB | 32 MB |
| 20 | 1.0 × 10^6 | 32 TB | 32 MB |
| 31 | 2.1 × 10^9 | **64 PiB** | **32 MB** |

Thirty-one doublings from a single extent reaches a stream larger than §14.1's
ceiling, on 32 MB of real data. It works because unkeyed appends land at the end and
never disturb existing cumulative counts (§3), so records *N*..2*N*−1 legitimately
repeat records 0..*N*−1 and every addressing invariant still holds.

### 15.2 What it exercises, and what it does not

Genuinely tested:

- B-tree descent, split, and memory footprint at 10^9 entries
- The Raft snapshot/log/state-machine cycle (§4.4) on a full-sized checkpoint
- Materialize time and failover (§5.8.1) — the numbers there become measurable
- Reference counting under extreme fan-in, and every path that must handle it
- **Copy-on-write compaction** (§4.6.1), because *everything* is shared, so no
  compaction may ever rewrite in place. A bug there is impossible to miss.

Not tested — worth being explicit, since the technique is tempting to over-trust:

- Fan-out breadth and placement diversity (§5.10): all references point at the same
  few ENs, so neighborhoods, domain spreading, and repair remain untested
- Disk capacity and bandwidth behaviour under real load
- Compaction across *distinct* data, and compression-ratio variety

It is a metadata-scale test, not a data-scale test. Both are needed; this one is
nearly free.

**The reference width already accommodates this.** References are 4 bytes (§4.6), so
2^32 references are nameable and 31 doublings from a single extent sit just inside the
limit. A 32nd doubling would overflow — which makes the test method reach almost
exactly as far as the design does, and no further. Convenient rather than
coincidental: both limits come from the same 4-byte field.

The same technique works one level up. **Concatenating an unkeyed object to itself
doubles its stream references**, which exercises the DN-side stream list and its
paging path (§14.2) at scales no real object would reach during development.

---

## 16. Open questions

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
6. **Soft allocation of SN memory between streams and extents** (§14.1.1). The 1/3
   : 2/3 split encodes a workload mix that varies wildly by tenant, so it must
   rebalance — and when a machine's mix drifts past what it was assigned, streams
   have to migrate, which is reassignment arriving from workload drift rather than
   failure.
7. **Shared-extent and shared-stream consequences** (§4.6.1): copy-on-write
   compaction, and billing attribution between a clone and its original.
8. **The DN-side stream-list entry has no budget** (§14.1.2). Sizings currently use
   the 200 B stream record as a loose upper bound; a real figure would shrink every
   stream-list number severalfold.
9. **Digest-based instance confirmation** (§13.1.6). A per-disk hash tree would make
   steady-state cost proportional to divergence rather than instance count, and
   divergence is normally zero.
10. **The maximum sync message size** (§13.1.2), which sets how much bucketing either
    sync pair needs.
11. **How widely an SN's streams spread across DNs** (§13.1.2). It swings the DN→SN
    digest from 0.4 MB to 85 MB per pair, and it is not determined by anything decided
    so far.
12. **Should streams carry a creation timestamp too?** (§4.6.3) Bob's expectation is
    yes, not yet thought through. Two asymmetries make it a different problem from the
    extent case rather than the same one: a stream is not itself encrypted, so a stream
    timestamp would be for id-reuse safety only and would not select a key; but
    "rewriting" a stream to escape a wrap means re-identifying it in every object that
    references it, where rewriting an extent is something compaction does anyway. So the
    wrap obligation is cheap for extents and expensive for streams — an argument for
    giving streams a *wider* timestamp rather than an equal one.
13. **Placement retry behaviour in a lopsided cluster** (§5.6.3). Free-space weighting
    and domain constraints interact: a markedly emptier domain attracts weighted picks and
    then rejects them, which is worst right after adding capacity. Bounded, but worth
    measuring.
14. **Primary EN selection and handover** (§8.1.1). The 3-of-4 intersection makes handover
    safe, but who chooses the primary, and how a new one is appointed when it fails, is
    undesigned.
15. **SCN width, scope, and validity window** (§4.6.3). The width and the 10-minute cap
    must be chosen together; a Raft log index is a natural per-ring counter, but what an
    SCN is scoped to is unsettled while objects span many rings.
16. **Expected object count** (§14.5). DN cost scales with object count rather than
    bytes, so the DN tier's share of the cost budget cannot be computed until this is
    pinned — it is the one part of §1.2 still unquantified.
17. **Why nesting forces serialized planned disruptions** (§5.6.4) — the
    arithmetic is verified but this operational constraint is asserted, and it
    determines how the cluster is actually operated.
18. **Choosing P, the rings per machine** (§5.8.1). Not a tuning knob: it trades
    core utilization and failover speed against stream-list size, and it multiplies
    with the unmeasured extent-entry size — the stream list grows as the *square*
    of that entry size across the plausible range.
19. **Transactions and long consistent reads** (§5.7). Getting them would drop the
    ring from 3× RAM to 2×, the single largest cost saving identified so far. §5.8
    makes the required read duration much shorter than ten minutes, so this looks
    more attainable than when first raised.
20. **Raft ring sizes and slack** (§5.6.1): how many members for an SN ring, for a
    DN ring, and does the 4/3 default hold for both?
21. **Where the live counters live** (§5.5.2). §4.5 shows the recursion cannot run
    away, so this is now a write-amplification question only: keep the
    authoritative count on the SN and let the DN cache it lazily, or accept one
    metadata write per data write.
22. **Bounding open logs per stream** (§4.2). RAM bounds the extent list; nothing
    bounds the logs, and each costs EN replicas and network.
23. **Background loop scheduling** (§13.2): prioritisation and rate limiting
    against foreground traffic, and whether the rewrite-shaped work — compaction,
    re-encoding, re-keying, garbage reclamation — is scheduled as one pass or many.
24. **Where the live counters live** (§5.5.2). §4.5 shows the recursion cannot run
    away, so this is now a write-amplification question only: keep the
    authoritative count on the SN and let the DN cache it lazily, or accept one
    metadata write per data write.
25. **The metadata key** (§5.5.4): `(parent dir id, name)` recommended over full
    path, so directory rename stays O(1).
26. **Do inline and packed remain distinct regimes?** (§11) Self-hosting makes
    inline objects pack automatically inside metadata extents, which may leave the
    explicit packed regime with nothing to add.
27. **Metadata-tier placement policy** (§5.5.3): more redundancy, maximal spread,
    replicas retained rather than erasure-coded?
28. **Remaining directory semantics** (§5.1): listing huge directories,
    permissions, quotas.
29. **The keyed `first_key` in the extent entry** (§14.1, §5.10.4). The only field
    whose size is unknown, and the stream list grows as the *square* of the entry —
    so a bounded key prefix with disambiguation-by-read looks strongly preferable to
    inlining an arbitrary key. Also: confirm the 60 B estimate by measurement.
30. **Local-index namespace and lifecycle** (§5.10.4): per-stream or per-ring, and
    how a slot is retired without silently re-pointing the extents that name it.
31. **Compression block size vs index granularity** (§7.1). A 20 KB SSD granule
    hurts compression ratio, and ratio is billable.
32. **Keyed objects** generally — the split-on-overflow B-tree shape of §10 needs
    its own pass.
33. **The packing threshold** (§11) — half of *which* maximum? Presumably ~16 MB
    physical.
34. **Packed-object billing attribution** (§11, §12) — per-object physical size is
    not additive under shared-extent compression.
35. **Billing policy** (§12): garbage, transient double footprint, metadata,
    instantaneous-usage contract, reporting granularity.
36. **Key custody within KS** (§7.3): how the daily master keys are themselves
    protected beyond the unlocking key, and whether customers may supply their own.
37. **EC vs read locality** (§4.3): align the fragment stripe to the granule, or
    keep a full replica?
38. **Compaction policy** (§10) — now partly a pricing decision.
39. **Membership and leader election** (§13).
40. `Nul` and `\0` framing were both listed originally and are folded together
    (§8.2). Confirm no distinction was meant.
41. Does a tombstoned unkeyed record number stay readable as "deleted" forever?
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
  (§14.4);
- who may authorize deleting a stream — the `DnId` is part of the stream's name, so
  there is exactly one owning DN by construction (§4.6.3);
- whether key derivation survives stream reassignment — it does, because the extent
  names its creating SN and its creation day (§4.6.3, §7.3.3).
