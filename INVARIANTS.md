# DeLawson — Named invariants, and a term index

Two catalogues, in one file so they cannot drift apart:

- **Part 1** — the invariants, named, grouped by area. Each says what must hold, where it is argued
  in `DESIGN.md`, what enforces it, what breaks without it, and how it could be falsified.
- **Part 2** — the terms, each listing the invariants it appears in.

This exists because of issue #67: "invariant" appears throughout `DESIGN.md`, each stated where it
was discovered and **none stated as a thing to check**. The three bugs of §17.12.1 were not failures
of any one rule; they were failures of the interaction between rules that were each right.

## How to read this

**The name is the key, not a number.** `DESIGN.md` §0 explains why numbering invites renumbering and
silently invalidates references; the same argument applies here with more force, because these names
are meant to be cited *from code and tests*. #67 asked for a numbered list; this uses stable
mnemonic names instead, and `DESIGN.md`'s section numbers as the targets. A test asserting
`EXTENT-OFFSET-WRITE-ONCE` says what it is checking in a way `INV-31` never can.

**Appended at the end of its area, never inserted.** Same discipline as `DESIGN.md`.

**`Argued` is an ordered list, and the first entry is canonical** (Asserted, Bob, 2026-08-19). An
invariant may be discussed in as many places as it turns out to matter; what the catalogue requires
is that **one of them defines it and tries to be the place a reader is sent.** The rest are
consequences, applications and corrections.

⚠️ **Where no section is canonical, that is a finding rather than a formatting problem.** Two entries
below have their statement spread across three sections with no single home —
`ASSIGNED-OFFSET-APPENDS-RECORD-ALIGNED` (§8.1, §8.1.1a-3, §8.2.3) and
`EXTENT-SEALS-BETWEEN-RECORDS`, which is a conclusion nobody states in one place. Those are exactly
the invariants most likely to drift, because a change to one discussion cannot be checked against a
definition that does not exist.

### What counts as an invariant

> A property that must hold of **every reachable state** or **every execution**, and that could in
> principle be **falsified by an observation**.

That rule is doing real work, because `DESIGN.md` §0.1 holds about 200 load-bearing statements and
only some of them belong here. Deliberately excluded:

| Not an invariant | Example | Why |
|---|---|---|
| a **decision** | AEGIS-128X2 is the codec | nothing is violated by choosing differently; it is a fork in the road, not a rule |
| a **parameter** | 32 MB physical extent cap, a 3-day soft-delete window | configuration. §8.2.4 says outright that reading the compressed cap as an invariant is reading it wrongly, since recompression may exceed it |
| a **derivation** | encryption is 0.63 of a core at line rate | a calculation, falsifiable by measurement but not a rule anything obeys |
| a **goal** | throughput over latency | a preference that arbitrates between designs |
| an **open question** | where the ACL is fetched from | §16's business |

⚠️ **The near-misses are the useful part of that table.** A catalogue that admitted parameters would
become a copy of §0.1 and would stop being a checklist. When something here turns out to be a
parameter in disguise, delete it and say so.

### The status columns are the work list

**Enforced** says what makes it true *today*, and **Checked** says what would catch it being false.
`design only` and `not yet` are not embarrassments — they are the reason this file is worth having,
and they are what #67 wants in order to scope the model checking.

---

# Part 1 — The invariants

## Addressing and identity

**`RECORD-NUMBER-STABLE`** — a record number, once assigned, names the same record for ever. It is
never reused, never shifted, and never renumbered by anything the store does to the bytes.
*Argued* §3, §1.0, §8.1.1a · *Enforced* by construction: appends land only at the tail, and update
and delete preserve counts · *Checked* unit tests over seal and stream-seal boundaries (§17.12.1
exercises 269→270); **model check wanted** for concat and compaction
*Breaks* the one property DeLawson exists for. External indexes silently resolve to the wrong
record — silently, which is worse than failing.

**`RECORD-NUMBERS-DENSE`** — the record numbers of an unkeyed object are a gapless sequence.
*Argued* §8.1.1c, §8.1.1a · *Enforced* by construction: no padding, because an append that will not
fit seals the extent short instead · *Checked* unit test; a sweep could verify it over a real object
*Breaks* count-based addressing (§3) degrades to a search, and `LOGICAL-OFFSETS-DENSE` with it.

**`LOGICAL-OFFSETS-DENSE`** — logical offsets within an extent have no holes and no padding.
*Argued* §8.1.1a-6 · *Enforced* by construction in the append path · *Checked* unit test
*Breaks* three mechanisms at once, which is why §8.1.1a-6 is emphatic: the fixed-offset check, the
out-of-order cache, and cumulative byte totals.

**`CANONICAL-OFFSET-SPACE`** — append offsets are assigned in a canonical length-stuffed space, so a
logical offset is invariant under re-framing.
*Argued* §8.2.2 · *Enforced* design only · *Checked* not yet; wants a test that re-frames an extent
and re-resolves every offset
*Breaks* coalescing that changes framing (§8.2.1) silently shifts every offset above it, destroying
the density and stability of `LOGICAL-OFFSETS-DENSE`. §8.2.2 exists because this contradiction was
latent.

**`FIXED-OFFSET-ADVANCES`** — every fixed-offset append advances the logical offset strictly, so a
zero-record append is refused at accept.
*Argued* §8.2.2 · *Enforced* design only · *Checked* unit test, cheap — it is a property of one
request
*Breaks* the fixed-offset exactly-once check cannot distinguish a retry from a new append.

**`EXTENT-IDENTITY-UNIQUE`** — `(SnId, ExtentId, creation second)` never names two different extents,
and a rewritten extent gets a **new** identity.
*Argued* §4.6.3, §8.1.1a-10 · *Enforced* code: `sn` refuses a creation second earlier than one
already used (`Naming::name`) · *Checked* unit test exists (`a_second_may_not_go_backwards`)
*Breaks* stale hints read whatever now occupies the slot instead of failing — §8.1.1a-10 calls id
reuse a corruption vector, and this is what licenses handing out extent-relative rows at all.

**`STREAM-IDENTITY-OWNED`** — a stream is `(DnId, StreamId)`, so exactly one DN owns it by
construction.
*Argued* §4.6.3, §13.1.8 · *Enforced* by construction · *Checked* by construction
*Breaks* who may authorize deleting a stream becomes unanswerable.

**`NO-CLUSTER-NAMES-IN-DATA-PLANE`** — references between levels are node-local: 4 B object→stream,
4 B stream→extent, 6 B extent→instance.
*Argued* §4.6 · *Enforced* design only — the placeholders inline whole records instead, which the
`dn` and `sn` size tests measure and complain about · *Checked* unit tests assert the current sizes
against §4.7's budget (issue #9)
*Breaks* every metadata sizing in §14, which is what decides how many machines an object needs.

**`POSITION-FACTS-IN-REFERRER`** — a fact that depends on *where* a target sits lives in the
referrer, never in the target.
*Argued* §4.6.2 · *Enforced* design only · *Checked* not yet — wants a shared-extent test
*Breaks* sharing. Two referrers at different positions would need different values of one field.

**`TIMESTAMPS-RELATIVE`** — timestamps are 8-byte seconds compared by signed difference, so there is
no absolute epoch and no work proportional to elapsed time.
*Argued* §4.6.3, §1.3 · *Enforced* design only · *Checked* not yet; a test that runs the comparison
across a wrap is cheap and worth having
*Breaks* §1.3's longevity goal — a cluster may lie dormant for eons and must still work.

**`PATH-IS-THE-PORTABLE-NAME`** — the user-visible durable name is the path. `DnId` and `SnId` are
internal and cluster-local.
*Argued* §4.6.3, §13.4.5.3 · *Enforced* design only · *Checked* not yet
*Breaks* georeplication: nothing a user wrote down survives the trip.

**`READ-NAMES-THE-OBJECT`** — every read names the object, whatever else it names.
*Argued* §9.1.3.4, §17.18 · *Enforced* **code**: a range read takes a path and a range, and the
narrowing that would let it name less is deliberately unbuilt until the capability exists (#78) ·
*Checked* the shape is enforced by parsing; the narrowing case cannot be tested until it exists
*Breaks* the request cannot be authorized at all, because the ACL is the object's and §4.6.6's
reverse edge is a count. A stream or extent alone does not identify an object even in principle.

**`OFFSET-SNAPS-FORWARD`** — a start named by a bare offset resolves to the first record beginning at
or after it. Always forward, never nearest.
*Argued* §9.1.3.4 · *Enforced* design only — an offset start returns `unimplemented`, since resolving
it needs a prefix sum §4.5 keeps lazily (§17.18) · *Checked* unit test, and it should assert the
*partition* property over adjacent boundaries, not just one resolution
*Breaks* independently computed chunk boundaries (§9.1.3.1) sometimes overlap and sometimes leave a
gap, and **neither vertex can detect either**.

## Summaries, counts and references

**`TAIL-ONLY-SUMMARIES-EXACT`** — a summary that only ever grows at the tail may be maintained
synchronously to the root; a summary of something that can change anywhere is lazy or absent.
*Argued* §4.5 · *Enforced* design only · *Checked* not yet
*Breaks* the rule that makes count-based addressing possible at all, and the rule that stops
self-hosted summary maintenance from running away (§5.5.2).

**`UNKEYED-COUNT-EXACT`** — an unkeyed object's record count is exact and synchronously maintained.
*Argued* §4.5, §3 · *Enforced* design only; `sn` keeps totals lazily today and says so · *Checked*
unit test
*Breaks* addressing. This is the one summary on the critical path, and §4.5 observes it is the only
one that *can* be exact — not luck, but why the scheme works.

**`REVERSE-EDGE-IS-A-COUNT`** — a target records how many references it has, never which.
*Argued* §4.6.6 · *Enforced* design only · *Checked* not yet
*Breaks* the largest thing the system stores doubles. See §9.1.3.5 for the one question this
genuinely leaves unanswerable, and how a capability answers it instead of an index.

**`COUNTS-ARE-RECOMPUTABLE`** — every reference count is recoverable by a forward mark-and-sweep, so
counts are auditable and repairable rather than authoritative.
*Argued* §4.6.6 · *Enforced* design only · *Checked* **this is the check** — the sweep is the
verifier for `REVERSE-EDGE-IS-A-COUNT`, and it is why a count beats a list
*Breaks* a wrong count becomes unrecoverable, and reclamation is either premature or never.

**`ZERO-REFERENCES-IS-A-BUG`** — an object with no referrers is an orphan, and an orphan is a bug
signal rather than a state to tolerate.
*Argued* §12.6.4 · *Enforced* design only · *Checked* the billing sweep detects it for free
*Breaks* real cost accrues that nobody is charged for, and silently.

## The append path

**`APPLIED-IS-A-PREFIX`** — an append is applied on a machine only once every append before it is
also on disk, so an instance's state is a single watermark.
*Argued* §8.1.1 · *Enforced* code, in the EN's offset handling · *Checked* unit tests
(`offsets_follow_one_another`); **model check wanted** for the interleaving
*Breaks* the bookkeeping stops being four numbers per open extent and becomes a set of outstanding
appends — unaffordable on a CPU-poor tier, and it invalidates the handover argument below.

**`QUORUM-OVERLAPS`** — a quorum is strictly more than half the instances, so any two quorums
intersect.
*Argued* §8.1.1, §8.1.7 · *Enforced* **code: config refusal at startup** (`Extents::validate`) ·
*Checked* unit test exists (`a_quorum_that_does_not_overlap_itself_is_refused`)
*Breaks* two disjoint sets could each hold an append the other never saw, so primary handover would
need a reconciliation protocol instead of getting one from the intersection.

**`DURABLE-IS-QUORUM-WATERMARK`** — durable means the primary knows a quorum of instances have
applied it. Nothing weaker is reported to a writer.
*Argued* §8.1.1 · *Enforced* code, in the EN's quorum counting · *Checked* unit tests over the
counting; **model check wanted**
*Breaks* an acknowledged write can be lost.

**`TRUNCATE-ABOVE-WATERMARK-ON-HANDOVER`** — on primary handover an instance discards everything
above the durable watermark.
*Argued* §8.1.1a · *Enforced* design only (#16.32 lists it as unspecified) · *Checked* **model check
wanted** — this is one of #67's three named protocols
*Breaks* instances diverge, and `INSTANCES-BYTE-IDENTICAL` fails, which erasure coding cannot
survive. Note this is the invariant that forced `TRUNCATION-SEALS`.

**`SEAL-NEVER-BELOW-DURABLE`** — a failure-mode seal takes the (*I* − *Q* + 1)-th smallest accepted
length, which is never below the durable length and is exact when every instance answers.
*Argued* §8.1.7, §8.1.7.1 · *Enforced* config supplies the arithmetic (`discard`, `least_heard`);
the SN-coordinated seal itself is **design only** (#55) · *Checked* derived and checked exhaustively
by hand over six (I, Q) pairs — which §8.1.7.1 notes is a model checker's job done badly, so
**model check wanted**
*Breaks* sealing below the durable length discards an append acknowledged as durable. Sealing
*above* is safe: it is a commitment to repair up to that length, and hole-filling already exists.

**`INSTANCES-BYTE-IDENTICAL`** — the instances of a sealed extent are byte-for-byte equal, including
ciphertext.
*Argued* §8.1.1a, §4.3.2.3 · *Enforced* by construction given the two above · *Checked* §13.1's cold
sweep, exactly rather than probabilistically, using the AEAD tag
*Breaks* erasure reconstruction combines fragments from different machines, so divergence yields
garbage rather than a detectable error.

**`EXTENT-OFFSET-WRITE-ONCE`** — within one extent identity, an offset is written at most once, ever.
*Argued* §7.2.4, §17.17 · *Enforced* **code**: the primary was already safe, since `take` only moves
`end` forward; the secondary was not, and `apply` now refuses different bytes at a written offset
(#83) · *Checked* unit tests exist (`an_offset_holds_one_plaintext_and_refuses_a_second`,
`the_same_bytes_at_the_same_offset_are_a_retry_not_a_clash`); **model check wanted** with handover
interleaved, which is the case no test reaches
*Breaks* the per-block IV repeats over different plaintext under one key: the XOR of the ciphertexts
is the XOR of the plaintexts, and AEGIS's nonce-misuse profile additionally yields tag forgery under
that extent key. Also makes `FIXED-OFFSET-ADVANCES`'s content comparison unsound, since it presumes
an offset has one history.

**`TRUNCATION-SEALS`** — anything that truncates an extent seals it, rather than letting later
appends reuse the vacated offsets.
*Argued* §7.2.4, §17.17 · *Enforced* **code**: `Extents::truncate` does both and they cannot be
called separately, which is the point. ⚠️ Its wire caller does not exist, because handover is
unspecified (§16.32, #55) · *Checked* four unit tests, including that a truncation discarding nothing
still seals and that a refused truncation seals nothing
*Breaks* `EXTENT-OFFSET-WRITE-ONCE`. Chosen over an IV epoch because an epoch needs agreement across
instances in the write path — which §8.1.3 exists to avoid — and a missed bump fails **silently**.

**`SN-NOT-IN-WRITE-PATH`** — the SN names an extent and chooses its instances, then takes no part in
appends to it.
*Argued* §8.1.3 · *Enforced* code: the FE remembers the open extent and asks once per extent ·
*Checked* unit tests (`one_append_asks_for_the_next_extent_and_the_rest_join`), and the SN's request
counters make it observable at runtime
*Breaks* the SN's request rate climbs towards the append rate while the number of extents it creates
stays the same.

**`ONE-UNSEALED-EXTENT-UNKEYED`** — an unkeyed object has exactly one unsealed extent at a time.
*Argued* §8.1.1a-7 · *Enforced* code, by there being one open extent per stream · *Checked* unit
test
*Breaks* the object logical offset stops being well defined, since the prior sum is no longer fixed
— which is why compare-and-append is unkeyed-only.

**`SELF-SEAL-MARK-IS-DURABLE`** — an instance that marks itself as requesting a seal records that
durably, and thereafter fails every further append **it sees**. Nothing clears the mark.
*Argued* §8.1.7.4.1 · *Enforced* design only; the mark belongs in the EN's per-disk extent journal
(§8.1.5) · *Checked* not yet — and the test that matters is a restart, not an append
*Breaks* a restart clears the mark, the instance resumes accepting appends, and it writes different
bytes at offsets its peers have moved past — `EXTENT-OFFSET-WRITE-ONCE` exactly. The obligation is
local on purpose: refusing what arrives, never observing what does not.

**`UNAPPENDABLE-EXTENT-SEALS`** — once *I* − *Q* + 1 instances have marked themselves, no further
append can reach a quorum, so the extent seals.
*Argued* §8.1.7.4, §8.1.7.4.1 · *Enforced* design only · *Checked* the arithmetic is a unit test;
the protocol is **model check wanted** alongside §8.1.7
*Breaks* an extent stays open while being unappendable, so every append fails only after waiting for
a quorum that cannot form.
⚠️ **Nothing implements this one.** It is a *consequence* of `SELF-SEAL-MARK-IS-DURABLE` plus the
quorum, not a threshold anybody detects — read as a rule it would need an observer, with a staleness
window and a race. Listed because it must hold, not because there is code to point at. Note
*I* − *Q* + 1 is `SEAL-NEVER-BELOW-DURABLE`'s order statistic counted from the other end.

**`ASSIGNED-OFFSET-APPENDS-RECORD-ALIGNED`** — an append whose offset the primary assigns must end on
a record boundary. Only a writer that fixed its own offsets may leave a fragment.
*Argued* §8.1, §8.1.1a-3, §8.2.3 · *Enforced* design only · *Checked* unit test
*Breaks* two interleaving appends split each other's records, and neither writer can know where its
bytes landed.

## Records, framing and sealing

**`ONE-BLOCK-ALWAYS-RETURNS`** — a range answer carries at least one block, even when that block
exceeds the budget.
*Argued* §9.1.3.6, §17.18 · *Enforced* **code**: the budget is tested *after* a block is taken, in
the EN's range walk · *Checked* unit test (`a_budget_smaller_than_a_block_still_gets_a_block`) over
all three measures
*Breaks* a caller whose buffer is smaller than one block gets nothing, retries, and gets nothing
again — a livelock wearing back-pressure's clothes, and it never learns it asked for too little.

**`RANGE-ENDS-ON-A-RECORD-BOUNDARY`** — a range answer never stops with a record open, so its
continuation is a record number strictly past where it started.
*Argued* §17.18, §8.2.3 · *Enforced* **code**: stopping requires both the budget met and no open
record · *Checked* unit test (`a_range_does_not_stop_in_the_middle_of_a_record`)
*Breaks* the continuation is the record the request already asked for, so a caller looping on it makes
no progress. This is `INCOMPLETE-RECORD-NOT-DURABLE` on the read path — half a record is not a
record — and the same rule stops both a write being acknowledged and a read being continued.

**`EXTENT-HOLDS-WHOLE-RECORDS`** — an extent contains only whole records. The boundary that matters is
the extent's, not the append's.
*Argued* §8.2.3 rule 1 · *Enforced* design only · *Checked* unit test per framing — and it must cover
`fixed` and `length`, where a lost boundary is unrecoverable rather than merely awkward
*Breaks* a reader of `fixed` or `length` data cannot resynchronize at all.

**`BLOCK-ENDS-ON-BOUNDARY-OR-HOLDS-NONE`** — an append block either ends on a record boundary, or
contains no record boundary at all. Never three records and half of a fourth.
*Argued* §8.2.4 · *Enforced* code in the `laws` client by construction; the primary EN is *supposed*
to check it independently, which is **design only** · *Checked* unit test at both ends
*Breaks* sealing has to split an append and discard half a record, producing an outcome that is
neither success nor failure — the state §8.2.4 removes rather than mitigates.

**`INCOMPLETE-RECORD-NOT-DURABLE`** — an append whose trailing record is incomplete is not durable
until a later append completes that record. This is a second gate in front of the watermark.
*Argued* §8.2.3 · *Enforced* code: the EN's `waiting` table holds appends whose record is unfinished
· *Checked* unit test; **model check wanted**, since it interacts with sealing and with the quorum
*Breaks* an extent can seal mid-record, and half a record is reported as written.

**`EXTENT-SEALS-BETWEEN-RECORDS`** — sealing happens between records, and therefore between blocks.
*Argued* §8.2.4 · *Enforced* follows from the two above · *Checked* **model check wanted** — #67
names this interaction as where the bugs were
*Breaks* nothing is ever cut only so long as this holds; without it the partial-commit case returns.

**`RECORD-FITS-ONE-EXTENT`** — a record's maximum size is an extent's: 4 GB logical.
*Argued* §8.2.4 · *Enforced* design only · *Checked* unit test
*Breaks* `EXTENT-HOLDS-WHOLE-RECORDS` becomes unsatisfiable. Note this cap is **not** the 32 MB
append bound: a writer may build a record out of many appends.

**`TERMINATOR-NOT-IN-DATA`** — data under a terminator framing may not contain its terminator, and
the client is what checks it.
*Argued* §8.2.3 rule 3 · *Enforced* **code, in `laws`** · *Checked* unit test
*Breaks* nothing detectable — below the client a stray `\n` in `lf` data is simply two records. The
check is only meaningful where the information still exists, which is the point.

**`FRAMING-IS-A-REPRESENTATION`** — framing is translatable on read and re-writable by coalescing, so
it is not a durable property of the data.
*Argued* §8.2.1, §8.2 · *Enforced* code: framing is declared at create and travels with every
request · *Checked* unit test over translation
*Breaks* nothing immediately, but it is what `CANONICAL-OFFSET-SPACE` is protecting, and the two must
be read together.

**`SEAL-CLOSES-TOP-DOWN`** — a container is marked closed before anything inside it is sealed: object,
then stream, then extent.
*Argued* §6.4 · *Enforced* design only · *Checked* not yet; the test that matters races an append
against a seal request
*Breaks* sealing the open extent first leaves the level above still entitled to hand out another, so
a concurrent append reopens what was just closed and the seal never converges. Same direction as
§13.1's sync loops, for the same reason.

**`REQUESTED-SEAL-DISCARDS-NOTHING`** — a seal from a request, or from an extent filling, quiesces and
seals where the bytes already stop. Only handover and a broken instance truncate.
*Argued* §6.4.1, §7.2.4 · *Enforced* code for the filling case (`take` seals at `extent.end`); the
requested case is design only · *Checked* unit tests cover the filling case
*Breaks* nothing directly — it is here because "seal" now names two operations that look alike and
differ in exactly this respect, and confusing them means either truncating when nothing needed it or
failing to truncate when something did (`TRUNCATION-SEALS`).

**`SEAL-REQUESTS-ARE-IDEMPOTENT`** — sealing something already sealed succeeds and reports what it
came to.
*Argued* §6.4.1 · *Enforced* design only · *Checked* not yet
*Breaks* retrying becomes unsafe, and the normal-then-failure-mode fallback makes timeouts the
ordinary case rather than an exception — so a non-idempotent seal is a seal that cannot be retried
exactly when it most needs to be.

**`SEALED-EXTENT-IMMUTABLE`** — a sealed extent's base is never rewritten in place. Update and delete
append to its log.
*Argued* §4.2, §8.3 · *Enforced* design only · *Checked* not yet
*Breaks* almost everything cacheable stops being cacheable (§9.1), and `INSTANCES-BYTE-IDENTICAL`
loses its cheap argument.

**`UPDATE-DELETE-PRESERVE-COUNTS`** — update and delete never change any cumulative record count and
never reopen a sealed extent.
*Argued* §8.3, §3 · *Enforced* design only · *Checked* unit test
*Breaks* `RECORD-NUMBER-STABLE`, and with it the asymmetry that makes unkeyed objects addressable and
keyed ones not.

## Keys and crypto

**`KEY-AND-SALT-COVER-ONE-PLAINTEXT`** — one key and one salt or IV may cover exactly one plaintext.
*Argued* §7.2.4 · *Enforced* partly: distinct per day, per SN, per extent, per block, and per wire
segment. The gap is the one `EXTENT-OFFSET-WRITE-ONCE` closes · *Checked* the wire half has a unit
test; the extent half is **not yet checked**
*Breaks* catastrophically rather than gradually — this is the parent rule, and every nonce-derivation
choice in §7.2 and §7.3 is a consequence of it.

**`NO-TWO-SEGMENTS-SHARE-A-NONCE`** — no two sealed wire segments under one key share a nonce.
*Argued* §17.5a · *Enforced* **code**: the nonce is a connection prefix, a direction byte and a
counter (`common/src/seal.rs`) · *Checked* unit test
*Breaks* `KEY-AND-SALT-COVER-ONE-PLAINTEXT` on the wire. Worth citing as the pattern: this is the one
place the discipline is already stated as an invariant and enforced by construction.

**`POLICY-ATTACHES-TO-A-DIRECTORY`** — an object's policy is its nearest ancestor directory's. There
is no per-object ACL and no per-tenant one.
*Argued* §5.9.1a · *Enforced* design only — authorization does not exist yet · *Checked* not yet
*Breaks* per-object policy puts unbounded metadata on the one tier with no ceiling (§14.2), and makes
"who can read what" unanswerable — the failure that made S3 and GCS deprecate object ACLs. Note a
tenant is **not** a unit here: aliases mean an object is reachable under two tenants at once.

**`ALIAS-READS-INTERSECT`** — a read through an alias must satisfy the alias's grant **and** the
owner's ACL, checked at the target.
*Argued* §5.1.1, §5.9.1a · *Enforced* design only · *Checked* not yet — and the test that matters
grants through an alias and then narrows the owner's ACL
*Breaks* an alias becomes a way to escape the owner's policy, which would make aliases a privilege
escalation rather than a view. It is also what makes deny rules unnecessary: intersection is monotone,
so every restriction is expressible as a smaller grant.

**`KEYS-NEVER-WHERE-USER-CODE-RUNS`** — user code runs only on Compute, and keys never exist where
user code runs.
*Argued* §1.5 · *Enforced* design only · *Checked* not yet — and §16 notes the real boundary is
**signed executables**, which makes code signing load-bearing and unspecified (#23)
*Breaks* the single security invariant of the design.

**`MASTER-KEYS-ARE-NEVER-PRUNED`** — the key store is append-only. Every day's master key is retained
for as long as any extent created that day exists.
*Argued* §7.3.2.4, §7.3.3 · *Enforced* design only · *Checked* not yet
*Breaks* a pruned key is every byte written that day, gone permanently — deleted silently, irreversibly,
and at a moment nobody is watching. §7.3's ~1 MB per century is what makes this free, so there is never
a reason to weigh it.

**`RE-KEYING-NEEDS-A-COMMITTED-MEMBERSHIP`** — a member is handed the unlocking key only on the strength
of a membership already committed, and never on one member's unconfirmed view.
*Argued* §7.3.2.2 · *Enforced* design only · *Checked* not yet; the test that matters evicts a member,
loses quorum, and checks the evicted machine is refused
*Breaks* a survivor whose log predates an **eviction** hands the key to exactly the machine the ring
expelled. What makes the safe version possible is that quorums of successive configurations intersect,
so a quorum of any known configuration contains someone who knows about the eviction.

**`UNLOCKING-KEY-IS-GENERATED`** — the unlocking key is generated, never chosen, and the operator
secrets of §7.3.2 are shares of it rather than passwords.
*Argued* §7.3.2.3 · *Enforced* design only · *Checked* not yet — and it is the kind of rule that is
broken by a convenience later, so it wants a check rather than a comment
*Breaks* §7.3.2.3's verifier, stored in the clear so a wrong key fails loudly, becomes an **offline
brute-force oracle** against anything a human composed. Harmless against 256 random bits; the whole
attack against a passphrase.

**`SHARDS-ARE-NOT-STORED-WITH-THE-HARDWARE`** — the k-of-n shards of the unlocking key live apart from
the machines, in separate places and under separate credentials.
*Argued* §7.3.2.5 · *Enforced* nothing can enforce it — it is operational, and listed here **because**
no code will ever check it · *Checked* by audit only
*Breaks* the distinction between "recoverable by its owners after a catastrophe" and "recoverable by
whoever takes the disks". §7.3.2.5 wants the first and the cryptography cannot tell them apart.

**`DISK-KEYS-ONLY-ON-ENS`** — no tier above an EN ever holds a key for data at rest.
*Argued* §7.3.5, §7.3.6 · *Enforced* design only · *Checked* not yet
*Breaks* the blast radius argument of §7.3.1. Note what it does *not* claim: every tier sees some
user data, so the trust boundary is DeLawson code against user code, not tier against tier.

**`EXTENT-KEY-FROM-CREATING-SN`** — an extent's key derives from the `SnId` that *created* it, never
from the SN currently serving it.
*Argued* §7.3.3 · *Enforced* by construction, since the `SnId` is part of the extent's name ·
*Checked* not yet; wants a test that reassigns a stream and reads an old extent
*Breaks* stream reassignment orphans every key for that stream's existing extents — a total,
unrecoverable loss, from a cache keyed on the wrong thing.

**`AEAD-TAG-IS-THE-CHECKSUM`** — every stored block is self-verifying, and the tag is checked before
the block is used as an erasure-repair source.
*Argued* §7.2.1, §4.3.2.3 · *Enforced* design only for extents; the wire layer does it in code ·
*Checked* §13.1's cold sweep is the standing check
*Breaks* corruption propagates into parity, and §4.3.4's observation that erasure coding faithfully
preserves corrupt data becomes unmitigated.

**`AUTH-CODE-CHECKED-ON-EVERY-DISK-READ`** — an EN verifies the tag on every read that comes off a
device. A cache hit is exempt.
*Argued* §9.1.1 · *Enforced* design only · *Checked* not yet
*Breaks* media corruption stops being localized to an instance, and a read cannot be retried against
another instance with the bad copy repaired invisibly.

**`CAPABILITY-IS-MEMBERSHIP-ONLY`** — a hint's capability attests that an extent is inside an object,
and never that the bearer may read it.
*Argued* §9.1.3.5 · *Enforced* design only (#78) · *Checked* unit test: a valid capability must not
survive an ACL that denies
*Breaks* three things at once — ACL changes acquire a revocation lag, a hint pasted into a bug report
becomes a grant, and the expiry stops being merely the hint window.

**`HINT-BOUND-BY-CAPABILITY`** — an extent presented with an object is believed only if a capability
proves the pairing.
*Argued* §9.1.3.5 · *Enforced* design only (#78) · *Checked* unit test with a forged pairing
*Breaks* a caller reads any extent in the cluster by naming it beside an object it may read. §4.6.6
previously claimed putting the stream in the hint closed this; it does not, because an asserted
pairing is only as good as the caller.

## Placement and durability

**`INSTANCE-LIST-ORDER-IS-SEMANTIC`** — the instance list is the persisted output of the placement
solver and must never be sorted or reordered. The first entry is the primary.
*Argued* §5.6.9 · *Enforced* design only (#19) · *Checked* unit test asserts the order survives the
wire (`an_open_extent_is_read_out_of_the_sns_answer`)
*Breaks* the placement decision is destroyed by a tidy-up, and the steady state stops being
verification because there is nothing left to verify against.

**`MEMBERSHIP-IS-NOT-LEADERSHIP`** — KS holds who may vote in a ring; the ring holds who is primary
now. Nothing outside a ring is authoritative about its leader.
*Argued* §5.5.6.2 · *Enforced* design only · *Checked* not yet
*Breaks* failover has to write to a single distinguished component and every caller has to read from
it, so the thing that must beat a timeout (§16.12) depends on the thing that must be most careful. A
stale leadership cache must cost only a redirect.

**`TOPOLOGY-IS-SIGNED-NOT-SECRET`** — the machine list, ring membership and failure-domain hierarchy
are authenticated and readable without the unlock ceremony; only master keys are encrypted under it.
*Argued* §5.5.6.3 · *Enforced* design only · *Checked* not yet
*Breaks* both ways, which is why it is one invariant. Encrypting topology puts a human ceremony in
front of automated recovery. Failing to *sign* it allows a placement attack: a forged domain map lands
four instances in one domain while every rule appears satisfied, and §5.6.8 computes achieved
durability from placements, so nothing downstream notices.

**`PLACEMENT-NEVER-REVISITED`** — placement is decided at write time and never reconsidered.
*Argued* §5.6.7, §5.6.9 · *Enforced* design only · *Checked* not yet
*Breaks* toggling a rule across a threshold causes churn, and `NO-DATA-MOVEMENT-EXCEPT-RANGE-SPLIT`
with it.

**`NO-DATA-MOVEMENT-EXCEPT-RANGE-SPLIT`** — nothing rebalances. Fullness is handled by redirecting
writes; only a DN range split moves anything.
*Argued* §5.11, §13.1.2 · *Enforced* design only · *Checked* not yet
*Breaks* the cost model, and the argument that a hot spot cannot be fixed by moving data.

**`NESTED-NOT-ORTHOGONAL-DOMAINS`** — failure domains nest; they are never orthogonal.
*Argued* §5.6.2 · *Enforced* design only · *Checked* the placement suite of §5.6.10
*Breaks* the durability arithmetic, which counts domains per recovery set at every nesting level.

**`SOLVER-TIMEOUT-DEFERS-NEVER-DROPS`** — a placement solver that runs out of time defers and
retries. It never places while ignoring a rule.
*Argued* §5.6.10 · *Enforced* design only (#20) · *Checked* the hardest-shape suite, which asserts
**latency percentiles rather than success**
*Breaks* durability silently: a placement that violates a domain rule looks identical to one that
does not, until the domain fails.

**`UNENFORCEABLE-RULE-NOT-ENFORCED`** — a placement rule that cannot be satisfied is simply not
enforced, so the minimum viable cluster is one machine.
*Argued* §5.6.7 · *Enforced* design only · *Checked* the single-box harness is the check (#4)
*Breaks* development and testing, not production — but it is an invariant because the alternative is
a system that refuses to start on a laptop.

**`UTILIZATION-BELOW-1-MINUS-1/D`** — cluster utilization stays below 1 − 1/D, where D is the domain
count. This is a durability requirement, not a capacity one.
*Argued* §4.3.4 · *Enforced* design only · *Checked* not yet
*Breaks* a full cluster cannot repair, so loss becomes certain rather than unlikely.

**`REPAIR-EXCEEDS-FAILURE-RATE`** — repair throughput exceeds the arrival rate of failures.
*Argued* §4.3.4.1 · *Enforced* design only · *Checked* not yet; it is a runtime property, so it wants
a standing alert rather than a test
*Breaks* loss is certain. §4.3.4.1 calls this the stability condition, and no code rate rescues a
system below it.

**`EC-NEVER-SPLITS-AN-EXTENT`** — erasure coding groups whole extents. It never splits one into
fragments, so a read never touches EC.
*Argued* §4.3.2.3 · *Enforced* design only · *Checked* not yet
*Breaks* the read path acquires a reconstruction case, which §9.8 spends a whole section removing.

**`EC-GROUP-EQUAL-LENGTH`** — the members of an erasure-coding group are of equal physical length,
and the SN owns the grouping.
*Argued* §4.3.2.3, §13.5.1 · *Enforced* design only · *Checked* not yet
*Breaks* the code cannot be applied. This is also why the SN must know an extent's sealed physical
size.

**`ONE-MEDIA-CLASS-PER-EXTENT`** — an extent has exactly one media class, and so do all of its
instances.
*Argued* §7.4.2 · *Enforced* design only · *Checked* not yet
*Breaks* compression block size is a durable property and EC needs identical bytes, so a mixed-media
extent cannot satisfy both. A fast-read copy must therefore be a tier or a cache, never an instance.

## Deletion, lifetime and replication

**`METADATA-OUTLIVES-ITS-DATA`** — metadata must not predecease the data it describes.
*Argued* §8.4.1d · *Enforced* design only · *Checked* not yet
*Breaks* data exists that nothing can find or attribute — and it makes the heroics window for user
deletes zero, which is where this rule bites in practice.

**`SOFT-DELETE-RESISTS-THE-OPERATOR`** — the soft-delete window is not shortenable by the principal
who requested the delete.
*Argued* §8.4.2 · *Enforced* design only · *Checked* not yet
*Breaks* the window stops being a safeguard against §4.3.4's most serious unmitigated loss mode,
which is wrong-but-authorized deletion.

**`RETAIN-REWRITE-INPUTS-FOR-THE-HINT-WINDOW`** — a rewrite retains its inputs for at least the hint
window.
*Argued* §10.1 · *Enforced* design only · *Checked* not yet; §9.1.3.5's `laws` support is what would
make it testable rather than asserted
*Breaks* the promise made to hint holders, which is now a promise made to **users** rather than an
internal convenience.

**`GEOREP-DIRECTORIES-READ-ONLY`** — a georeplication destination directory is read-only
permanently, and is never promoted in place.
*Argued* §13.4.5.4 · *Enforced* design only · *Checked* not yet
*Breaks* offset-level split brain between the two clusters. Note this is also a mechanism that
resists the operator, per §4.3.4.2.

## Billing

**`BILL-PHYSICAL-NOT-LOGICAL`** — billing counts physical bytes.
*Argued* §12.3 · *Enforced* design only · *Checked* not yet
*Breaks* the 2× slack rule of §12.1, which is what makes the bill track cost.

**`BILLING-READS-ONLY-SNAPSHOTS`** — billing runs over retained snapshots, never over live DN, SN or
EN metadata.
*Argued* §12.8 · *Enforced* design only · *Checked* not yet
*Breaks* reproducibility. A bill that cannot be recomputed from a retained input cannot be defended.

**`REVENUE-INVARIANT-UNDER-REGROUPING`** — total revenue is unchanged by any regrouping of references
among tenants.
*Argued* §12.6.3 · *Enforced* design only · *Checked* **this one is a property to prove, not to
test** — it is arithmetic over the sharing rule
*Breaks* sharing becomes an exploit: colluding tenants cut their combined bill.

## Concurrency and liveness

⚠️ These four are the ones that actually failed. §17.12.1 is the evidence, and all of them are
invisible to a single writer — the run that found them used eight.

**`DECIDE-AND-WAIT-UNDER-ONE-LOCK`** — deciding to wait and enqueueing the waiter are one step under
one lock. The waiters live in the object being waited on.
*Argued* §17.12.1, §17.10 · *Enforced* **code**, in `dn` (waiters in `Object.waiting`) and `fe`
(`Stream.getting`) · *Checked* unit tests exist for the plan states; **model check wanted**, because
the failure needs an interleaving
*Breaks* a missed wakeup. The work finishes in the gap, wakes nobody, and the request waits for
ever. §17.12.1 is explicit that ordering the two operations correctly cannot fix this — only not
having the gap can.

**`NO-REENTRANT-LOCK-ACQUISITION`** — a method called under a shard lock must not take that lock.
*Argued* §17.12.1 · *Enforced* code, by convention and by ⚠️ comments on `look` and `describe` ·
*Checked* not yet — a convention with no check, which is the weakest kind
*Breaks* self-deadlock. Under eight writers it consumed every worker one at a time, and **it looked
like a performance problem and was a liveness one.**

**`WAITERS-ARE-ALWAYS-ANSWERED`** — every parked request is eventually answered, including when the
work it waited on failed.
*Argued* §17.12.1 · *Enforced* code: `give_back`, `gave_up` and `give_up` exist for exactly this ·
*Checked* unit test wanted for the failure paths specifically; **model check wanted**
*Breaks* a request holds a `Reply` nothing will ever send.

**`ONE-REQUEST-IN-FLIGHT-PER-STREAM`** — when many callers need the same next extent or stream, one
asks and the rest join it.
*Argued* §17.10, §17.13.1 · *Enforced* **code** in `fe` (`Plan::Ask` / `Plan::Join`) and `dn`
(`extending`) · *Checked* unit tests exist
*Breaks* the request rate to the tier below climbs towards the append rate while the work it does
stays constant — `SN-NOT-IN-WRITE-PATH` in a different guise.

---

# Part 2 — Term index

For each term: where it is defined, and every invariant that names it.

⚠️ **A term appearing in many invariants is a term whose meaning must be pinned down**, and this is
the index's real use beyond navigation. §8.1.6 records the cost of not doing it: *logical* and
*physical* positions are both 32-bit numbers, the document used "physical" where the code says
"compressed", and they are equal in exactly the case that hides the mistake. The entries below marked
⚠️ are the ones where a reader can still pick the wrong meaning.

| Term | Defined | Invariants naming it |
|---|---|---|
| **append block** | §8.1 | `BLOCK-ENDS-ON-BOUNDARY-OR-HOLDS-NONE`, `ASSIGNED-OFFSET-APPENDS-RECORD-ALIGNED`, `RECORD-FITS-ONE-EXTENT`, `EXTENT-SEALS-BETWEEN-RECORDS`, `APPLIED-IS-A-PREFIX`, `ONE-BLOCK-ALWAYS-RETURNS` |
| **applied** | §8.1.1 | `APPLIED-IS-A-PREFIX`, `DURABLE-IS-QUORUM-WATERMARK`, `SEAL-NEVER-BELOW-DURABLE` |
| **capability** | §9.1.3.5 | `CAPABILITY-IS-MEMBERSHIP-ONLY`, `HINT-BOUND-BY-CAPABILITY`, `KEY-AND-SALT-COVER-ONE-PLAINTEXT` |
| **directory** | §5.1 | `POLICY-ATTACHES-TO-A-DIRECTORY`, `ALIAS-READS-INTERSECT` |
| ⚠️ **compressed position** | §8.1.6 | `KEY-AND-SALT-COVER-ONE-PLAINTEXT`, `EC-GROUP-EQUAL-LENGTH`. *The same space §4.5 and §8.1.1a-2 call **physical**.* |
| **creation second** | §4.6.3 | `EXTENT-IDENTITY-UNIQUE`, `EXTENT-KEY-FROM-CREATING-SN`, `HINT-BOUND-BY-CAPABILITY` |
| **durable** | §8.1.1 | `DURABLE-IS-QUORUM-WATERMARK`, `INCOMPLETE-RECORD-NOT-DURABLE`, `TRUNCATE-ABOVE-WATERMARK-ON-HANDOVER`, `SEAL-NEVER-BELOW-DURABLE` |
| **erasure-coding group** | §4.3.2.3 | `EC-NEVER-SPLITS-AN-EXTENT`, `EC-GROUP-EQUAL-LENGTH`, `INSTANCES-BYTE-IDENTICAL`, `AEAD-TAG-IS-THE-CHECKSUM`, `ONE-MEDIA-CLASS-PER-EXTENT` |
| **extent** | §4.2 | `EXTENT-IDENTITY-UNIQUE`, `EXTENT-HOLDS-WHOLE-RECORDS`, `EXTENT-OFFSET-WRITE-ONCE`, `EXTENT-SEALS-BETWEEN-RECORDS`, `SEALED-EXTENT-IMMUTABLE`, `ONE-UNSEALED-EXTENT-UNKEYED`, `RECORD-FITS-ONE-EXTENT`, `ONE-MEDIA-CLASS-PER-EXTENT`, `EC-NEVER-SPLITS-AN-EXTENT` |
| **extent identity** | §4.6.3 | `EXTENT-IDENTITY-UNIQUE`, `EXTENT-OFFSET-WRITE-ONCE`, `EXTENT-KEY-FROM-CREATING-SN`, `HINT-BOUND-BY-CAPABILITY` |
| **framing** | §8.2 | `FRAMING-IS-A-REPRESENTATION`, `TERMINATOR-NOT-IN-DATA`, `EXTENT-HOLDS-WHOLE-RECORDS`, `CANONICAL-OFFSET-SPACE` |
| **hint** | §8.1.1a-10 | `HINT-BOUND-BY-CAPABILITY`, `CAPABILITY-IS-MEMBERSHIP-ONLY`, `EXTENT-IDENTITY-UNIQUE`, `RETAIN-REWRITE-INPUTS-FOR-THE-HINT-WINDOW` |
| **instance** | §4.3 | `INSTANCES-BYTE-IDENTICAL`, `INSTANCE-LIST-ORDER-IS-SEMANTIC`, `QUORUM-OVERLAPS`, `ONE-MEDIA-CLASS-PER-EXTENT`, `EC-GROUP-EQUAL-LENGTH` |
| **key** (record key) | §3 | `RECORD-NUMBER-STABLE` (by contrast — keyed objects have no positional address), `UPDATE-DELETE-PRESERVE-COUNTS` |
| **key** (cryptographic) | §7.3 | `KEY-AND-SALT-COVER-ONE-PLAINTEXT`, `KEYS-NEVER-WHERE-USER-CODE-RUNS`, `DISK-KEYS-ONLY-ON-ENS`, `EXTENT-KEY-FROM-CREATING-SN`, `NO-TWO-SEGMENTS-SHARE-A-NONCE`, `MASTER-KEYS-ARE-NEVER-PRUNED`, `UNLOCKING-KEY-IS-GENERATED`, `SHARDS-ARE-NOT-STORED-WITH-THE-HARDWARE` |
| ⚠️ **logical offset** | §8.1.6, §8.2.2 | `LOGICAL-OFFSETS-DENSE`, `CANONICAL-OFFSET-SPACE`, `FIXED-OFFSET-ADVANCES`, `OFFSET-SNAPS-FORWARD`, `EXTENT-OFFSET-WRITE-ONCE`, `ONE-UNSEALED-EXTENT-UNKEYED`. *Measured in canonical length-stuffed bytes, not in the stored framing.* |
| **media class** | §7.4 | `ONE-MEDIA-CLASS-PER-EXTENT` |
| **object** | §3 | `READ-NAMES-THE-OBJECT`, `RECORD-NUMBER-STABLE`, `HINT-BOUND-BY-CAPABILITY`, `CAPABILITY-IS-MEMBERSHIP-ONLY`, `ZERO-REFERENCES-IS-A-BUG`, `ONE-UNSEALED-EXTENT-UNKEYED` |
| **path** | §4.6.3 | `PATH-IS-THE-PORTABLE-NAME`, `HINT-BOUND-BY-CAPABILITY`, `READ-NAMES-THE-OBJECT`, `POLICY-ATTACHES-TO-A-DIRECTORY` |
| **failure domain** | §5.6 | `NESTED-NOT-ORTHOGONAL-DOMAINS`, `TOPOLOGY-IS-SIGNED-NOT-SECRET`, `SOLVER-TIMEOUT-DEFERS-NEVER-DROPS` |
| **ring** | §5.7 | `MEMBERSHIP-IS-NOT-LEADERSHIP`, `TOPOLOGY-IS-SIGNED-NOT-SECRET`, `RE-KEYING-NEEDS-A-COMMITTED-MEMBERSHIP` |
| **placement** | §5.6 | `PLACEMENT-NEVER-REVISITED`, `INSTANCE-LIST-ORDER-IS-SEMANTIC`, `NESTED-NOT-ORTHOGONAL-DOMAINS`, `SOLVER-TIMEOUT-DEFERS-NEVER-DROPS`, `UNENFORCEABLE-RULE-NOT-ENFORCED`, `NO-DATA-MOVEMENT-EXCEPT-RANGE-SPLIT` |
| **primary** | §8.1.1 | `APPLIED-IS-A-PREFIX`, `DURABLE-IS-QUORUM-WATERMARK`, `TRUNCATE-ABOVE-WATERMARK-ON-HANDOVER`, `INSTANCE-LIST-ORDER-IS-SEMANTIC`, `TRUNCATION-SEALS` |
| **quorum** | §8.1.1 | `QUORUM-OVERLAPS`, `DURABLE-IS-QUORUM-WATERMARK`, `SEAL-NEVER-BELOW-DURABLE`, `TRUNCATE-ABOVE-WATERMARK-ON-HANDOVER`, `UNAPPENDABLE-EXTENT-SEALS`, `SELF-SEAL-MARK-IS-DURABLE` |
| **record** | §8.2 | `EXTENT-HOLDS-WHOLE-RECORDS`, `BLOCK-ENDS-ON-BOUNDARY-OR-HOLDS-NONE`, `INCOMPLETE-RECORD-NOT-DURABLE`, `RECORD-FITS-ONE-EXTENT`, `TERMINATOR-NOT-IN-DATA`, `OFFSET-SNAPS-FORWARD`, `ASSIGNED-OFFSET-APPENDS-RECORD-ALIGNED`, `EXTENT-SEALS-BETWEEN-RECORDS`, `RANGE-ENDS-ON-A-RECORD-BOUNDARY` |
| **record number** | §3 | `RECORD-NUMBER-STABLE`, `RECORD-NUMBERS-DENSE`, `UNKEYED-COUNT-EXACT`, `UPDATE-DELETE-PRESERVE-COUNTS`, `PATH-IS-THE-PORTABLE-NAME` |
| **reference** | §4.6 | `NO-CLUSTER-NAMES-IN-DATA-PLANE`, `REVERSE-EDGE-IS-A-COUNT`, `COUNTS-ARE-RECOMPUTABLE`, `ZERO-REFERENCES-IS-A-BUG`, `POSITION-FACTS-IN-REFERRER`, `REVENUE-INVARIANT-UNDER-REGROUPING` |
| **seal** (extent) | §6 | `EXTENT-SEALS-BETWEEN-RECORDS`, `SEAL-NEVER-BELOW-DURABLE`, `TRUNCATION-SEALS`, `SEALED-EXTENT-IMMUTABLE`, `RECORD-NUMBERS-DENSE`, `UNAPPENDABLE-EXTENT-SEALS`, `SEAL-CLOSES-TOP-DOWN`, `REQUESTED-SEAL-DISCARDS-NOTHING`, `SEAL-REQUESTS-ARE-IDEMPOTENT` |
| **seal** (wire segment) | §17.5a | `NO-TWO-SEGMENTS-SHARE-A-NONCE`. ⚠️ *An unrelated sense of the word; `common/src/seal.rs` is this one.* |
| **stream** | §4.1 | `STREAM-IDENTITY-OWNED`, `SN-NOT-IN-WRITE-PATH`, `ONE-REQUEST-IN-FLIGHT-PER-STREAM`, `EXTENT-KEY-FROM-CREATING-SN`, `READ-NAMES-THE-OBJECT` |
| **summary** | §4.5 | `TAIL-ONLY-SUMMARIES-EXACT`, `UNKEYED-COUNT-EXACT`, `COUNTS-ARE-RECOMPUTABLE` |
| **watermark** | §8.1.1 | `APPLIED-IS-A-PREFIX`, `DURABLE-IS-QUORUM-WATERMARK`, `TRUNCATE-ABOVE-WATERMARK-ON-HANDOVER`, `SEAL-NEVER-BELOW-DURABLE`, `INCOMPLETE-RECORD-NOT-DURABLE` |

## What the index shows

Three things fall out of the counts, and each is a claim about where to spend effort:

- **`extent`, `record` and `watermark` are the most-referenced terms**, and all three are in the write
  path. That agrees with #67's instinct about which protocols earn a specification.
- **`logical offset` is referenced by six invariants and is the term §8.1.6 says is easiest to
  confuse.** Six invariants resting on a distinction the compiler had to be recruited to enforce is
  the highest-risk entry in this table.
- **The two senses of `seal`** — an extent sealing and a wire segment being sealed — share a word and
  a source file name and have nothing to do with each other. Worth renaming one before the confusion
  costs something.
