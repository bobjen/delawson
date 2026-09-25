# ObjectInfo and BatchObjectInfo benchmarks

Use the existing `laws bench-create` and `laws bench-info` commands. The standard
workload is **20 million objects**, created once in batches of **1,000**, with
**16 clients × 16 requests in flight**. Measure ObjectInfo (batch 1) and
BatchObjectInfo (batch 1,000), each for three 5-second trials. `bench-create`
must use `--trials 1`: its default of three creates 60 million objects.

## Keep the cluster alive until measurements finish

**Spawn the whole build/start/create/measure/stop sequence detached.** Do not
start the cluster under a tool command with an EXIT cleanup trap and then try
to benchmark it in a later command: returning or timing out runs that trap and
stops the cluster before the benchmark. Merely backgrounding a child, or using
a tool's asynchronous session, is not sufficient protection from process-group
cleanup. Disconnect stdin, redirect both output streams, and start a new session.
The detached runner alone owns the cleanup trap, which runs after measurements
(or on failure). Poll its log and status file from separate tool calls.

From the repository root, this launcher uses a fresh temporary run directory
and leaves logs and a numeric exit status:

```bash
python3 - <<'PYRUN'
import pathlib, subprocess, tempfile
root = pathlib.Path.cwd()
output = pathlib.Path(tempfile.mkdtemp(prefix="delawson-object-bench-"))
runner = output / "run.sh"
runner.write_text(r'''#!/usr/bin/env bash
set -euo pipefail
cd "$1"
output=$2
finish() {
    result=$?
    trap - EXIT
    ./scripts/stop-laws --run-dir "$output/run" > "$output/stop.log" 2>&1 || true
    echo "$result" > "$output/status"
    exit "$result"
}
trap finish EXIT
./scripts/start-laws --release --run-dir "$output/run"
client=(./target/release/laws --config "$output/run/cluster-effective.toml" --to FE0)
"${client[@]}" bench-create --objects 20000000 --batch 1000 --clients 16 --depth 16 --trials 1
"${client[@]}" bench-info --objects 20000000 --batch 1 --clients 16 --depth 16 --seconds 5 --trials 3 --trial 1
"${client[@]}" bench-info --objects 20000000 --batch 1000 --clients 16 --depth 16 --seconds 5 --trials 3 --trial 1
''')
with (output / "output.log").open("w") as log:
    child = subprocess.Popen(
        ["bash", str(runner), str(root), str(output)],
        stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
(output / "pid").write_text(str(child.pid) + "\n")
print(f"PID {child.pid}; results in {output}")
PYRUN
```

Use `tail` on the printed directory's `output.log`. Completion is indicated by
`status` (0 means success); it is written **after cluster cleanup**. `stop.log`
records cleanup separately. Do not launch another cluster until cleanup finishes:
temporary run directories still use the same configured TCP ports. Do not run
this while another development cluster is using those ports.

`start-laws` waits for the registrar to report a healthy known Raft leader after
starting/unlocking the KS members and before starting DNs. Preserve this gate:
a ready file only proves the listener is up. Older launchers without it can fail
DN0 registration with `Raft has no known Raft leader`; check historical launchers
before running. If startup fails, allow cleanup to finish before another attempt.
Never interpret failed startup or nonexistent-object lookups as a performance result.

## Interpreting results

Require all 20 million creates to complete and every lookup to be both answered
and found. An entry limit can produce impressive asking rates with zero answers.
Report the three-trial mean, range, and spread. `bench-info` also retains one
response from client zero at the end of each trial and, after timing, reports its
serialized byte size (including the four-byte message frame) and returned object,
stream, extent, and instance counts. These are sampled response rows, not
transport-batch sizes or aggregate metadata counts for omitted rows. It does
not estimate compressed size. The five seconds cover issuing
requests; draining outstanding requests can make elapsed time longer.

Current `bench-info` defaults request detail flags 23 (streams, extents,
instances, and counts). Older results using plain rows (`--flags 0`) are not
directly comparable. For a regression comparison, run this same procedure on
both commits, sequentially, with matching configuration and flags. Do not replace
it with a smaller object count, batch size, or the one-object variant.

`scripts/bench-laws` measures ping, not these object benchmarks.

## 2026-09-24: segmented message batches

Sequential release runs using the procedure above, default detail flags 23,
20 million objects, 16 clients × depth 16. Before: `8729d5b`; after: `8e0fc76`.
Every lookup was answered and found in both runs.

| Workload | Before (objects/sec) | After (objects/sec) | Throughput change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 (one trial) | 1,862,568 | 1,803,837 | -3.2% |
| ObjectInfo (three-trial mean) | 88,315 | 77,976 | -11.7% |
| BatchObjectInfo, batch 1,000 (three-trial mean) | 515,596 | 381,962 | -25.9% |

ObjectInfo ranges: before 87,456–89,262 (2.0% spread), after 77,473–78,479
(1.3%). BatchObjectInfo ranges: before 511,694–521,410 (1.9%), after
365,826–396,555 (8.0%). This is one three-trial session per revision; it
shows a regression worth investigating, not its cause.

Both fresh cluster starts encountered the DN0 registration/election race and
succeeded on retry after cleanup. Raw logs were saved in
`/tmp/delawson-object-bench-20260924/{output,baseline}.log` (temporary files).

## Historical rich-metadata revision, rerun 2026-09-24

Revision `8bb860e` (2026-09-09) was built in an isolated worktree and run with
the same 20M-object workload, explicit flags 23, 16 clients × depth 16, and
three 5-second lookup trials. Its topology matches today's: four DNs and four
SNs, three assigned SNs per DN and two assigned ENs per SN. Messaging settings
and response entry limits also match. Each revision uses its own client binary.

Do not use the September 8 revision just because it accepts flags 23:
`bc15b60` fixed rich batches sending a count-only zero-row extent allowance.
`8bb860e` includes that fix and the earlier instance-flag fix `07207ed`.
Before timing, a separate append-and-seal object was queried through both
ObjectInfo and BatchObjectInfo. Actual responses confirmed counts, stream rows,
extent rows, and four instance disk/EN references. Extents without the instances
flag returned empty instance lists. This verification object is outside the
20M empty-object benchmark population.

| Workload | September 9 revision | Current before segments | Current with segments |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,939,706 | 1,862,568 | 1,803,837 |
| ObjectInfo | 89,061 | 88,315 | 77,976 |
| BatchObjectInfo, batch 1,000 | 502,160 | 515,596 | 381,962 |

Rates are objects/names per second. Historical ObjectInfo ranged from
88,126 to 89,662 (1.7% spread); batches ranged from 499,413 to 503,560 (0.8%).
All creates completed and all lookups were answered and found. The detached
runner exited 0 after cleanup. Logs and the ten detail-verification responses
are in `/tmp/delawson-rich-bench-8bb860e/` (temporary files).


## Hybrid individual messages, 2026-09-24

Revision `a2ff50d`, same standard workload, flags 23, pool cap 32GiB and 2KiB
small-message threshold. Create: 1,492,484 objects/sec (20M in 13.40 seconds).
All lookups were answered and found.

| Workload | Segmented batches only | Hybrid individual messages | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 77,976 | 72,367 | -7.2% |
| BatchObjectInfo, batch 1,000 | 381,962 | 282,079 | -26.2% |

Rates are names/sec; three-trial means. ObjectInfo trials: 71,557, 72,595,
72,949 (1.9% spread). Batch trials: 291,127, 270,048, 285,063 (7.5% spread).
Logs: `/tmp/delawson-object-bench-_yf7nygn/`. The runner exited 0 after cleanup.
The first startup hit the known Raft election race; the retry succeeded. The
startup readiness gate was added afterward and did not affect these measurements.

## Direct encryption of pool leaves, 2026-09-24

Working tree based on `1333e46`, with sender encryption changed to incremental
AEGIS in the outgoing pool leaves. Same standard 20M-object workload, flags 23,
16 clients × depth 16, three trials, 32GiB pool cap and 2KiB message threshold.
The sender's unconditional plaintext scratch copy and separate ciphertext
allocation are removed. LZ4 attempts still use contiguous compression input;
receive-side buffering is unchanged.

| Workload | Previous hybrid run | Direct encryption | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,492,484 | 1,507,782 | +1.0% |
| ObjectInfo | 72,367 | 73,974 | +2.2% |
| BatchObjectInfo, batch 1,000 | 282,079 | 309,529 | +9.7% |

Rates are objects/names per second. ObjectInfo trials: 73,655, 74,291, 73,977
(0.9% spread). Batch trials: 307,901, 309,999, 310,688 (0.9% spread).
All 20M objects were created in 13.26 seconds; every lookup was answered and
found. Startup succeeded on the first attempt with the KS readiness gate.
The detached runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-_e3frsen/` (temporary files).
These are sequential sessions, not interleaved before/after measurements.

## Independent LZ4 leaf fast paths, 2026-09-24

Working tree based on `1333e46`, including direct sender encryption and the new
independent LZ4 input/output leaf fast paths. Same standard 20M-object workload,
flags 23, 16 clients × depth 16, three lookup trials, 32GiB pool cap and 2KiB
message threshold.

| Workload | Previous direct-encryption run | LZ4 fast paths | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,507,782 | 1,549,170 | +2.7% |
| ObjectInfo | 73,974 | 75,330 | +1.8% |
| BatchObjectInfo, batch 1,000 | 309,529 | 271,163 | -12.4% |

Rates are objects/names per second. ObjectInfo trials: 76,345, 74,488, 75,157
(range 74,488–76,345; 2.5% spread). Batch trials: 240,184, 306,133, 267,173
(range 240,184–306,133; 24.3% spread). The batch result is substantially more
variable than the previous session; this single sequential comparison does not
establish a stable regression or its cause.

All 20M objects were created in 12.91 seconds; every lookup was answered and
found. Startup succeeded, and the detached runner exited 0 after cleanup.
Logs: `/tmp/delawson-object-bench-7l_3tys8/` (temporary files).

### Repeat of the LZ4 fast-path run

Repeated unchanged with a fresh cluster and the same standard workload.
Create: 1,509,738 objects/sec (20M in 13.25 seconds).

| Workload | Previous direct encryption | First LZ4 run | Repeat LZ4 run | Repeat vs direct encryption |
| --- | ---: | ---: | ---: | ---: |
| ObjectInfo | 73,974 | 75,330 | 75,686 | +2.3% |
| BatchObjectInfo, batch 1,000 | 309,529 | 271,163 | 307,531 | -0.6% |

Rates are names/sec, three-trial means. ObjectInfo trials: 74,903, 76,096,
76,060 (range 74,903–76,096; 1.6% spread). Batch trials: 310,633, 305,003,
306,958 (range 305,003–310,633; 1.8% spread). The first run's large batch
slowdown did not reproduce; this repeat is close to the direct-encryption
baseline, with no demonstrated material batch throughput change.

Every lookup was answered and found. The detached runner exited 0 after
cleanup. Logs: `/tmp/delawson-object-bench-mgv_l8je/` (temporary files).

## Shared anonymous pool mappings, 2026-09-24

Working tree based on `60e5cd2`, changing only the pool mapping helper from
`MAP_PRIVATE | MAP_ANONYMOUS` to `MAP_SHARED | MAP_ANONYMOUS`. This applies
to block storage and external links. Pool tests passed. Same standard workload
and settings as the preceding LZ4 fast-path repeat, with a fresh cluster.

| Workload | MAP_PRIVATE (preceding repeat) | MAP_SHARED | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,509,738 | 1,494,816 | -1.0% |
| ObjectInfo | 75,686 | 74,845 | -1.1% |
| BatchObjectInfo, batch 1,000 | 307,531 | 307,269 | -0.1% |

Rates are objects/names per second; lookups use three-trial means. ObjectInfo
trials: 73,947, 74,934, 75,655 (range 73,947–75,655; 2.3% spread). Batch
trials: 304,533, 306,754, 310,520 (range 304,533–310,520; 1.9% spread).
These sequential sessions show no clear material throughput difference.

All 20M objects were created in 13.38 seconds; every lookup was answered and
found. The detached runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-ozofp2x4/` (temporary files).

## Virtual scratch aliases for LZ4, 2026-09-24

Working tree based on `60e5cd2`, retaining shared pool mappings and the direct
single-leaf paths. Cross-leaf compression input/output, decompression output,
and sealed receive input use persistent virtual scratch reservations populated
with aliases of the actual pool pages. LZ4 output capacity is allocated and
initialized in the pool before compression, then truncated to its actual size.
Same standard 20M-object workload, flags 23, 16 clients × depth 16, three lookup
trials, 32GiB pool cap and 2KiB message threshold.

| Workload | Shared mappings, copying fallback | Virtual aliases | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,494,816 | 1,359,438 | -9.1% |
| ObjectInfo | 74,845 | 73,768 | -1.4% |
| BatchObjectInfo, batch 1,000 | 307,269 | 232,418 | -24.4% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 73,191, 73,960, 74,154 (range 73,191–74,154; 1.3% spread).
Batch trials: 231,583, 233,547, 232,123 (range 231,583–233,547; 0.8% spread).
The batch slowdown is consistent across these three trials. Mapping syscalls
and temporary worst-case output pool allocation are plausible costs, but this
run does not isolate them.

All 20M creates completed in 14.71 seconds; every lookup was answered and found.
The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-_p2g83aj/` (temporary files).
Workspace tests passed, along with an additional maximum-size (64MiB + four
bytes) unaligned round-trip test exercising worst-case output reservation size.

## 16KiB blocks with virtual scratch aliases, 2026-09-24

Changed the single hardcoded `CHUNK_BYTES` to 16KiB. Index capacity and tree
spans/depth derive from that constant. The default message promotion threshold
is block size / 2 (8KiB), and the default pool block count is 32GiB / block size
(2,097,152), preserving the byte capacity. The sample configuration now uses
these derived defaults. Aliasing remains enabled when OS page size divides
block size; this machine still uses 4KiB OS pages. Shared mappings and virtual
LZ4 scratch paths remain enabled.

Same standard 20M-object workload, flags 23, 16 clients × depth 16, three lookup
trials. This comparison changes both block size and message promotion threshold.

| Workload | 4KiB aliases, 2KiB cutover | 16KiB aliases, 8KiB cutover | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,359,438 | 1,519,472 | +11.8% |
| ObjectInfo | 73,768 | 74,536 | +1.0% |
| BatchObjectInfo, batch 1,000 | 232,418 | 292,073 | +25.7% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 74,038, 74,880, 74,690 (range 74,038–74,880; 1.1% spread).
Batch trials: 292,970, 291,086, 292,163 (range 291,086–292,970; 0.6% spread).
The batch mean remains 4.9% below the earlier shared-mapping copying fallback
(307,269/s, with 4KiB blocks and a 2KiB cutover).

All 20M objects were created in 13.16 seconds; every lookup was answered and
found. Workspace tests passed, including block-relative boundary, alias, tree,
and pool-exhaustion tests. The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-3fx1tq1w/` (temporary files).

## 64KiB blocks with virtual scratch aliases, 2026-09-24

Changed `CHUNK_BYTES` from 16KiB to 64KiB. Derived defaults now give a 32KiB
message cutover, 524,288 blocks for the same 32GiB capacity, 16,384 indexes per
node, and maximum tree depth four. Virtual aliases remain enabled. Test-only
pool capacities now derive from their original 4GiB byte reservation: a fixed
million-block fixture would request 64GiB and was rejected by Linux. The Raft
segmented-message fixture now scales with the default cutover as well.

Same standard 20M-object workload, flags 23, 16 clients × depth 16, three lookup
trials. Both block size and message promotion threshold change together.

| Workload | 16KiB aliases, 8KiB cutover | 64KiB aliases, 32KiB cutover | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,519,472 | 1,617,054 | +6.4% |
| ObjectInfo | 74,536 | 74,354 | -0.2% |
| BatchObjectInfo, batch 1,000 | 292,073 | 317,932 | +8.9% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 73,869, 74,852, 74,342 (range 73,869–74,852; 1.3% spread).
Batch trials: 317,612, 316,020, 320,164 (range 316,020–320,164; 1.3% spread).
Batch throughput is 3.5% above the earlier shared-mapping copying fallback
(307,269/s), which used 4KiB blocks and a 2KiB cutover; this does not isolate
the effect of aliasing from block size and cutover.

All 20M objects were created in 12.37 seconds; every lookup was answered and
found. Workspace tests passed. The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-qqpsjlu2/` (temporary files).

## 256KiB blocks with virtual scratch aliases, 2026-09-24

Working tree based on `1af9c6c`, changing `CHUNK_BYTES` to 256KiB. Derived
settings give a 128KiB cutover, 131,072 blocks for the same 32GiB pool cap,
65,536 indexes per node and maximum tree depth three. Shared mappings and
virtual scratch aliases remain enabled. Same standard 20M-object workload,
flags 23, 16 clients × depth 16, three lookup trials.

| Workload | 64KiB aliases, 32KiB cutover | 256KiB aliases, 128KiB cutover | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,617,054 | 1,611,260 | -0.4% |
| ObjectInfo | 74,354 | 74,194 | -0.2% |
| BatchObjectInfo, batch 1,000 | 317,932 | 332,502 | +4.6% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 73,407, 74,537, 74,639 (range 73,407–74,639; 1.7% spread).
Batch trials: 335,120, 330,411, 331,976 (range 330,411–335,120; 1.4% spread).
Block size and promotion threshold change together, so the comparison does not
isolate their individual effects.

An index-growth test would allocate over 16GiB of payload plus its pool copy
at this block size. It now crosses an index boundary using a small rolling
range and still checks depth-two growth, node reclamation, and return of all
blocks. Workspace tests passed with this bounded fixture; no additional
functional failures were observed at 256KiB.

All 20M objects were created in 12.41 seconds; every lookup was answered and
found. The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-xdrvcq6a/` (temporary files).

## BatchObjectInfo response sizes, 2026-09-24

The saved FE logs from previous runs recorded operations, DN fan-out and latency,
but not response size or metadata row counts. Added an untimed sampled-response
report to `bench-info` and reran the standard 20M-object workload with 256KiB
blocks and a 128KiB cutover. All three samples for each workload agreed:

| Sample response | Serialized bytes, including 4-byte frame | Objects | Streams | Extents | Instances |
| --- | ---: | ---: | ---: | ---: | ---: |
| ObjectInfo | 232 | 1 | 1 | 0 | 0 |
| BatchObjectInfo, batch 1,000 | 219,052 | 1,000 | 1,000 | 0 | 0 |

The workload creates empty objects; flags 23 request extents/instances/counts,
but there are no extent or instance rows to return. A single batch response is
about 214KiB, so it fits in a 256KiB leaf if it starts at the beginning. Packed
transport batches may still combine multiple responses or start a response
partway through a leaf. These samples do not establish that all internal RPCs
or transport compression segments fit one block.

Run rates: create 1,562,928/s (20M in 12.80s), ObjectInfo mean 73,834/s
(trials 73,076, 74,436, 73,990; 1.8% spread), BatchObjectInfo mean 325,315/s
(trials 324,117, 322,091, 329,735; 2.3% spread). All lookups were answered
and found; the runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-kw92hi0i/` (temporary files). The running binary
also performed an isolated sealing estimate after timing; that extra estimate
was removed from the source at the user's request and is not reported here.

## One-shot direct/aliased encryption at 64KiB, 2026-09-24

Restored 64KiB blocks and the derived 32KiB message cutover. Sender encryption
now calls one-shot in-place AEGIS directly on a single-leaf body, or on a virtual
alias of a cross-leaf body. The output scratch reservation is reused after LZ4
releases its alias. Incremental per-leaf encryption remains only as the fallback
on platforms without compatible shared-page aliases. Receiver decryption was
already one-shot with contiguous input. The workspace suite passed, including
ciphertext/tag compatibility across multiple leaves.

Same standard 20M-object workload, flags 23, 16 clients × depth 16, three lookup
trials, and 32GiB pool cap. Baseline is the preceding 64KiB incremental-encryption
run, not the intervening 256KiB experiment.

| Workload | 64KiB incremental encryption | 64KiB direct/aliased encryption | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,617,054 | 1,592,349 | -1.5% |
| ObjectInfo | 74,354 | 74,858 | +0.7% |
| BatchObjectInfo, batch 1,000 | 317,932 | 313,604 | -1.4% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 74,448, 74,916, 75,210 (range 74,448–75,210; 1.0% spread).
Batch trials: 313,440, 315,243, 312,131 (range 312,131–315,243; 1.0% spread).
This sequential comparison shows no throughput improvement from the new
sender encryption paths.

Every sampled ObjectInfo response was 232 serialized bytes including its frame,
with one object and one stream. Every sampled BatchObjectInfo response was
219,052 bytes with 1,000 objects and 1,000 streams. Both had zero extent and
instance rows; samples are measured after timing, without a compression estimate.
All 20M objects were created in 12.56 seconds; every lookup was answered and
found. The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-8nlzqro9/` (temporary files).

## Two allocation bucket probes, 2026-09-24

Working tree based on `99dc6cc`, reducing `DEFAULT_CHOICES` from four to two.
Each probed bucket still attempts reclamation; the pool still has 256 buckets.
64KiB blocks, 32KiB message cutover, 32GiB pool capacity, virtual LZ4 aliases,
and direct/aliased encryption are unchanged. Same standard 20M-object workload,
flags 23, 16 clients × depth 16, three lookup trials.

| Workload | Four probes | Two probes | Change |
| --- | ---: | ---: | ---: |
| Create, batch 1,000 | 1,592,349 | 1,597,281 | +0.3% |
| ObjectInfo | 74,858 | 74,844 | -0.02% |
| BatchObjectInfo, batch 1,000 | 313,604 | 311,960 | -0.5% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 74,519, 75,076, 74,935 (range 74,519–75,076; 0.7% spread).
Batch trials: 306,780, 312,511, 316,590 (range 306,780–316,590; 3.1% spread).
This run shows no clear throughput change from reducing the probe count.

All 20M creates completed in 12.52 seconds; every lookup was answered and found.
All sampled batch responses were 219,052 serialized bytes including the frame,
with 1,000 objects, 1,000 streams, zero extents and zero instances. ObjectInfo
samples were 232 bytes with one object and one stream. Pool tests passed, and
the standalone benchmark compiles; its named probe-count modes were corrected
to retain their explicit counts when the production default changes.
The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-rnvm0sqi/` (temporary files).

## Eight allocation bucket probes, 2026-09-24

Changed only the allocation probe limit from two to eight, with reclamation at
each choice. All other settings match the preceding two/four-probe comparison:
256 buckets, 64KiB blocks, 32KiB cutover, 32GiB cap, and direct/aliased encryption.
Same standard 20M-object workload, flags 23, 16 clients × depth 16, three trials.

| Workload | Two probes | Four probes | Eight probes | Eight vs four |
| --- | ---: | ---: | ---: | ---: |
| Create, batch 1,000 | 1,597,281 | 1,592,349 | 1,594,997 | +0.2% |
| ObjectInfo | 74,844 | 74,858 | 75,608 | +1.0% |
| BatchObjectInfo, batch 1,000 | 311,960 | 313,604 | 316,214 | +0.8% |

Rates are objects/names per second; lookup rates are three-trial means.
ObjectInfo trials: 75,937, 75,488, 75,401 (range 75,401–75,937; 0.7% spread).
Batch trials: 314,876, 312,025, 321,741 (range 312,025–321,741; 3.1% spread).
The batch differences across probe counts remain smaller than the observed
trial spread; these sequential sessions do not demonstrate a clear advantage.

All 20M creates completed in 12.54 seconds; all lookups were answered and found.
Batch samples remained 219,052 bytes with 1,000 objects, 1,000 streams, zero
extents and zero instances; ObjectInfo samples were 232 bytes with one object
and one stream. Pool tests passed. The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-s4kztclg/` (temporary files).

After comparing two, four and eight probes, restored the four-probe default.
The small throughput differences suggest probe count is not a significant
bottleneck in this workload; they do not isolate every cost of pooled storage.

## Compact message payload layout, 2026-09-24

Working tree based on `7b03736`: keep the small `Vec<u8>` embedded, box the
large `SegmentedBuffer`, and change the deferred serialization error from
`Option<String>` to `Option<Box<String>>`. On this machine `MessageBytes`
shrinks from 64 to 32 bytes, `Arg` from 72 to 40, and `StructWriter` from
72 to 40; `StructReader` remains 56 bytes. A 3,001-argument batch response's
argument elements occupy 120,040 rather than 216,072 bytes, excluding payloads
and spare vector capacity. Small messages incur no new allocation; promotion
adds one box allocation and recording an error adds one box allocation.

This experiment also includes the pending inline hints on payload access/write
methods and an explicit all-or-error `Write::write_all` implementation. Thus
the throughput comparison does not isolate boxing from those changes.
Workspace tests passed. The common crate alone lacks the workspace-enabled
`Reply::new_test` test helper; use the workspace test configuration.

Standard workload and storage settings are unchanged: 20M objects, flags 23,
16 clients by depth 16, 64KiB blocks, 32KiB cutover, and four pool probes.
First run: create 1,634,213/s; ObjectInfo trials 77,574, 77,737, 75,773
(mean 77,028/s); BatchObjectInfo trials 271,684, 359,728, 291,625
(mean 307,679/s, 28.6% spread). Previous four-probe means were 74,858/s
and 313,604/s respectively. This noisy batch result establishes no speedup.
Every lookup was answered and found; samples remained 232 bytes for ObjectInfo
and 219,052 bytes for BatchObjectInfo, with 1,000 objects and streams and no
extents or instances in the batch. Runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-a3v70w7r/` (temporary files).

Repeat with identical code: create 1,647,049/s; ObjectInfo trials 75,592,
76,139, 76,149 (mean 75,960/s, 0.7% spread); BatchObjectInfo trials
355,481, 363,840, 360,165 (mean 359,829/s, 2.3% spread). The repeat batch
mean is 14.7% above the preceding four-probe baseline, but the first run's
variation means that improvement should be treated as provisional. Boxing
and inlining have not been measured separately. All lookups were answered
and found, sample sizes/counts were unchanged, and cleanup finished with
status 0. Logs: `/tmp/delawson-object-bench-dqoszrzs/` (temporary files).

## Incremental row and merge allocation improvements, 2026-09-24

Starting from `4799e92`, apply changes cumulatively and run the standard detached
20M-object benchmark after each. Storage settings, rich flags 23, client count,
and response sizes/counts are unchanged. Workspace tests passed at each step.

| Change | ObjectInfo/s | BatchObjectInfo/s | Batch trials/s |
| --- | ---: | ---: | --- |
| Starting point (previous repeat) | 75,960 | 359,829 | 355,481 / 363,840 / 360,165 |
| Single-row argument builders | 77,572 | 361,119 | 356,118 / 368,006 / 359,234 |
| Move FE response arguments | 76,587 | 381,932 | 382,465 / 379,981 / 383,351 |
| Reserve row and argument capacities | 78,260 | 447,924 | 444,727 / 446,272 / 452,774 |
| Cache append leaf and defer RNG access | 78,203 | 459,575 | 457,527 / 460,717 / 460,483 |

Single-row builders return an `Arg` directly where callers previously appended
a temporary one-element vector. Existing vector-returning entry points remain
for callers that require a vector. No clear batch improvement (0.4%, within
trial variation). Logs: `/tmp/delawson-object-bench-hk1co_sn/`.

FE now records parsed piece lengths and moves owned DN response arguments into
per-object slots instead of cloning payloads. Plain final answers also move
arguments out of slots. Detailed final answers retain the existing limit
validation and reconstruction. Batch throughput rose 5.8%, with a 0.9% trial
spread. Logs: `/tmp/delawson-object-bench-s_q3wm2w/`. Both runners finished
with status 0 after cleanup; all lookups were answered and found.

The next implemented step reserves fixed-row payload sizes (70 bytes for an
object, 64 for a shape or stream, 32 for a batch header), bounded by the small
message threshold. DN rich batch capacity estimates include the shape, object,
and usual one stream; per-object assembly reserves its known argument count.
Workspace tests passed. Batch throughput rose another 17.3%, with a 1.8%
trial spread; ObjectInfo trials were 77,580 / 79,358 / 77,841. All lookups
were answered and found and sample sizes/counts were unchanged. Runner
finished with status 0 after cleanup: `/tmp/delawson-object-bench-8b8oi8jo/`.
These first three changes were committed together as `5f8008c`.

Fourth step: cache the last initialized leaf in `SegmentedBuffer` so small
appends can copy directly without traversing the tree. Pruning invalidates the
cache before recycling blocks; boundary-crossing writes retain allocation-error
rollback. Message payloads now borrow the thread-local RNG only when the pool
actually requests a random value. Workspace tests passed, including a rolling
model mixing direct appends, writers, truncation, clearing and prefix discard.
Committed as `44209c2`. The completed benchmark averaged 78,203 ObjectInfo/s
(trials 77,232 / 77,685 / 79,692) and 459,575 BatchObjectInfo/s (trials
457,527 / 460,717 / 460,483, 0.7% spread). Batch throughput rose 2.6% over
capacity reservations alone, and 27.7% over the starting repeat. ObjectInfo
was effectively unchanged from the capacity step. All lookups were answered
and found, sample sizes/counts were unchanged, and the runner exited 0 after
cleanup. Logs: `/tmp/delawson-object-bench-lkf3non4/` (temporary files).
These are sequential runs, not randomized alternating comparisons; small
differences should still be interpreted with that limitation.

## ObjectInfo perf comparison, 2026-09-24

Compared current `73d1321` with the all-Vec `8729d5b` checkout using the same
standard 20M-object workload, rich flags 23, 16 clients by depth 16. Recorded
only the three batch-1 trials with `perf record -F 199 -e cpu-clock
--call-graph dwarf,4096`, attached to all FE/DN/SN ready-file PIDs. The client,
KS and EN are excluded. Each detached runner owned build, startup, population,
both lookup workloads and cleanup. Baseline's only source-tree adjustment is
the previously documented KS-leader readiness gate in `scripts/start-laws`;
measured application code is unchanged.

Current: 985,075 ObjectInfo lookups, 139.719 sampled server CPU seconds;
Vec: 1,112,309 lookups, 138.754 CPU seconds. Approximate server CPU per
lookup rose from 124.74us to 141.84us (13.7%). These are CPU sums across
processes/threads, not request latencies. Perf perturbs scheduling and throughput;
the profiled rates should not be compared directly with unprofiled benchmarks.
No samples were reported lost. Kernel symbols are restricted, so kernel CPU
cannot be attributed reliably to individual kernel functions. DWARF stacks
are bounded; use the exclusive symbol samples, not incomplete inclusive
call chains, for the table below.

| Exclusive sampled CPU, us/ObjectInfo lookup | Vec | Current |
| --- | ---: | ---: |
| Kernel (unresolved symbols) | 63.82 | 69.09 |
| libc I/O and syscall wrappers | 9.04 | 10.81 |
| libc allocation/free methods | 13.87 | 8.61 |
| libc copy/zero methods | 2.71 | 3.90 |
| Pool allocation methods | 0 | 2.16 |
| Segmented buffer/reader methods | 0 | 5.76 |
| MessageBytes methods | 0 | 3.14 |
| Named LZ4 methods | 1.48 | 2.02 |
| Named crypto methods | 2.64 | 3.25 |
| Other | 31.17 | 33.09 |

Rows group non-overlapping exclusive samples by symbol, not all inclusive work
for a subsystem; inlined work can be attributed elsewhere. Zero means no
matching symbols in the old representation, not absence of equivalent work.
Unlike BatchObjectInfo, ObjectInfo is dominated by kernel/I/O and per-message
transport overhead. Allocator CPU is lower after the metadata improvements.

Promising next experiments, not yet implemented or measured:

- Retain a bounded number of leaves per connection across batch resets.
  `Wire::send` clears sealed storage after each transport segment and clears
  plaintext after sending; `Reader::fill` similarly clears sealed/open storage.
  `MessageBuffer::clear` replaces the tree, returning its blocks to the pool.
  Old Vec send buffers retained capacity. Reuse could avoid both pool operations
  and some tree setup; retained memory must be bounded for idle connections.
- Single-leaf receive/send paths could avoid constructing the 64-entry iovec
  array and use scalar socket I/O. `machine::Stream::write` currently forwards
  even scalar writes to `write_vectored`, so that layer also needs consideration.
  The kernel difference is not proof of readv/writev being the cause.
- Parsing a frame wholly within one leaf could use a contiguous reader, while
  keeping segmented decoding for crossings. The profile shows reader setup
  and wrapper costs, but does not yet quantify the benefit of this fast path.

The subsequent unprofiled batch-1000 means in these runs were 462,930/s current
and 511,106/s Vec. Every lookup in both workloads was answered and found; both
runners exited 0 after cleanup. Current samples remained 232 serialized bytes
for ObjectInfo and 219,052 for BatchObjectInfo with the same row counts. Logs,
perf captures, exact-period reports and category summaries (temporary files):
`/tmp/delawson-object-profile-current-53n9i7y_/` and
`/tmp/delawson-object-profile-vec-h1dgde6r/`.

## Bounded connection-buffer retention, 2026-09-24

Working tree based on `73d1321`. Retain a lazily allocated prefix of 1, 2,
or 4 leaves per reusable `MessageBuffer`, preserving required index nodes.
Reset hides all old bytes; discarded live ranges outside the retained prefix
are still reclaimed. Ordinary `SegmentedBuffer::new` keeps its release-on-clear
behavior. A high-water leaf bound lets pruning recognize a wholly retained
subtree without scanning unused index entries on every reset.

This is per connection buffer, not a shared per-connection cache. A direct
connection has four such buffers (send plaintext, send sealed, receive sealed,
receive opened); batched sending has a fifth for the alternate plaintext batch.
With 64KiB blocks, fully populated idle retention is:

| Leaves per buffer | Blocks including index | KiB/buffer | KiB/direct connection | KiB/batched connection |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1 | 64 | 256 | 320 |
| 2 | 3 | 192 | 768 | 960 |
| 4 | 5 | 320 | 1,280 | 1,600 |

Retention is a maximum, not eager allocation. Workspace tests passed for the
initial implementation; the pool test checks all three limits plus zero,
unchanged leaf indexes on reuse, index-block counts, prefix discard, truncation,
reset, inaccessible stale bytes, and return of every block on drop. Socket tests
now check that reset leaves no readable data rather than assuming zero capacity.

Same standard 20M-object workload and rich flags 23; scalar I/O is not included
in the retention comparisons.

| Retained leaves | ObjectInfo/s | BatchObjectInfo/s | Batch trial spread |
| --- | ---: | ---: | ---: |
| 0 (preceding unprofiled run) | 78,203 | 459,575 | 0.7% |
| 1 | 78,468 | 398,242 | 18.1% |
| 1, repeat | 79,347 | 468,261 | 2.5% |
| 2 | 80,120 | 474,747 | 0.6% |
| 4 | 78,781 | 437,738 | 13.7% |

One leaf: ObjectInfo trials 77,990 / 78,654 / 78,760; batch trials
368,205 / 386,179 / 440,341. The wide batch variation prevents a firm
conclusion from this run alone. Logs: `/tmp/delawson-object-bench-pwvyidy9/`.
Two leaves: batch trials 473,329 / 474,758 / 476,155. Logs:
`/tmp/delawson-object-bench-xx6quric/`. Both runners completed with status 0
after cleanup; every lookup was answered and found and response samples were
unchanged. These sequential runs are not randomized alternating comparisons.

Four leaves: ObjectInfo mean 78,781/s; batch trials 414,436 / 474,211 /
424,566 (mean 437,738/s, 13.7% spread). Runner exited 0 after cleanup;
all lookups succeeded and response samples were unchanged. Logs:
`/tmp/delawson-object-bench-kh7cuyok/`. One leaf is being repeated before
the scalar-I/O experiment because the first one-leaf and four-leaf runs
had wide variation.

Requested one-leaf repeat: ObjectInfo averaged 79,347/s and BatchObjectInfo
468,261/s (trials 460,621 / 471,687 / 472,476, 2.5% spread). The original
398K batch mean was not reproduced. One leaf is close to two leaves' 475K
mean, and four showed no advantage, so retain one leaf for the next experiment.
All lookups succeeded, samples were unchanged, and cleanup finished with status
0. Logs: `/tmp/delawson-object-bench-f0s4lb5u/`.

Next experiment adds scalar reads/writes whenever the remaining transfer fits
in one leaf, avoiding iovec construction; multi-leaf transfers stay vectored.
The plain socket `Stream::write` now calls scalar socket write rather than
forwarding it to write_vectored. TLS keeps its existing serialization/locking.
Workspace tests passed, including scalar short transfers, interruptions and
EOF rollback, alongside existing multi-window vectored and socket tests.
Completed scalar-I/O benchmark with one retained leaf: ObjectInfo 80,596/s
(trials 80,130 / 80,899 / 80,760; 1.0% spread) and BatchObjectInfo 470,620/s
(trials 467,558 / 470,610 / 473,693; 1.3% spread). Compared with the immediately
preceding one-leaf repeat, this is +1.6% ObjectInfo and +0.5% BatchObjectInfo,
small enough that these sequential trials do not establish a clear advantage.
All lookups were answered and found, samples were unchanged, and cleanup
finished with status 0. Logs: `/tmp/delawson-object-bench-ketj7a81/`.
Leave one retained leaf per buffer and the scalar-I/O fast path enabled.

## ObjectInfo perf after retention and scalar I/O, 2026-09-24

Compared `a2f4327` with all-Vec `8729d5b`, using the same standard detached
20M-object workload and profiling only the three ObjectInfo trials. Both runs
attach to FE/DN/SN processes, excluding the client, KS and EN. Record 199Hz
cpu-clock instruction-pointer samples without stack dumps; concurrently collect
perf stat task-clock, context-switches, CPU migrations and page faults. The
baseline still has only the startup KS-readiness gate adjustment.

Current completed 982,481 profiled lookups; Vec completed 1,072,689. Despite
omitting stacks, counter collection still perturbed throughput, so compare the
matched instrumented runs, not their rates against normal benchmarks. Perf stat
measured 173.45us aggregate server task-clock per current lookup versus 159.94us
for Vec (+8.4%). Sample-weighted CPU estimates are lower (139.71us versus
128.66us, +8.6%); these are distinct estimators, and the rows below sum to the
sampled estimate, not task-clock. Kernel symbols remain unavailable.

| Exclusive sampled CPU, us/ObjectInfo lookup | Vec | Current |
| --- | ---: | ---: |
| Kernel, unresolved | 66.73 | 70.13 |
| libc I/O/syscall wrappers | 11.13 | 12.38 |
| libc allocation/free | 13.72 | 9.46 |
| libc copy/zero | 2.98 | 3.31 |
| Pool allocation methods | 0 | 0 |
| Segmented buffer/reader methods | 0 | 5.09 |
| MessageBytes methods | 0 | 2.61 |
| Named LZ4 methods | 1.57 | 2.38 |
| Named crypto methods | 3.72 | 3.57 |
| Other | 28.82 | 30.76 |

As before, symbol buckets are exclusive, not inclusive subsystem timings;
inlined work can be charged elsewhere. Zero means no matching samples. There
were no pool allocation-method samples in the current capture: retaining one
leaf has removed that previous visible cost for this workload.

Specific remaining candidates, not changed during this investigation:

- `MessageBytes::write` and `check` account for 1.22us and 0.78us per lookup
  respectively; the whole named MessageBytes group is 2.61us. Tightening the
  small-Vec write path and keeping promotion/error machinery cold is a more
  targeted next experiment than another pool change.
- `SegmentedBuffer::{reset,prune,retain}` together account for 1.27us per
  lookup; append_slow when copying a framed response into the batch accounts
  for another 0.73us. A reset invalidates the tail cache even when keeping
  leaf zero, so the first append still calls ensure_leaf/insert. Wire sending
  and receiving also reset buffers that are commonly already reset. Avoiding
  redundant reset/pruning and cheaply reusing the first retained leaf may help.
- The buffer receive and compression-output paths still initialize zero slices
  and append-copy them into leaves. A direct zero-fill/reservation operation
  could avoid a pass, though sampled copy/zero is only 0.34us higher than Vec.

The existing scalar field parser already checks the minimum of current chunk
and message remainder. Decryption and LZ4 already have single-leaf paths. These
profiles do not justify adding a second deserializer merely to avoid boundaries.

Context switches per lookup were 3.939 current versus 3.833 Vec; migrations
0.498 versus 0.472; faults 0.0076 versus 0.0086. Scheduling/socket overhead is
large in both, with only modest differences in these counters. This does not
identify which kernel functions account for the additional sampled time.
Logging/ZSTD/formatting named self samples were about 4.81us current and 5.06us
Vec, another common cost rather than an explanation of the regression.

All lookup results in both workloads were answered and found, and both runners
completed cleanup with status 0. No application code changed. Captures, exact
period reports, stat counters and JSON summaries (temporary files):
`/tmp/delawson-object-perf-light-current-fjo1pdad/` and
`/tmp/delawson-object-perf-light-vec-_xdno6m9/`.

## Small-Vec append fast path, 2026-09-24

Working tree based on `a2f4327`. Isolate a short inline successful Small(Vec)
append path in MessageBytes: check the stored-error flag, dispatch to the Vec,
check addition and the unchanged promotion threshold, then extend. Promotion
and already-segmented writes move into a non-inlined helper; constructing a
stored I/O error moves into a cold helper. Bounds checks, promotion, storage
representation and wire format are unchanged. Both before and after already
use Vec for small payloads; this tests wrapper/code-generation overhead, not
an intrinsic advantage of Vec storage over a contiguous pool leaf.

Workspace tests passed, including threshold promotion, cross-block round trips
and failure/rollback coverage. Same standard detached benchmark, one retained
leaf per connection buffer and scalar I/O enabled:

| Workload | Preceding baseline | Small-Vec fast path | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo/s | 80,596 | 82,507 | +2.4% |
| BatchObjectInfo/s | 470,620 | 501,688 | +6.6% |

ObjectInfo trials: 81,588 / 82,478 / 83,455 (2.3% spread). Batch trials:
499,734 / 500,682 / 504,649 (1.0% spread). These are sequential sessions,
so the modest ObjectInfo improvement especially should not be treated as a
precise causal estimate. Every lookup was answered and found, sample sizes and
row counts were unchanged, and cleanup completed with status 0. Logs:
`/tmp/delawson-object-bench-wna0ttdp/` (temporary files).

Generated-code evidence: the cached pre-change DN binary from the last perf
run (build ID ab8f23ee7f462fe09f4600affdfb8bb71a58d0d4) contains separate
MessageBytes::write functions of 1,599/1,648 bytes and check functions of
235 bytes. The experimental DN binary has no separate write/check symbols;
its remaining write_all instances are 172/225 bytes, versus 1,741 bytes for
the previous write_all instance. Function sizes are not executed-instruction
counts, but support the explanation that outlining the uncommon work allowed
a substantially smaller hot path. The underlying byte copy is unchanged.

## Cached writable span for MessageBytes, 2026-09-24

On top of the preceding small-Vec experiment, ordinary writes now compare
against a cached allowance, copy to a cached pointer, and advance the pointer,
logical length, and allowance. Growth, promotion, leaf transitions, and stored
errors use a separate slow routine. Backing lengths synchronize on slow
mutations; reads also expose initialized bytes written through the cache.

Same standard detached workload, 20M objects, flags 23, 16 clients x depth 16.

| Workload | Preceding small-Vec path | Cached span | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 82,507 | 80,750 | -2.1% |
| BatchObjectInfo | 501,688 | 513,041 | +2.3% |

ObjectInfo trials: 80,571, 80,845, 80,833 (0.3% spread). BatchObjectInfo:
511,407, 515,759, 511,958 (0.8%). Create: 1,927,875 objects/sec.
All creates completed and every lookup was answered and found. Sample
responses remain 232 bytes (one object/stream) and 219,052 bytes (1,000
objects/streams), including framing, with no extent or instance rows.
The runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-pn_4cv2u/` (temporary files).
These sequential sessions show a mixed result, not a universal improvement.

Layout matters: MessageBytes grew from 32 to 56 bytes, Arg from 40 to 64,
and StructWriter from 40 to 64. The existing Arg variants embed MessageBytes;
the earlier boxing change boxed its SegmentedBuffer and error internally,
not MessageBytes itself. Thus the new cache also enlarges every Arg. This
must not be mistaken for a comparison holding argument size constant.

Validation: cargo test --workspace passed. New tests cover cached writes read
before backing-length synchronization, clone/move across threads, leaf
crossings against a contiguous model, truncation, patching, clearing, and
stored errors (including empty writes). Existing exhaustion and round-trip
tests also pass. No wire-format change.

## Move payload indirection to Arg, 2026-09-24

Keep the cached writable span, but change Arg::Struct/Bytes to own
Box<MessageBytes>, and keep SegmentedBuffer inline in MessageBytes's Large
variant. Small payloads still use the Vec variant. Arg is now 24 bytes,
MessageBytes 96, StructWriter 104, and StructReader remains 56. In particular,
adding metadata to MessageBytes no longer increases the size of every Arg.

Two standard detached runs, with no code change between them:

| Workload | Cached span, inline payload | Boxed payload | Boxed payload repeat |
| --- | ---: | ---: | ---: |
| ObjectInfo | 80,750 | 81,727 | 81,457 |
| BatchObjectInfo | 513,041 | 351,797 | 408,751 |

First ObjectInfo trials: 81,823, 81,610, 81,747 (0.3% spread); batches:
317,479, 328,476, 409,436 (26.1%). Repeat ObjectInfo: 80,709, 81,950,
81,712 (1.5%); batches: 395,469, 409,444, 421,340 (6.3%).
The batch slowdown repeats, although its magnitude is noisy. Described-object
and stream rows are themselves Arg::Struct values, so boxing adds one heap
allocation per such small payload; this is a plausible explanation, not a
profiled attribution. No automatic reversion: the requested layout is retained.

Both runs created all 20M objects, every lookup was answered/found, and both
runners exited 0 after cleanup. Sample response sizes and row counts are
unchanged. Logs: `/tmp/delawson-object-bench-ivs4miiq/` and
`/tmp/delawson-object-bench-zqm0a8bz/` (temporary files).
Workspace tests passed after updating constructors and forwarding ownership;
wire encoding is unchanged.

## Inline small finished payloads, 2026-09-24

Separate mutable MessageBytes (including its cached writable span) from finished
MessagePayload. Arg embeds the latter: a compact enum holding inline Vec metadata,
a boxed SegmentedBuffer, or a boxed failed writer on the error path. The helper
enum keeps the existing Struct/Bytes argument variants without another heap
allocation. Finishing moves the Vec or tree and commits cached writes without
copying payload bytes; reopening a finished payload moves the same storage back
into a writer. Successful finished payloads carry no write cursor or error slot.

Measured sizes: MessagePayload 24 bytes, Arg 32, MessageBytes 96,
StructWriter 104, StructReader 56. Compared with boxing every MessageBytes,
Arg grows from 24 to 32 bytes but small rows no longer allocate a payload box.
Compared with embedding the cached MessageBytes, Arg shrinks from 64 to 32.

Same standard detached 20M-object workload, flags 23, 16 clients x depth 16,
three five-second trials of each lookup batch size:

| Workload | Box every payload (repeat) | Inline small, first | Inline small, repeat |
| --- | ---: | ---: | ---: |
| ObjectInfo | 81,457 | 81,764 | 82,012 |
| BatchObjectInfo | 408,751 | 457,868 | 526,484 |

First ObjectInfo trials: 80,834, 82,857, 81,601 (2.5% spread). First batches:
418,902, 510,312, 444,389 (20.0%); this spread motivated repeating unchanged
code. Repeat ObjectInfo: 81,440, 82,416, 82,179 (1.2%). Repeat batches:
527,487, 525,899, 526,066 (0.3%). The repeat recovers the batch slowdown;
it is also 2.6% above the earlier 513,041 cached-span/inline-writer result,
but session variability prevents a strong claim about that smaller difference.

Both runs created all 20M objects and every lookup was answered and found.
Sample responses remain 232 bytes with one object/stream and 219,052 bytes
with 1,000 objects/streams, including framing and with zero extent/instance
rows. Both detached runners exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-d5jayp6e/` and
`/tmp/delawson-object-bench-_nph0pk6/` (temporary files).

Validation: cargo test --workspace and git diff --check passed. New tests
verify storage pointers remain unchanged when finishing/reopening small and
segmented payloads, cached tail bytes become visible, cloning preserves bytes,
and failed serialization remains an error through finish/clone/reopen. Existing
wire compatibility, promotion, exhaustion, forwarding and reader tests pass.
The wire format is unchanged. Finished payload mutation sites explicitly reopen
a writer rather than carrying a cursor in every argument.
