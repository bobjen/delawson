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
laws=./target/release/laws
cluster="$output/run/cluster-effective.toml"
raft_nodes=()
while read -r identity kind _; do
    if [[ $kind == ks || $kind == sn || $kind == dn ]]; then
        raft_nodes+=("$identity")
    fi
done < <("$laws" --config "$cluster" services)

# start-laws gates on the KS registrar and every DN ring; wait here for every
# KS, SN, and DN member to report a known, healthy leader before benchmarking.
deadline=$((SECONDS + 60))
while true; do
    missing=()
    for identity in "${raft_nodes[@]}"; do
        listen=$(sed -n 's/^listen = "\(.*\)"$/\1/p' "$output/run/$identity/process.toml")
        state=$("$laws" --config "$cluster" --at "$listen" --to "$identity" status 2>&1) || state=""
        if [[ ! $state =~ raft_leader[[:space:]]+[0-9]+ || $state != *"raft_failed false"* ]]; then
            missing+=("$identity")
        fi
    done
    ((${#missing[@]} == 0)) && break
    if ((SECONDS >= deadline)); then
        echo "benchmark: no healthy known Raft leader on: ${missing[*]}" >&2
        for identity in "${missing[@]}"; do
            listen=$(sed -n 's/^listen = "\(.*\)"$/\1/p' "$output/run/$identity/process.toml")
            "$laws" --config "$cluster" --at "$listen" --to "$identity" status >&2 || true
        done
        exit 1
    fi
    sleep 0.02
done
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
starting/unlocking the KS members and before starting DNs. It then waits for
every DN ring to report a healthy known leader before returning. The benchmark
runner checks every KS, SN, and DN member before population. Preserve all gates:
a ready file only proves the listener is up.
Older launchers without the KS gate can fail DN0 registration with `Raft has no
known Raft leader`; check historical launchers before running. If startup fails,
allow cleanup to finish before another attempt. Never interpret failed startup
or nonexistent-object lookups as a performance result.

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

## ObjectInfo perf after cached writes and compact payloads, 2026-09-24

Compared current cf96712 with a fresh all-Vec 8729d5b run. Reused the earlier
lightweight detached perf runner: standard 20M objects, flags 23, 16 clients x
depth 16, three five-second ObjectInfo trials recorded at 199Hz cpu-clock
without stacks, concurrently collecting task-clock, context switches, migrations,
and faults. Attach to all FE/DN/SN processes, excluding client/KS/EN. The
standard BatchObjectInfo trials still run afterward, without profiling.
Baseline application code is unchanged; only its existing startup KS-readiness
gate patch is present. Both runs exited 0 after cleanup and all creates and
lookups succeeded (all lookups answered/found).

Current completed 992,118 profiled ObjectInfo lookups, Vec 1,072,871.
Aggregate server task-clock per lookup: 171.21us current, 160.43us Vec,
+6.72%. This is consistent with the remaining unprofiled throughput gap.
The profiled lookup rates themselves were 66,059/s and 71,329/s, with 8.9%
and 10.2% spreads; instrumentation perturbs throughput. Do not compare these
rates directly with the normal 82K/88K measurements.

The sample-weighted CPU estimates are 138.51us current and 130.05us Vec
(+6.51%). They are distinct from task-clock; the following exclusive symbol
buckets sum to the sampled estimate, not the hardware/software counter total.

| Exclusive sampled CPU, us/ObjectInfo lookup | Vec | Current |
| --- | ---: | ---: |
| Kernel, unresolved | 67.77 | 70.57 |
| libc I/O/syscall wrappers | 11.74 | 12.83 |
| libc allocation/free | 14.14 | 9.19 |
| libc copy/zero | 2.90 | 2.91 |
| Segmented storage/reader path symbols | 0 | 5.85 |
| MessageBytes methods | 0 | 2.20 |
| MessagePayload methods | 0 | 0.46 |
| Named LZ4 methods/adapters | 1.62 | 2.51 |
| Named crypto methods | 3.66 | 3.53 |
| Other | 28.22 | 28.47 |

No matching pool allocation-method samples. Segmented storage/reader grouping
also includes wire Cursor instantiations on SegmentedReader. As with earlier
profiles, inlining and generic-symbol attribution limit subsystem accounting:
these are exclusive named-symbol buckets, not inclusive times. No samples were
lost. Kernel symbols remain unavailable, so the additional kernel cost cannot
be assigned to particular kernel functions. Approximate sampled per-process
CPU/lookup: DN 57.66 vs 55.18us, FE 48.86 vs 45.17us, SN 31.99 vs 29.70us
(current vs Vec). The difference is spread across services, largest in FE.

The old standalone per-field MessageBytes::write/check hot symbols are absent.
But the current MessageBytes + MessagePayload buckets still total 2.66us:
work has shifted to slow/setup paths, not disappeared. Named write_slow is
0.47us, synchronize 0.26us, write_at 0.20us, from_storage 0.16us, finished
payload conversion 0.21us. The earlier corresponding message-only bucket was
2.61us (different revision/run; not a precise before/after attribution).

Concrete remaining candidates, no implementation changes in this investigation:

- Retained single-leaf reset and first append: reset/prune/retain total 1.22us,
  append_slow copying a framed payload 0.70us, plus named insert paths about
  0.40us. reset always calls prune, which invalidates the tail; append's fast
  path excludes offset zero. Thus a retained leaf still takes tree setup on
  its first append. Reader::fill also clears sealed storage after processing,
  then receive clears it again before the next read. A cheap retained-leaf
  reset/append path is the strongest next focused experiment.
- Receive/output initialization still appends zero-filled slices before receiving
  or compressing. read_exact_from itself samples at 0.59us; direct initialized
  reservation could avoid setup/copy work. However total copy/zero is equal to
  Vec, so this profile does not support claiming extra copying is the main gap.
- Thread-local RNG access remains visible (one named RANDOM closure 0.57us),
  even though no pool allocation methods sampled. Making TLS lookup lazy as
  well as number generation is another candidate, not an established saving.

Context switches/lookup: 3.903 current, 3.840 Vec; migrations: 0.491 vs
0.485; faults: 0.0073 vs 0.0093. Allocation/free is already cheaper than Vec;
this is not evidence for tuning pool contention or returning to malloc.

Artifacts: `/tmp/delawson-object-perf-cf96712-_o791he5/` and
`/tmp/delawson-object-perf-vec-repeat-x1fx6v_o/`, each with runner, profile
wrapper, captures, output, stat counters, symbol/process-period reports, and
summary.json. Analysis helper: `/tmp/delawson-object-perf-analyze.py`.
All are temporary files. No application code changed.

## Cheap retained-leaf reset and first append, 2026-09-24

Keep RETAINED_LEAVES at one. SegmentedBuffer::reset now sets start/end to zero
and retains the root as the append tail without pruning when the sole allocation
is retained leaf zero. General cleanup still handles larger trees, then primes
the first retained leaf. Direct append accepts the empty zero-offset cached leaf;
nonzero block boundaries still find the next leaf. SegmentedWriter also primes
its writable cache from this retained leaf, avoiding its first-write slow path.
No block bytes are cleared by reset, and no mappings change.

cargo test --workspace passed, including counters compiled only for tests proving
that repeated retained-leaf resets bypass prune and first fitting appends bypass
append_slow. Tests also cover cleanup of a larger tree and verify that a full
leaf is not overwritten by the next append, including reopening a stream writer
at that boundary. Existing rolling-buffer, failure, retention and ownership
audits pass. git diff --check passed.

Standard detached workload, two runs of unchanged code:

| Workload | Previous cf96712 repeat | First run | Repeat |
| --- | ---: | ---: | ---: |
| ObjectInfo | 82,012 | 74,566 | 83,601 |
| BatchObjectInfo | 526,484 | 465,466 | 500,270 |

First ObjectInfo trials: 74,568, 73,477, 75,652 (2.9% spread). First batches:
443,770, 499,483, 453,144 (12.0%). Repeat ObjectInfo: 85,677, 82,813,
82,313 (4.0%); repeat batches: 511,619, 486,707, 502,485 (5.0%).
The repeat ObjectInfo mean is 1.9% above the previous repeat, but between-run
variation is larger; these measurements do not establish a reliable speedup.
The requested fast-path behavior is retained and independently tested.

Both runs created all 20M objects, every lookup was answered/found, and cleanup
completed with status 0. Sample response sizes/counts are unchanged. Logs:
`/tmp/delawson-object-bench-emxpssub/` and
`/tmp/delawson-object-bench-x0ui7k53/` (temporary files).

## ObjectInfo perf after cheap reset: CPU versus stalls, 2026-09-24

Profiled the current uncommitted retained-leaf reset/append changes against
8729d5b twice with matched instrumentation. Standard detached 20M-object workload,
flags 23, 16 clients x depth 16; only the three ObjectInfo trials are profiled,
then ordinary BatchObjectInfo trials run. All four runs created all objects,
answered/found every lookup, and exited 0 after cleanup. Baseline retains only
its startup readiness patch. No additional application changes in this analysis.

First pair added cycles, instructions, stalled-cycles-frontend and cache-misses
to the usual task-clock/scheduling counters and 199Hz cpu-clock sampling.
All counters report 100% runtime coverage. Backend-stall events are unsupported.
On this Ryzen 7 5825U, perf identifies frontend stalls as stalled cycles during
issue (idle-cycles-frontend alias, event 0xa9); cache-misses is its generic event
0x64/umask 0x09. These event counts are clues, not a direct accounting of DRAM
latency, blocked time, or all possible execution stalls.

| Hardware-counter run, per lookup | Vec | Current | Change |
| --- | ---: | ---: | ---: |
| Instructions | 190,550 | 189,169 | -0.7% |
| Cycles | 348,250 | 363,330 | +4.3% |
| Frontend-stall events | 136,615 | 152,162 | +11.4% |
| Cache-miss events | 5,448 | 6,337 | +16.3% |
| IPC | 0.547 | 0.521 | -4.8% |
| Server task-clock, us | 190.35 | 196.99 | +3.5% |
| Context switches | 3.620 | 3.640 | +0.6% |

Current completed 853,031 ObjectInfo lookups; Vec 888,717. Effective counted
cycles/task-clock implies 1.84GHz current versus 1.83GHz Vec, so a 4GHz
conversion would not describe these runs. Both use eight cores/16 SMT threads.
Instruction counts are essentially equal while cycles and cache/stall events
are higher, consistent with less efficient execution rather than just more
instructions. CPU time includes stalls while a thread is running; blocked
socket time and scheduler queue delay are distinct. These captures do not
measure off-CPU waits directly or prove the cause of the extra cycles.

Hardware counter collection materially perturbed both throughput and sample
composition (kernel ~95us/lookup and LZ4 ~6us, compared with ~70us/~2us in
lighter captures). Consequently a second matched pair used the previous
lighter setup: task-clock/scheduling counters plus 199Hz CPU samples only.

| Lighter run | Vec | Current |
| --- | ---: | ---: |
| Completed profiled ObjectInfo lookups | 1,065,978 | 1,012,189 |
| Task-clock us/lookup | 160.86 | 168.42 |
| Sample-weighted us/lookup | 127.54 | 137.39 |
| Context switches/lookup | 3.855 | 3.893 |
| Migrations/lookup | 0.486 | 0.494 |

Task-clock remains 4.7% higher, so the gap is not purely off-CPU delay.
The sample estimate is a different estimator (+7.7%); do not add sample buckets
to the task-clock number. Current vs Vec exclusive samples: kernel 69.83 vs
66.57us, libc I/O wrappers 12.76 vs 11.64us, allocator 9.70 vs 13.20us,
copy/zero 2.85 vs 2.99us, segmented storage/reader symbols 4.66 vs zero,
MessageBytes 1.99 and MessagePayload 0.53 vs zero, LZ4 2.45 vs 1.61,
crypto 3.89 vs 3.63, other 28.73 vs 27.88us. Kernel symbols remain unresolved;
socket internals cannot be distinguished from other kernel work. Both user and
kernel sampled CPU are higher; no basis for attributing the whole gap to sockets.
No pool allocation-method samples matched.

Targeted symbols before reset change -> after, same lighter profiling setup:
reset 0.309 -> 0.387us, prune 0.456 -> 0.154, retain 0.451 -> 0.243,
append_slow 0.704 -> 0.174, insert 0.400 -> 0.065. Combined 2.32 -> 1.02us.
These exclusive symbol samples confirm substantial removal of slow-path work,
with residual calls expected for other pruning and multi-leaf operations.
They are not a controlled estimate of the exact speedup.

Hardware-pair artifacts:
`/tmp/delawson-object-perf-reset-2or_2b0_/` and
`/tmp/delawson-object-perf-reset-vec-ow2mliyn/`.
Lighter-pair artifacts:
`/tmp/delawson-object-perf-reset-light-8dohe5xe/` and
`/tmp/delawson-object-perf-reset-light-vec-6vwcb1lt/`.
Each includes output, status, runner, profile wrapper, perf capture, stat.csv,
period report and summary.json. Analyzer scripts:
`/tmp/delawson-object-perf-hardware-analyze.py` and
`/tmp/delawson-object-perf-analyze.py`. These are temporary files.

## Two retained leaves with the reset fast path, 2026-09-24

Changed only common/src/wire/buffer.rs RETAINED_LEAVES from one to two,
keeping the uncommitted cheap-reset and first-append changes. Retention remains
lazy: a buffer that grows to two leaves keeps those leaves and their index
block (three 64KiB blocks total); one-leaf buffers still keep just one block.
The existing single-leaf cheap-reset branch still applies while only one leaf
has been allocated; a retained two-leaf tree uses general reset/prune logic.

Standard detached 20M-object workload, flags 23, 16 clients x depth 16,
three five-second lookup trials:

| Workload | One leaf, previous repeat | Two leaves | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 83,601 | 82,498 | -1.3% |
| BatchObjectInfo | 500,270 | 537,665 | +7.5% |

ObjectInfo trials: 81,968, 82,794, 82,732 (1.0% spread). BatchObjectInfo:
532,950, 537,197, 542,848 (1.8%). This run is reasonably consistent internally,
but prior one-leaf sessions varied substantially; no convincing ObjectInfo
improvement, and the batch result is promising rather than a controlled estimate
of the gain. All creates completed, all lookups were answered/found, and sample
response sizes/counts are unchanged. Runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-v_s6hg5e/` (temporary files).

The focused bounded-retention test passed (including one/two/four retained
leaves and ownership reclamation), as did git diff --check. The preceding
full workspace test run covered the unchanged reset/append implementation.
Leave retention at two leaves for this experiment. No commit yet.

## Four retained leaves, 2026-09-24

Changed only RETAINED_LEAVES from two to four. Full retained capacity is four
64KiB data leaves plus one index block (320KiB); allocation/retention remains
lazy. The measured 219,052-byte BatchObjectInfo response fits in four data
leaves, though a transport batch can contain multiple responses.

Same standard detached workload and three trials:

| Workload | Two leaves | Four leaves | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 82,498 | 82,447 | -0.1% |
| BatchObjectInfo | 537,665 | 543,781 | +1.1% |

ObjectInfo trials: 82,253, 82,648, 82,439 (0.5% spread). BatchObjectInfo:
541,932, 539,678, 549,732 (1.8%). Essentially unchanged ObjectInfo; the small
batch increase is within the scale of run variability. All 20M creates and
lookups succeeded, all lookups answered/found, response sizes/counts unchanged,
runner exited 0 after cleanup. Logs:
`/tmp/delawson-object-bench-wm9yc9p2/` (temporary files).
Existing bounded-retention validation covers four leaves; git diff --check
passed. Leave retention at four for this experiment, uncommitted.

No-op behavior inspected, not changed in this measurement:
- Reset bypasses prune only for a single retained leaf. A two/four-leaf retained
  tree still invokes prune/retain, although retain can immediately return the
  unchanged root without walking/releasing children.
- VirtualScratch reserves its virtual range persistently but NOT its aliases.
  Each multi-leaf callback rebuilds aliases using mremap; the callback guard
  removes them with a fixed PROT_NONE anonymous mmap afterward. Identical leaves
  on the next call therefore still cause real kernel mapping work. Single-leaf
  crypto/compression fast paths bypass aliasing. Retention alone does not remove
  this repeated mapping work; caching aliases requires a separate change.

## Temporary per-segment compressed-size logging, 2026-09-24

With four retained leaves, temporarily added opt-in DELAWSON_LOG_BATCH_SIZES
logging after successful segmented sealing: plaintext bytes, stored payload
bytes (LZ4 output or original bytes if uncompressed), final wire bytes, and a
compression flag. Stored payload excludes the mode byte, optional four-byte
original length, 12-byte header and 16-byte authentication tag. Wire bytes include
all of those. A single line describes one sealed transport segment, not one
object or one application message. Source instrumentation was removed after
the capture; original and instrumented seal.rs copies are preserved in the run
directory. Normal source contains no size logger.

Used the standard detached 20M-object benchmark, adding byte-offset phase markers
for process stderr logs before ObjectInfo, after ObjectInfo and after
BatchObjectInfo. The statistics below aggregate FE/DN/SN sends in those windows,
including both requests and responses; exclude client/KS/EN. A phase-boundary
line may be omitted if only partially written at the marker. All creates and
lookups succeeded, all lookups answered/found, and cleanup exited 0.

| Phase | Transport segments | Plaintext p99 / max, bytes | Stored payload p99 / max, bytes | Final wire p99 / max, bytes |
| --- | ---: | ---: | ---: | ---: |
| ObjectInfo | 1,426,482 | 1,160 / 4,408 | 325 / 1,000 | 358 / 1,033 |
| BatchObjectInfo | 271,198 | 219,052 / 986,762 | 34,278 / 142,707 | 34,311 / 142,740 |

ObjectInfo: only 12.5% of segments used LZ4 (most fall below the 256-byte
compression threshold). No plaintext or final wire segment exceeded 64KiB.
BatchObjectInfo: all observed segments used LZ4. 4.04% of plaintext segments
exceeded 64KiB, 3.09% exceeded 128KiB, and 0.06% exceeded 256KiB. Only 71
final wire segments exceeded 64KiB (0.026%), two exceeded 128KiB, none exceeded
256KiB. Mean final wire sizes: ObjectInfo 164 bytes, BatchObjectInfo 3,411 bytes.

For FE segments containing exactly the known 219,052-byte plaintext response,
7,577 observations compressed to 33,595–34,900 payload bytes (mean 34,199).
Thus the usual single BatchObjectInfo response fits one leaf on the wire,
while its plaintext uses four. FE's largest plaintext batch was 438,104 bytes;
DN's largest was 986,762. The 219KB response size is not the maximum batch size.

Compressed size alone does not determine mapping/retention requirements:
compression reads plaintext, decompression writes plaintext, and the current
compression-output path reserves LZ4's worst-case capacity before truncating to
actual output size. These can span leaves even when compressed data fits one.
The ObjectInfo observations suggest multi-leaf aliases are not a frequent path
there, but this logging run does not prove their rate in normal uninstrumented
runs or directly count mapping operations.

Logging itself is significant for the small-message phase: ObjectInfo averaged
26,532/s versus the preceding uninstrumented 82,447/s. BatchObjectInfo averaged
470,010/s with 15.5% trial spread. Do not use these as performance results.
Process stderr plus runner output totals about 140.5MiB including startup and
population, and per-segment logging may alter batching itself. No logging was
added to BatchObjectInfo's application response formatting.

Artifacts: `/tmp/delawson-batch-size-3qz5i4qe/` contains raw logs, before/after
phase offsets, original/instrumented source, runner, analyze.py, sizes.json
(per-role/aggregate mean, p50/p95/p99/max and threshold rates), and analysis.txt.
All temporary. Existing retention/reset changes remain uncommitted; this task
adds only this measurement record to the final source diff.

## Buffer-owned persistent aliases, 2026-09-24

Each segmented buffer now owns its optional contiguous alias reservation and
cached leaf indexes. Normal serialization continues to address pool leaves
directly; the reservation is created only when a caller requests a contiguous
alias (the codecs retain their single-leaf fast paths). It initially reserves
64MiB of virtual address space, growing geometrically when required, without
allocating extra payload blocks. Callback-scoped borrows prevent concurrent
mutation/recycling. The mapping travels with its buffer, including sender swaps.

Unchanged requested leaves incur no mmap/mremap/munmap calls. Newly requested or
changed leaves are mapped, coalescing consecutive backing indexes. Reset retains
cached aliases; before exposing a view, every requested slot is checked against
the live tree. Dormant aliases never expose recycled blocks. Reservation growth,
a changed rolling origin, mapping failure, or unwinding can require rebuilding
mappings. Destruction releases the reservation before returning the tree's blocks.
Codec-owned scratch windows were removed. Buffer counts and pipeline scheduling
are unchanged; this does not implement receive prefetch. Retention remains four
leaves per message buffer.

Full `cargo test --workspace` passed. Alias tests cover independent buffers,
swaps, retained reset without mapping calls, suffix growth/replacement, subranges,
rolling offsets, virtual reservation growth, callback errors/panics, and recovery
from an injected partial mapping failure. The final suffix-replacement test was
also rerun separately. Existing wire compatibility and maximum-size tests pass.

Same standard detached 20M-object benchmark, flags 23, 16 clients x depth 16,
three five-second trials per lookup workload; no temporary batch-size logging.

| Workload | Previous four-leaf retention | Buffer-owned aliases | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 82,447 | 82,005 | -0.5% |
| BatchObjectInfo | 543,781 | 558,981 | +2.8% |

ObjectInfo trials: 81,252, 82,423, 82,341 (1.4% spread). Batch trials:
557,045, 558,489, 561,409 (0.8%). Creation: 1,870,416 objects/sec.
Every lookup was answered and found; runner status 0 after cleanup. Logs:
`/tmp/delawson-object-bench-5gf6o4dp/`. This is a sequential-session comparison,
not proof that the entire modest batch improvement comes from fewer mappings.

## One retained leaf with persistent aliases, 2026-09-24

Changed only `RETAINED_LEAVES` from four to one; buffer-owned alias caching
remains enabled. Same standard detached 20M-object workload, flags 23,
16 clients x depth 16, three five-second trials each.

| Workload | Four retained leaves | One retained leaf | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 82,005 | 82,593 | +0.7% |
| BatchObjectInfo | 558,981 | 470,486 | -15.8% |

ObjectInfo trials: 82,904, 82,498, 82,378 (0.6% spread), consistent with
essentially unchanged throughput. Batch trials: 442,269, 455,868, 513,320
(15.1% spread): materially lower in this session but noisy, so the precise
regression needs replication. Large batches can again recycle leaves and change
their cached alias slots; no syscall counts were collected in this run.
Creation: 1,927,417 objects/sec. All 20M creates completed, every lookup was
answered and found, and status was 0 after cleanup. Logs:
`/tmp/delawson-object-bench-xec_zdeu/`. Code remains at one retained leaf.

## ObjectInfo pruning and alias activity, 2026-09-24

Temporary aggregate counters on the one-retained-leaf implementation, using the
standard detached 20M-object workload. Daemon status snapshots bracketed each
lookup phase; the benchmark client separately reported its lifetime totals.
Snapshots include a small amount of status-connection/background activity.
Counters measured resets, prune calls, prune changes to root/depth/base/allocated
end, all tree block returns/takes, alias views, and actual alias mapping syscalls.
No per-message logging. Counter overhead means these are activity measurements,
not a new clean performance comparison.

Across 1,239,585 successful ObjectInfo lookups (three trials):

| Counter | All daemons | Benchmark client | Total |
| --- | ---: | ---: | ---: |
| Reset calls | 18,984,745 | 5,875,512 | 24,860,257 |
| Prune calls | 4,519,623 | 996,395 | 5,516,018 |
| Prune tree-geometry changes | 0 | 0 | 0 |
| Blocks returned | 392 | 196 | 588 |
| Blocks acquired | 392 | 196 | 588 |
| Alias views | 0 | 0 | 0 |
| Alias mmap / mremap / munmap calls | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |

Thus ObjectInfo used no alias mapping at all, and its many prune calls made no
root/depth/base/allocated-range changes. The 588 block returns are all returns,
including buffer destruction, not 588 pruning events. The pattern is consistent
with connection creation/destruction: ordinary daemons each acquired/returned
four blocks for status traffic; FE0 acquired/returned 244; the client 196; KS0
80 including background traffic. This counter did not separately label frees
by caller. These data rule out repeated alias remapping as the remaining
ObjectInfo throughput gap; they do not measure socket or other kernel activity.

BatchObjectInfo exercised the counters heavily: daemons plus client made
34,410 alias views, 84,353 mremap calls mapping 84,548 leaves, 451 mmap calls,
and 408 munmap calls; 208,565 blocks were returned and 34,410 prune calls changed
tree geometry. Its larger buffers therefore do recycle and remap with one leaf
retained, unlike ObjectInfo.

All creates/lookups succeeded and runner status was 0 after cleanup. Raw logs,
phase snapshots, original and instrumented source, launcher, and `analysis.json`:
`/tmp/delawson-prune-counts-a1ewykz_/`. Temporary instrumentation was removed by
exactly restoring the saved sources; retention remains one leaf. No production
counter or logging changes remain.

## Defer thread-local RNG access until allocation, 2026-09-24

After committing buffer-owned aliases/reset changes as `f056807`, moved the
thread-local RNG lookup and mutable borrow inside the allocation callback in
six paths: MessageBuffer append, receive, append_message, append_payload, and
Wire sealing / Reader opening. Previously number generation was lazy but these
paths still accessed the thread-local RNG even when retained capacity sufficed.
No buffer geometry, retention (one leaf), or codec policy changed.

Clean standard detached run, 20M objects, flags 23, 16 clients x depth 16:

| Workload | Previous one-leaf run | Deferred RNG access | Change |
| --- | ---: | ---: | ---: |
| ObjectInfo | 82,593 | 83,721 | +1.4% |
| BatchObjectInfo | 470,486 | 538,628 | +14.5% |

ObjectInfo trials: 82,964, 84,372, 83,827 (1.7% spread). Batch trials:
536,160, 542,741, 536,982 (1.2%). The previous batch baseline had 15.1%
spread, so its large apparent gain cannot confidently be attributed to RNG
access; the ObjectInfo change is also modest relative to run variation.
Create: 1,924,885 objects/sec. Every lookup was answered and found; status 0
after cleanup. Logs: `/tmp/delawson-object-bench-nrj4sqgb/`.

Validation: `cargo test --workspace` passed on a separate run before the clean
benchmark; `git diff --check` passed. The initial standalone common test command
lacked its `testing` feature and failed compiling Reply::new_test. A first
workspace run overlapped `/tmp/delawson-object-bench-o5a60i27/` and stalled in
EN tests; it was stopped, the affected test passed in isolation, and the full
workspace rerun passed without a concurrent benchmark. The stall's cause was
not established. That overlapping benchmark is excluded from this comparison.

Deferred-RNG repeat, unchanged source and same standard workload, with no
concurrent tests: ObjectInfo **83,818/sec**, trials 83,392 / 83,983 / 84,080
(0.8% spread); BatchObjectInfo **541,042/sec**, trials 539,358 / 541,310 /
542,456 (0.6%). These are +0.1% and +0.4% versus the preceding clean run,
respectively. Creation: 1,938,044 objects/sec. All 20M creates completed and all
lookups were answered and found. Status 0 after cleanup; logs:
`/tmp/delawson-object-bench-ey9yk2yp/`. This confirms repeatability of the new
rates, though the earlier noisy batch baseline still limits causal attribution.

## Rerun the pre-RNG-change baseline, 2026-09-24

The earlier 470,486/sec BatchObjectInfo baseline was suspicious, so restored
exactly the two source files from `f056807` and ran the standard benchmark twice
with eager thread-local RNG access. No concurrent tests or instrumentation.
Both completed all 20M creates and every lookup was answered and found; status
0 after cleanup. Deferred-access source was saved and restored after both runs.

| Implementation | ObjectInfo run 1 | ObjectInfo run 2 | BatchObjectInfo run 1 | BatchObjectInfo run 2 |
| --- | ---: | ---: | ---: | ---: |
| Before: eager RNG access, fresh runs | 82,514 | 83,402 | 535,656 | 530,331 |
| After: deferred RNG access, preceding clean runs | 83,721 | 83,818 | 538,628 | 541,042 |

Means across the two sessions: before 82,958 ObjectInfo/sec and 532,994
BatchObjectInfo/sec; after 83,770 and 539,835, respectively: approximately
**+1.0% and +1.3%**. The original 470K batch baseline was unusually low; it
should not be used to attribute a 14.5% improvement to deferred RNG access.
These modest differences remain subject to run-to-run variation.

Fresh baseline trial details:

- `/tmp/delawson-object-bench-96d9n89s/`: ObjectInfo [81973, 83107, 82461]; BatchObjectInfo [533567, 532030, 541372].

- `/tmp/delawson-object-bench-mqhki8i7/`: ObjectInfo [81919, 83481, 84807]; BatchObjectInfo [529338, 531172, 530483].

## Allocation-heavy pool benchmark: RNG access frequency, 2026-09-24

Extended the existing `pool/examples/pool_bench.rs`, retaining its 4KiB pool
blocks, 256 buckets/four probes, initially empty pool capped at 1,000,000 blocks,
eight threads x 1,000,000 allocation/free decisions, seed 42. Added environment
selection `POOL_BENCH_RNG=local|held|per-alloc` (default local). Local passes the
worker's RNG directly; held borrows TLS across the entire loop; per-alloc looks
up/borrows TLS only while generating the allocation's random value, releasing
that borrow before calling the pool. Workload-choice and bucket RNGs remain
independent and identically seeded in all modes. Modes select const-generic
loop variants before timing; TLS initialization is outside timing.

Reproduce after `cargo build --release -p delawson-pool --example pool_bench`:

```bash
POOL_BENCH_RNG=per-alloc ./target/release/examples/pool_bench external256x4all 8 1000000 1000000 42
```

Substitute `local`/`held` and `inline256x4all`. Six trials per mode/layout;
rotated mode order and alternated layout order. No history, probes, payload
validation, or shared peak-live accounting in timed runs.

| Links | Worker-local mean ms | Held TLS mean ms | Per-allocation TLS mean ms |
| --- | ---: | ---: | ---: |
| External | 85.764 | 86.916 | 85.756 |
| Inline | 88.623 | 91.047 | 88.946 |

Per-allocation versus held TLS was 1.3% faster externally and 2.3% faster
inline in these samples, not a demonstrated regression. Versus worker-local,
per-allocation was effectively equal externally and 0.4% slower inline.
Do not interpret the small speedups as a general advantage: code generation,
scheduling, and cache behavior can differ. Holding the borrow across the whole
loop is an amortized eager-access comparison, not an exact recreation of message
boundaries. The production pool implementation itself is unchanged.

All 36 trials performed exactly 4,003,439 successful allocations and matching
frees (including final held-block cleanup), with zero failures. Issued blocks
vary with interleaving despite identical per-thread random sequences:

| Links / RNG mode | Mean issued blocks | Range | Timing range, ms |
| --- | ---: | ---: | ---: |
| external / local | 8038 | 7217–8552 | 84.283–87.550 |
| external / held | 7958 | 7096–8440 | 85.075–88.817 |
| external / per-alloc | 7848 | 7219–8279 | 84.744–86.836 |
| inline / local | 8012 | 7892–8211 | 87.220–90.431 |
| inline / held | 8095 | 7505–8457 | 90.532–91.852 |
| inline / per-alloc | 8032 | 7061–8535 | 86.261–90.726 |

Raw JSON, log, detached runner, status 0:
`/tmp/delawson-pool-rng-6tlnlu4c/`. Deferred-access message code remains in place;
the example now retains these optional RNG comparison modes. Release build and
`git diff --check` passed. No production pool code was changed.

## Small payload and segmented field CPU experiments, 2026-09-25

Sequential standard detached runs: 20M objects, batch 1,000 creation, 16 clients
with depth 16, detail flags 23, then three ObjectInfo and BatchObjectInfo trials.
All creates completed, every lookup was answered and found, sample responses
remained 232 and 219,052 serialized bytes, and every runner exited with status 0
after cleanup. Rates are three-trial means in lookups/s.

| Variant accumulated from previous row | ObjectInfo | BatchObjectInfo | Run directory |
| --- | ---: | ---: | --- |
| Small finished-payload direct write and direct slice reader | 84,162 | 538,292 | `/tmp/delawson-object-bench-du6gzot9/` |
| Small decoded payload copied directly into Vec | 84,783 | 554,606 | `/tmp/delawson-object-bench-7sf6_lm_/` |
| SegmentedWriter caches next pointer, first run | 86,632 | 554,116 | `/tmp/delawson-object-bench-sj8zzg8m/` |
| Same, repeated | 84,247 | 553,593 | `/tmp/delawson-object-bench-4mnak4ld/` |
| Direct SegmentedWriter::write_all | 84,607 | 546,703 | `/tmp/delawson-object-bench-dhqux1jo/` |
| Fixed-slice StructReader with one offset update, first run | 84,735 | 496,918 | `/tmp/delawson-object-bench-hk33qdq_/` |
| Same, repeated | 84,427 | 471,801 | `/tmp/delawson-object-bench-g5hbk0su/` |

The direct small decode removes MessageBytes construction, slow first write,
and finished-payload conversion for each small received field. Its batch rate
rose about 3% from the preceding variant in this sequence; ObjectInfo varied
within the normal range. The cached pointer and explicit `write_all` showed no
reproducible benefit and were removed. The fixed-slice reader checked a proposed
offset against the clipped leaf limit and updated only that offset per field,
but its dynamic slice indexing was much slower on BatchObjectInfo; it was
reverted. The first fixed-slice batch run spread 15.5%; the repeat spread 2.5%
and confirmed the regression. The final source retains the two small-payload
changes and the original segmented writer and StructReader field loop. Workspace
tests passed for each implementation before benchmarking.

After the measured runs, the small decode path was adjusted to skip a second
`OnceLock` configuration lookup when wrapping the already size-checked Vec in a
finished payload. Per-field serialization and parsing use no atomics; the
configuration lookup happens at payload setup, while pool atomics are reached
only on block allocation and release. This final lookup adjustment passed
workspace tests but was not separately throughput measured.

## Reset fully consumed message batches, 2026-09-25

Earlier temporary counters found 5,516,018 prune calls and zero tree geometry
changes during an ObjectInfo benchmark. One frequent source is the reader's
`discard_before(end)` after its last message in a batch, while the absolute
end still named the old offset. The reader now calls `MessageBuffer::clear`
there, resetting start and end to zero. `SegmentedBuffer::reset` already skips
pruning when only the retained first leaf remains. A partially consumed batch
still uses `discard_before`, and generic pool pruning is unchanged. The socket
test now checks the zero-based empty state after the final message, including
large messages and each sealing mode.

A general segmented-buffer shortcut was tried first, then removed in favor of
the reader reset. That intermediate run measured 84,837 ObjectInfo/s and
548,949 BatchObjectInfo/s (`/tmp/delawson-object-bench-m_87fnti/`); it is not
the final implementation. The first run of the reader reset varied widely:
77,817 and 533,984/s, with 14.9% and 9.6% trial spreads
(`/tmp/delawson-object-bench-2_3z25v5/`). The repeated standard 20M-object run
measured 84,808 ObjectInfo/s (84,215–85,277, 1.3% spread) and 540,257
BatchObjectInfo/s (536,156–543,908, 1.4% spread). All creates completed,
every lookup was answered and found, and cleanup finished with status 0.
The reset removes a no-op prune on fully consumed single-leaf batches, but
these rates do not show an end-to-end speedup. Repeated run directory:
`/tmp/delawson-object-bench-k80pzt_o/`.

## Shared request/response wire helpers, 2026-09-25

The request and response paths now share header encoding/decoding and the
framed reader's decode/discard lifecycle. Request payloads still use the
header `Op` as their structure tag only when it matches the payload's `Op`;
response structures always carry their own `Op`. The standard 20M-object run
completed with every lookup answered and found, and cleanup status 0.

| Run | Create, batch 1,000 | ObjectInfo | BatchObjectInfo, batch 1,000 |
| --- | ---: | ---: | ---: |
| First | 1,883,808 | 84,681 | 523,336 |
| Repeat | 1,880,877 | 85,361 | 554,024 |

Rates are objects or names per second. The first batch run included a low
477,790/s trial (13.2% spread), so its mean is noisy. The repeat ObjectInfo
trials were 83,927–87,239/s (3.9% spread); BatchObjectInfo trials were
552,802–555,526/s (0.5% spread). The repeated rates are in the same range as
other recent runs, without an evident throughput regression. Logs:
`/tmp/delawson-object-bench-hiojz6la/` and
`/tmp/delawson-object-bench-egjlxj0c/` (temporary files).

## Inline response payloads, 2026-09-25

Responses now hold one `Arg` inline. Responses with multiple pieces put those
pieces inside a tagged group; `ListExtents` and `BatchObjectInfo` have explicit
outer response tags. The response wire format changed, with no compatibility
requirement for an earlier release. The standard 20M-object run created all
objects and found every lookup; cleanup ended with status 0.

| Workload | Three-trial mean | Trial range | Spread |
| --- | ---: | ---: | ---: |
| ObjectInfo | 84,430/s | 83,900–85,124/s | 1.4% |
| BatchObjectInfo, batch 1,000 | 548,252/s | 544,666–551,088/s | 1.2% |

Create, batch 1,000: 1,928,537 objects/s. ObjectInfo sampled response: 235
serialized bytes; BatchObjectInfo sampled response: 219,055 bytes, both including
the four-byte frame. These are three bytes larger than the preceding run because
the outer group adds a tag and `Op` ahead of the argument-count prefix. The rates
are close to the preceding repeated run (85,361/s and 554,024/s), but a single
sequential pair does not establish a throughput difference. Logs:
`/tmp/delawson-object-bench-iwmzf8eu/` (temporary files).

## Single tagged request payload, 2026-09-25

Requests now hold one tagged payload. The payload begins with its `Op`; the
request header carries only the correlation ID and recursion-depth limit. The
standard 20M-object run created every object, found every lookup, and exited
with cleanup status 0.

| Workload | Three-trial mean | Trial range | Spread |
| --- | ---: | ---: | ---: |
| ObjectInfo | 83,989/s | 83,451–84,586/s | 1.4% |
| BatchObjectInfo, batch 1,000 | 543,410/s | 539,076–545,629/s | 1.2% |

Create, batch 1,000: 1,888,953 objects/s. Sampled responses were 235 and
219,055 serialized bytes, including the four-byte frame. The rates are close
to the previous response-only run (84,430/s and 548,252/s). Log:
`/tmp/delawson-object-bench-cvvhf_10/` (temporary files).

## Op-driven payloads, 2026-09-25

`Arg` is a struct with an `Op`; the Op's group trait selects the wire layout,
so individual values no longer carry a separate kind byte. Optional
soft-deleted reads put their flag inside the request's positional payload.
The standard run created all 20M objects, every lookup was answered and found,
and detached cleanup exited 0.

| Workload | Three-trial mean | Trial range | Spread |
| --- | ---: | ---: | ---: |
| ObjectInfo | 84,770/s | 83,991–85,332/s | 1.6% |
| BatchObjectInfo, batch 1,000 | 538,036/s | 534,014–541,087/s | 1.3% |

Create, batch 1,000: 1,896,121 objects/s. Sampled responses were 231 and
216,053 serialized bytes, including the four-byte frame. Rates are close to
the preceding run (83,989/s and 543,410/s). Log:
`/tmp/delawson-object-bench-exz313s_/` (temporary files).

## Compact Arg payload storage, 2026-09-25

Group children now occupy the payload storage slot instead of a separate
`parts: Vec<Arg>` field on every Arg. The Op trait still selects the layout,
and groups still splice owned child payloads without copying their bytes.
The standard 20M-object run created every object, found every lookup, and
finished cleanup with status 0.

| Workload | Three-trial mean | Trial range | Spread |
| --- | ---: | ---: | ---: |
| ObjectInfo | 84,471/s | 84,013–84,911/s | 1.1% |
| BatchObjectInfo, batch 1,000 | 543,096/s | 541,090–545,367/s | 0.8% |

Create, batch 1,000: 1,931,188 objects/s. Sampled responses remained 231
and 216,053 serialized bytes, including the four-byte frame. Rates were in
the same range as the previous run (84,770/s and 538,036/s). Log:
`/tmp/delawson-object-bench-pu8dypd4/` (temporary files).

## Hadoop HDFS ObjectInfo comparison, 2026-09-27

To compare single-object metadata lookups, a one-machine Hadoop 3.5.0 HDFS run
used `FileSystem.getFileStatus(path)` (HDFS `getFileInfo`), the closest analogue
to DeLawson `ObjectInfo`. `BatchObjectInfo` was not tested. Hadoop was run with
Java 17, replication 1, and 64 NameNode RPC handlers.

The saved client source is
`/home/bob-jenkins/hadoop-objectinfo-bench-20m/results/HadoopObjectInfoBench.java`;
the compiled jar is next to it. It creates 20,000,000 completed zero-byte files
under `hdfs://localhost:9000/objectinfo-bench-3pxfek1b`, arranged as 1,000
numeric shard directories of 20,000 files each. Creation opens and closes each
file without writing data, using 16 FileSystem clients and 16 workers per client
(256 workers total). For measurement, the same 16-by-16 concurrency chooses
random object IDs, calls `getFileStatus`, and verifies each result is a file of
length zero. It runs one warmup followed by three 5-second trials.

| Phase | Result |
| --- | ---: |
| Create 20M empty files | 2,549.399 s; 7,845 files/s |
| Random `getFileStatus` warmup | 35,337 lookups/s |
| Trial 1 | 42,958 lookups/s |
| Trial 2 | 42,271 lookups/s |
| Trial 3 | 42,978 lookups/s |
| Three-trial mean | 42,736 lookups/s; range 42,271–42,978; 1.7% spread |

Every measured lookup returned an existing file with length zero. The run did
not record RPC byte counters or packet captures. The previously discussed
rough estimate of 110–120 bytes per request-plus-response protobuf excludes
Hadoop IPC framing/headers and TCP/IP, so it is not a measured on-wire size.

### Retained namespace and cleanup

The Hadoop cluster is stopped. Preserve the completed namespace; do not format
its storage directories or rerun the client's `create` mode. The saved data and
run files are at `/home/bob-jenkins/hadoop-objectinfo-bench-20m`:

- HDFS namespace: `/objectinfo-bench-3pxfek1b`.
- NameNode metadata and edit logs: `storage/name` (about 6.0 GB at capture).
- DataNode storage: `storage/data` (44 KB; the objects are empty).
- Configuration, start/stop scripts, client source/jar, and results: `conf/`,
  `start.sh`, `stop.sh`, and `results/`.
- Total saved directory size was about 6.5 GB.

The NameNode console log was compressed losslessly to
`results/namenode.out.gz`: 10,758,371,249 bytes expanded and 548,687,186 bytes
compressed (about 19.6:1, or 94.9% smaller). The raw uncompressed log is not
retained separately. HDFS edit logs in `storage/name` are required to recover
the namespace. No SecondaryNameNode or CheckpointNode was run; plan a checkpoint
before continued metadata-writing use.

For a same-input codec comparison, streamed the retained gzip log through its
decompressor and recompressed the expanded text with each codec, counting output
bytes to `wc` rather than writing another large file. These are single-threaded,
level-1 CLI runs; elapsed time includes reading/decompressing the source gzip,
compressing the text, and counting bytes. The retained gzip file is 52 bytes
larger than the timed gzip recompression, due to gzip metadata.

| Codec | Elapsed | Compressed bytes | Expanded/compressed ratio |
| --- | ---: | ---: | ---: |
| gzip -1 | 35.63 s | 548,687,134 | 19.61:1 |
| Zstandard -1 | 28.55 s | 442,072,114 | 24.34:1 |

On this log, Zstandard's output was 19.4% smaller and its end-to-end pass took
19.9% less time than gzip. This compares whole-stream CLI output; DeLawson's
logger uses Zstandard level 1 in independent blocks with its own 8-byte block
headers, so its on-disk log ratio can differ.

To restart the saved namespace and remeasure without recreating objects:

```bash
cd /home/bob-jenkins/hadoop-objectinfo-bench-20m
set -e
trap './stop.sh' EXIT
./start.sh
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 HADOOP_CONF_DIR=$PWD/conf \
  /opt/hadoop/bin/hadoop jar results/hadoop-objectinfo-bench.jar \
  HadoopObjectInfoBench measure /objectinfo-bench-3pxfek1b
```

The `measure` mode only performs lookups. The existing root check in `create`
mode refuses to populate over this namespace; retain that guard so the 40-minute
creation is not repeated.

## Block-pool allocation tuning, 2026-09-28

The pool can issue more blocks than are simultaneously in use because its
bucket-local free lists leave some blocks temporarily unavailable to a given
allocation. This is a space leak in the sense that unused blocks accumulate as
slack, but it has not grown in proportion to total pool operations: extra issued
blocks were nearly proportional to peak block use and nearly independent of
operation count. As unused slack builds up, allocations are faster and fresh
block issuance falls sharply, so the growth slows rather than accumulating at a
constant rate per operation.

The current tradeoff is 256 buckets, 4 probes per allocation, reclaiming each
probed bucket's inactive deleted list whenever its free list is empty, and
growing by only the one block needed. Delete-side reclaim is disabled. Having
far more buckets than logical CPUs reduces atomic contention. Eight probes
were slower but produced fewer unused blocks; the difference in unused-block
percentage between 8 and 4 probes narrowed slowly over longer runs.

The 512-million-operation mixed test (8 threads, 64 million operations each)
showed batch growth's tradeoff: starting empty, growth by 1 issued slightly
fewer blocks but ran about 7% slower than growth by 4. With 70,000 blocks
prepopulated, the two growth settings had indistinguishable throughput, while
growth by 1 issued fewer additional blocks. This points to the amount of slack
left by fresh growth as the main difference, rather than the cost of probing
four buckets before growing. The benchmark was restored to start with zero
blocks afterward.
