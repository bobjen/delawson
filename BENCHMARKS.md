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
Report the three-trial mean, range, and spread. The five seconds cover issuing
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
