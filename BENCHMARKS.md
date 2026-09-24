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
