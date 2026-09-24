# Working in DeLawson

Read `CLAUDE.md` for the clean-room constraint and project rules.

## ObjectInfo / BatchObjectInfo benchmarks

Read `BENCHMARKS.md` and use its detached launcher. The standard workload is
20M objects, create batches of 1,000, 16 clients × depth 16, rich flags 23,
then three trials each of batch 1 and batch 1,000. Do not substitute a smaller
workload or `scripts/bench-laws` (which benchmarks ping).

The detached runner must own startup, population, measurement, and cleanup.
Do not start a cluster in a separate tool call with an EXIT cleanup trap:
that can stop it before measurements begin. Use the recorded log/status files.

A listener's ready file does not mean KS has elected a Raft leader. `start-laws`
waits for the registrar's healthy known leader after starting/unlocking KS members
and before starting DNs. Preserve that gate. Historical checkouts may lack it;
check their startup sequence before running, rather than rediscovering the DN0
registration failure. Keep any compatibility adjustments separate from measured
source changes and report them.
