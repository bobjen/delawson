# DeLawson

An open source distributed store, designed by Bob Jenkins, written by AI in Rust. 
Perhaps for shared AI memory. Billing will be by physical storage, proportional 
to costs. Automatic compression and encryption. Objects can contain up to 2^64 records.

Unfinished. Do not depend on. Expect frequent backward-incompatible changes. Garbage 
collection, replication, erasure coding, user accounts, caching, 
ring balancing, keyed objects, compute et cetera are still unimplemented. No hardening 
done. Currently implementing EN persistence. You can see what is implemented, and what
is designed, but this will remain a single-person development until all the 
major pieces are in and working.

Named after George DeLawson Lowry (b. 17 January 1880, Hagersville, Ontario;
d. 1919, Buffalo, New York) — a piano teacher who
played jazz in the style of Gershwin. He went by **Laws**, short for DeLawson.
See [his page](https://burtleburtle.net/gene/glowry18800117.html).

## Overview

A single DeLawson object holds up to 2^64 records. A cluster can have up to 2^64 machines.
Record numbers are stable. An object is decomposed through four levels:

```
object  ──▶ metadata + list of streams      (cumulative record count per stream)
stream  ──▶ list of extents                 (cumulative record count per extent)
extent  ──▶ list of instances               (replicas; erasure-coded once sealed)
instance ─▶ contiguous block of data on disk
```

Those levels are split across three node types:

```
DN  Directory Node   directory hierarchy, object metadata, stream definitions
SN  Stream Node      extents per stream, instances per extent, erasure coding
EN  Extent Node      stores instances: the encrypted compressed bytes on disk
```

Around those sit `FE` (Front End: authorization and coordination), `KS` (Key
Store: the daily master keys), the `laws` client (compression, interface,
connection to FE), and eventually Compute machines and ring management. User 
code runs only on Compute. Decryption keys never go on machines where user code runs.

The DN tier will be self-hosted: directory and object metadata live in DeLawson
keyed objects, reached through a small bootstrap root that is the only state
stored outside the object hierarchy. (Currently it's just another Raft ring.)

### Two object flavors

- **Sequential** — variable-length records, appended only at the end,
  addressed by 64-bit record number.
- **Keyed** — records ordered by sorted key, insertion permitted in the
  middle. Not started, will be implemented on top of sequential.

Since it is open source, a storage cluster can run inside a compute cluster, sharing network.
Stable indexes are possible, referencing 
(object, record number) for sequential objects and (object, key) for keyed objects.

## Details

Implementation in progress. See [DESIGN.md](DESIGN.md) for the architecture. Github issues are
being used to track bugs I didn't want to address quite yet. For the standard 
20-million-object ObjectInfo/BatchObjectInfo benchmark, see [BENCHMARKS.md](BENCHMARKS.md).
On a single 16-core machine running all processes, batch object creation can do 710K/sec, 
object info can do 83K/sec, and batch object info can do 525K/sec. That's parallel throughput,
not serial.

```sh
scripts/start-laws              # build debug, start every process in the roster
laws --to KS0 ping              # "KS0 is alive"
laws registry                   # what KS knows: identity -> machine, port
laws placement                  # which spreading rules this cluster can enforce
scripts/bench-laws --clients 8  # start release, measure ping, stop
scripts/stop-laws               # stop everything, either flavour

laws create /bob/notes --flavor keyed   # laws -> FE -> DN -> SN
laws object-info /bob/notes             # laws -> FE -> DN
laws log-histogram --to DN0           # what DN0 is mostly saying
laws log-histogram --path /saved/logs --log-name dn0  # no running cluster needed
laws log-grep "Raft" --path /saved/ks0-member0-0000000.zst
logs --dir run/DN0/logs --of dn0 grep "an error"   # or read the files directly
```

KS0 holds the only fixed address in a cluster. `start-laws` hands every other process a
port, the process announces it to KS, and everything else is found by name.

Everything sent is encrypted, and compressed, in whole batches rather than per message —
so a batch of small messages compresses even when nothing in it would on its own.
