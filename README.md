# DeLawson

An open source distributed store, written in Rust. Perhaps for shared AI memory.
Billing will be by physical storage, proportional to costs. Automatic compression
and encryption.

Named after George DeLawson Lowry (b. 17 January 1880, Hagersville, Ontario;
d. 1919, Buffalo, New York) — a piano teacher who
played jazz in the style of Gershwin. He went by **Laws**, short for DeLawson.
See [his page](https://burtleburtle.net/gene/glowry18800117.html).

## Status

**Design phase. No implementation yet.**

See [DESIGN.md](DESIGN.md) for the architecture, and its Open Questions
section for what is still undecided.

## Two object flavors

- **Sequential** — variable-length records, appended only at the end,
  addressed by 64-bit record number.
- **Keyed** — records ordered by sorted key, insertion permitted in the
  middle.

Since it is open source, a storage cluster can run inside a compute cluster, sharing network.
Indexes are possible, referencing 
(object, record number) for sequential objects and (object, key) for keyed objects.

## The shape of it

A single DeLawson object holds up to 2^64 records. A cluster can have up to 2^32 machines.
An object is decomposed through four levels:

```
object  ──▶ metadata + list of streams      (cumulative record count per stream)
stream  ──▶ list of extents                 (cumulative record count per extent)
extent  ──▶ list of instances               (replicas; erasure-coded once sealed)
instance ─▶ contiguous block of data on disk
```

Those levels are split across three node types:

```
DN  Directory Node   directory hierarchy, object metadata, inline tiny objects
SN  Stream Node      extents per stream, instances per extent, erasure coding
EN  Extent Node      the bytes on disk
```

Around those sit `FE` (Front End: authorization and coordination), `KS` (Key
Store: the daily master keys), the `delawson` client (compression, interface,
connection to FE), and eventually Compute machines. User code runs only on 
Compute, and decryption keys never go on machines where user code runs.

The DN tier is self-hosted: directory and object metadata live in DeLawson
keyed objects, reached through a small bootstrap root that is the only state
stored outside the object hierarchy.
