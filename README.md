# DeLawson

A distributed store for very large objects, written in Rust.

Named after George DeLawson Lowry.

## Status

**Design phase. No implementation yet.**

See [DESIGN.md](DESIGN.md) for the architecture, and its Open Questions
section for what is still undecided.

## The shape of it

A single DeLawson object holds up to 2^64 records. No machine in the cluster
is large enough to hold such an object alone, so an object is decomposed
through four levels:

```
object  ──▶ metadata + list of streams      (cumulative record count per stream)
stream  ──▶ list of extents                 (cumulative record count per extent)
extent  ──▶ list of instances               (replicas; erasure-coded once sealed)
instance ─▶ contiguous block of data on disk
```

Nothing in the system holds a per-record index. Locating record *N* is three
binary searches over cumulative counts, then one device-sized read and a short
forward scan. That is what makes 2^64 records addressable.

## Two object flavors

- **Sequential** — variable-length records, appended only at the end,
  addressed by 64-bit record number.
- **Keyed** — records ordered by sorted key, insertion permitted in the
  middle.
