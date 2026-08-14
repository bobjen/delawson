# DeLawson

A distributed store for very large objects, in Rust. See `DESIGN.md`.
Currently design-phase; no implementation yet.

## Clean-room constraint — read this first

DeLawson is being built **from first principles**. Bob worked on Microsoft
Cosmos until 2026-07-01, and a sibling project in `~/src/outlaws*` was written
partly during that employment and is contaminated with work-proprietary
information.

**Never read, search, grep, or reference anything under `~/src/outlaws*`.**
Do not use it as a source of designs, names, constants, or file layouts.
If a design question comes up, derive the answer here or ask Bob — do not go
looking for a precedent in that tree.

Everything in this repo must be traceable to Bob's statements in conversation,
public sources, or reasoning done here.

## Notes and memory

Long-term project memory lives in the sibling repo `~/src/delawson-notes`
(`memory/`, indexed by `memory/MEMORY.md`). This is a multi-year project;
record durable decisions there rather than relying on conversation history.
