# DeLawson

A distributed store for very large objects, in Rust. See `DESIGN.md`.
Currently design-phase; no implementation yet.

## Clean-room constraint — read this first

DeLawson is being built **from first principles**. Bob worked on Microsoft
Cosmos until 2026-07-01, and a sibling project in `~/src/outlaws*` was written
partly during that employment and is contaminated with work-proprietary
information.

**Never read, search, grep, or reference anything under `~/src/outlaws*`, nor
the `github.com/bobjen/laws` and `github.com/bobjen/laws-notes` repos.**
Do not use them as a source of designs, names, constants, or file layouts.
If a design question comes up, derive the answer here or ask Bob — do not go
looking for a precedent in that tree.

Everything in this repo must be traceable to Bob's statements in conversation,
public sources, or reasoning done here.

This is enforced, not merely requested. `~/.claude/settings.json` carries deny
rules for those paths plus a `PreToolUse` hook,
`~/.claude/hooks/clean-room-guard.sh`, which rejects any tool call whose input
matches the patterns in `~/.claude/clean-room-blocklist.txt`. The guard exempts
writes whose `file_path` is under `~/src/delawson`, `~/src/delawson-notes`, or
`~/.claude`, so these docs can name the forbidden sources in order to forbid
them. `Bash` has no `file_path` and is therefore always checked — meaning even
a harmless shell command that merely mentions those names is blocked.

## Issues

GitHub Issues on `bobjen/delawson` are the project's work queue. A large project
accumulates millions of them, so filing is cheap and expected.

**File an issue whenever you notice something that should be fixed or is worth
thinking about, but acting on it now would derail what you are doing.** An issue
only has to describe the question, problem, or need. It may leave the solution
open, and it does not have to propose one.

- **#1 is "Ship DeLawson"** — the checklist of everything that must be true
  before a release is allowed. Nothing ships while a box there is unchecked.
- Issues also track large work as **checklists with sub-issues**, so a complete
  checklist means we are aware of nothing further outstanding.
- **Grep open issues before filing**, to avoid duplicates. When you do find a
  duplicate, close it *as a duplicate* pointing at the canonical issue.
- Work them ordered by **(value delivered) / (remaining work)**. Compute that
  exactly when it is easy; often it is not, and then it is not done.

**Closing an issue requires updating the documentation first.** A closed issue is
effectively forgotten, whereas `DESIGN.md` is expected to be read again for
years. If an issue's resolution is not reflected in `DESIGN.md`, the issue is not
done. `DESIGN.md` §16 — not the issue tracker — remains the canonical list of
open design questions; do not mirror it into issues wholesale.

## Notes and memory

Long-term project memory lives in the sibling repo `~/src/delawson-notes`
(`memory/`, indexed by `memory/MEMORY.md`). This is a multi-year project;
record durable decisions there rather than relying on conversation history.
