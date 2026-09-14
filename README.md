# DeLawson

An open source distributed store, written in Rust. Perhaps for shared AI memory.
Billing will be by physical storage, proportional to costs. Automatic compression
and encryption.

Named after George DeLawson Lowry (b. 17 January 1880, Hagersville, Ontario;
d. 1919, Buffalo, New York) — a piano teacher who
played jazz in the style of Gershwin. He went by **Laws**, short for DeLawson.
See [his page](https://burtleburtle.net/gene/glowry18800117.html).

## Status

Early implementation. All five daemons exist — KS, DN, SN, EN, FE — plus the `laws`
client, the `common` library they share, and a single-box test harness. They have a
lifecycle, registration, an encrypted and compressed message path, and logging; none
of them stores anything yet.

See [DESIGN.md](DESIGN.md) for the architecture, its Open Questions section for
what is still undecided, and section 17 for what has been built.

```sh
scripts/start-laws              # build debug, start every process in the roster
laws --to KS0 ping              # "KS0 is alive"
laws registry                   # what KS knows: identity -> machine, port
laws placement                  # which spreading rules this cluster can enforce
scripts/bench-laws --clients 8  # start release, measure ping, stop
scripts/stop-laws               # stop everything, either flavour

laws create /bob/notes --flavor keyed   # laws -> FE -> DN -> SN
laws object-info /bob/notes             # laws -> FE -> DN
laws log-histogram --to DN0 --width 1   # what DN0 is mostly saying
logs --dir run/DN0/logs --of dn0 grep "an error"   # or read the files directly
```

KS holds the only fixed address in a cluster. `start-laws` hands every other process a
port, the process announces it to KS, and everything else is found by name.

Everything sent is encrypted, and compressed, in whole batches rather than per message —
so a batch of small messages compresses even when nothing in it would on its own.

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

### KS unlock development ceremony

`./scripts/start-laws` defaults to `--unlock-mode direct`: `laws` generates a
random unlock key and public verifier once under `run/unlock-direct/`. KS starts
locked with only the verifier; the script supplies the key from its private file.
`--unlock-mode shards` instead generates standard 7-of-13 Shamir shares under
`run/unlock-shards/` and reconstructs the key in the operator (`laws`) process.
Existing ceremony directories are reused, never silently replaced. These are
local development secrets, stored together for convenience.

Standalone commands (no running cluster needed for generation or recovery):

```sh
laws unlock-generate --output-dir ceremony
laws unlock-recover --share ceremony/share-0{1,2,3,4,5,6,7} --output recovered-key
laws --to KS0 unlock --key-file recovered-key
laws --to KS0 unlock --share ceremony/share-0{1,2,3,4,5,6,7}
laws unlock-generate --direct --output-dir direct-ceremony
```

Secret files are created exclusively with mode 0600 in a new 0700 ceremony
directory. Share format `dls1:set:threshold:index:value` uses hexadecimal 32-byte
set IDs and values, decimal threshold/index, and GF(256) polynomial 0x11b.
The verifier is SHA-256 of `DeLawson unlock verifier v1` followed by a zero byte
and the 32-byte random key. A verifier belongs to the trusted KS configuration;
this initial format does not bind a cluster identifier. Shares are not individually
authenticated; KS checks the recovered key against the verifier.

This step implements the unlock gate, not encrypted persistence. Master epochs
remain in memory. Machine-authenticated TLS is available as an opt-in path below;
without it the existing shared configuration wire key still protects transport.

### Optional TPM hardware testing (Linux)

Ordinary unit tests and `start-laws` do not require a TPM. The capability command below is an access probe. `scripts/test-tpm` exercises
hardware signing and external verification, not DeLawson network authentication. Hardware tests are explicitly opt-in and separate from the default suite. Pure protocol tests should use test
keys, including an untrusted identity to exercise rejection. An explicitly
requested hardware test must fail clearly if hardware/access is unavailable,
rather than silently passing or falling back to a software key.

On this development host, `/dev/tpmrm0` is owned by `tss:tss` with mode 0660,
and the operator account is not in group `tss`. The TPM device and `tpm2-tools`
are already present; access is the current blocker. Other Linux installations
may use a different group or require TPM 2.0 to be enabled in firmware and the
kernel driver loaded. Check the local setup first:

```sh
ls -l /dev/tpmrm0
id
command -v tpm2_getcap
```

For a one-command test, an administrator can give just that process the `tss`
group while keeping it under the invoking user's UID:

```sh
sudo -u "$(id -un)" -g tss -- tpm2_getcap -T device:/dev/tpmrm0 properties-fixed
```

This queries capabilities without provisioning keys. It makes no permanent group
membership, device-mode, or udev changes. The supplemental access ends when the
process exits. Local sudo policy must allow the requested user/group combination.
Prefer the resource-manager device `/dev/tpmrm0` over raw `/dev/tpm0`.
Run the opt-in hardware smoke test with temporary access:

```sh
sudo -u "$(id -un)" -g tss -- ./scripts/test-tpm
```

It requires Python 3, OpenSSL and tpm2-tools. It creates an unrestricted RSA-2048
signing key under the null hierarchy with `fixedtpm`, `fixedparent` and
`sensitivedataorigin` attributes, signs a fresh challenge, and verifies it with
OpenSSL. It also requires rejection of a changed challenge and an unrelated
public key. Private signing material remains TPM-protected; the temporary context
file is not a plaintext private key. No persistent handles or owner credentials
are used. Temporary files are deleted on normal completion or a handled failure,
and the Linux resource manager releases per-connection transient handles when
each tool exits. The test accepts only a resource-manager device, selected with
`--device` if necessary, and never globally flushes TPM objects.

This demonstrates signing with a TPM-created key, not remote attestation or
cluster admission. Trusting that public key is still a separate bootstrap step.
Do not run the whole build or test suite as root. Ordinary tests do not invoke
this script, and it does not silently skip when hardware is unavailable.

Hardware tests should use transient objects and clean up their own resources;
they must not clear the TPM, change hierarchy authorization, or overwrite
persistent objects used by other applications. A software TPM can later supply
an optional integration-test backend on machines without hardware; it does not
establish hardware-backed protection.

References: [tpm2_getcap](https://tpm2-tools.readthedocs.io/en/stable/man/tpm2_getcap.1/)
and [TPM transport selection](https://github.com/tpm2-software/tpm2-tss/blob/master/doc/tcti.md).

### Machine authentication and enrollment

`messaging.authentication.members` enables TLS 1.3 with pinned RFC 7250 raw public
keys. Each entry contains `public_key` (hex DER SubjectPublicKeyInfo) and `services`
(the process identities that physical host may run). Clients check both the key
and the requested service identity. TLS verifies proof of possession and supplies
forward secrecy; there is no certificate-expiration dependency, TLS 1.2 fallback,
session resumption, or early data. The existing wire framing and inner encryption
remain inside TLS. This path has not yet been performance-tuned.

Enrollment is supplied by trusted bootstrap configuration for now. A machine's
private credential is supplied separately through `machine_identity` in
`process.toml`. The `laws --machine-identity FILE` option supplies a local machine
credential when needed. Omitting it makes the operator anonymous: an anonymous
operator can unlock KS by presenting the right unlock key. KS requires an enrolled
machine for registration, checks that it may host the claimed service, and permits
SN-key requests only from machines enrolled for an EN in the roster. This checks
EN admission, not yet per-SN placement permissions. User authorization, mutable
membership through consensus, and automatic ring re-unlock are still future work.

Software identity and full local-cluster exercise (no TPM required):

```sh
./scripts/machine-identity --software --output-dir run/host-identity
./scripts/start-laws --machine-identity run/host-identity
./target/debug/laws --config run/cluster.toml --to KS0 status
```

Create the parent `run/` directory first if it does not exist. `start-laws` admits
that one host for all configured services, writes only its public enrollment into
`run/cluster.toml`, and supplies its local identity file to each process. It does
not modify the checked-in cluster configuration. Other deployments must assign
the correct subset of services to each machine. Keep using the generated cluster
configuration when running `laws`; `--at` also requires `--to` so the expected
service can be authenticated. Without `--machine-identity`, `start-laws` retains
the existing fast development transport.

The identity generator refuses to overwrite an existing directory. Software
identity files contain development private keys. A TPM identity instead stores
public key data and random unique input for an owner-hierarchy primary key:

```sh
sudo -u "$(id -un)" -g tss -- ./scripts/machine-identity --tpm --output-dir run/tpm-identity
```

The signing template explicitly uses `rsa2048:rsapss-sha256:null`: a signing-only
key requires a null symmetric algorithm, not the tools' implicit AES default.
The TPM signer recreates the same RSA-PSS/SHA-256 primary from that input and the
TPM's owner seed for each handshake, verifies its public key against enrollment,
and signs without exporting the private key. This survives process and ordinary
machine restarts; clearing/replacing the TPM or losing the unique input requires
explicit re-enrollment. No persistent TPM handle is created. If the owner
hierarchy has authorization, pass `--owner-auth FILE` when generating the identity;
the local identity then references that file. This initial implementation invokes
tpm2-tools per handshake and is intentionally simple rather than optimized.
Daemon processes using it need resource-manager access for their lifetime; a
one-command capability probe does not grant later processes access.

Validation commands:

```sh
cargo test --workspace --features delawson-common/testing
cargo build --workspace
./scripts/test-machine-auth
sudo -u "$(id -un)" -g tss -- ./scripts/test-machine-auth --tpm
```

The default integration script uses software identities. `--tpm` exercises the
same TLS/operator/KS/EN path with a real TPM signer. Both start isolated temporary
processes and check anonymous unlock, authenticated key issuance, successful
registration, anonymous key-request rejection, and rejection of an enrolled host
impersonating another host's EN. They stop their own processes and remove their
temporary files. No system-wide permissions are changed. The smaller
`scripts/test-tpm` remains useful for diagnosing device/signing support alone.

References: [rustls raw-public-key verification](https://docs.rs/rustls/latest/rustls/crypto/fn.verify_tls13_signature_with_raw_key.html),
[TLS 1.3](https://www.rfc-editor.org/rfc/rfc8446), and
[TPM primary creation](https://tpm2-tools.readthedocs.io/en/latest/man/tpm2_createprimary.1/).

## Byte-stream objects

Use `bytes` framing for ordinary file contents, with no visible record delimiters:

```sh
target/debug/laws append-block /file --framing bytes --file input.bin
target/debug/laws read-bytes /file --offset 17 --length 4096 --output slice.bin
```

`read-bytes` writes raw binary to stdout when `--output` is omitted. A read at or
past EOF succeeds with no bytes; a read crossing EOF returns the available suffix.
Each append block counts as one record, even an empty append. FE stores a hidden
four-byte length before each block. `read --record N --framing bytes` retains the
existing textual CLI record display; `read-bytes` strips all hidden framing.
Canonical lengths count payload bytes, with record counts tracked separately.
ObjectInfo adds the object framing overhead; bytes mode adds none. Record counts
still include empty appends. Counts-only sizes use existing DN caches: request `object-info /file
--want extents` to refresh them after sealing, then `--want counts` for totals.

Byte offsets survive appends and storage coalescing, which preserves append record
boundaries. Length-changing updates or deletes shift subsequent byte offsets.
Objects can exceed the byte-offset range: aggregate logical size still saturates
at `OLoff::MAX`, while record access remains available. The top `u64` value retains
its existing no-value-sentinel meaning.

Byte reads walk extent locators and scan preceding append-block headers at EN.
A header supplies the canonical payload length directly, so skipped payloads
are neither decrypted nor sent to FE. Only overlapping
blocks are fetched and decompressed. Responses contain at most 1 MiB of requested
bytes; the CLI repeats requests for larger reads. Header lookup is still linear
and repeated requests can revisit headers; a prefix index and efficient
continuations remain future work.

Run the isolated integration test with `cargo build --workspace` followed by
`scripts/test-bytes`. It uses temporary configuration, ports and data, with one DN
for the existing same-DN concat path, and cleans up its processes afterwards.

### KS persistence and Raft

KS stores encrypted master keys and activation seconds in `data_dir/master-keys/`.
Preserve that directory and the matching operator recovery material. Both snapshots
and journals keep key payloads encrypted under the unlock key; consensus metadata
remains readable while locked. Keep KS recovery files only on physically protected
KS machines, never in DeLawson.

The first key is usable immediately after commitment and application. Later keys
wait `master_key_delay_secs` (default 3600, including start-laws). Normal startup
requires unlocking; `require_unlock = false` uses a known development key.
Changing operator recovery material does not re-encrypt an existing store.

Without explicit Raft members, each KS process is an independent singleton.
For a replicated ring, configure each member with the same map and ring name,
a distinct process ID/data directory, and the required machine enrollment:

```toml
[raft]
ring = "KS0"
heartbeat_ms = 75
election_min_ms = 150
election_max_ms = 300
primary_wait_ms = 150
request_timeout_ms = 1000
[raft.members]
KS0 = "127.0.0.1:9400"
KS1 = "127.0.0.1:9401"
KS2 = "127.0.0.1:9402"
```

Require `election_min_ms >= 2 * heartbeat_ms`, `election_max_ms > election_min_ms`,
`primary_wait_ms >= election_min_ms`, and `2 * primary_wait_ms < 99 * election_min_ms`.
These are checked at startup. Use matching primary-wait settings on every member and
restart the whole ring when introducing leases or changing those settings.

Use the actual configured listen addresses. Locked members can vote and journal
entries toward commitment. An unlocked member can unlock authenticated peers.
Key reads reuse quorum heartbeat acknowledgments for a 150 ms lease measured
from send time. After election, a primary waits 150 ms and commits/applies its
current-term no-op before serving clients. A minority loses read permission when
its lease expires. The lease clock includes system suspend; safety assumes clock
rates within 1% of real time. All ring members must use the same lease protocol.
Followers forward requests to a known primary. `laws raft-members --members-file
FILE` proposes a joint-consensus transition using a JSON ID/address map. New IDs
must be admitted KS processes. Automatic ring maintenance and client bootstrap
failover remain future work; `--to` and `--at` select a member explicitly.

Recovery starts from the newest snapshot and replays later numbered journals.
Filenames use a physical journal batch sequence, distinct from the Raft log index.
Bootstrap creates snapshot zero. Rotation precedes snapshot publication and carries
any uncommitted suffix into the new journal. An incomplete final active frame can
be discarded; complete corruption fails recovery. Earlier pre-Raft formats are
refused without migration.

Snapshot defaults are one day or new journal bytes reaching the larger of the
previous snapshot size and `snapshot_min_log_bytes = 1048576`. Only changed state
is snapshotted. `snapshot_retain = 10` keeps ten snapshots and all journals after
the oldest retained snapshot. These settings are configurable.

Routine tests use direct calls and in-memory storage:

```sh
cargo test -p delawson-ks --features delawson-common/testing
```

Occasional filesystem and process integration checks:

```sh
cargo test -p delawson-ks --features delawson-common/testing -- --ignored
cargo build --workspace
scripts/test-ks-raft
scripts/test-machine-auth
```

Machine-auth tests use software identities by default; `--tpm` remains opt-in.
User authorization, unlock-key replacement, recovery cutoffs, selective execution,
and chunked snapshot transfer are deferred. The registry remains ephemeral.
See DESIGN.md §§17.48–17.51 for storage and consensus details.
