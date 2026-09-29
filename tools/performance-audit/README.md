# Lightyear observation performance audit

See [RESULTS.md](RESULTS.md) for the measured outcome and [REPRODUCE.md](REPRODUCE.md) for portable checkout/build commands.

This is a local comparison harness, not an upstream benchmark or a claim about
application throughput. It executes real Lightyear systems and validates their
outputs. It does not implement an alternate networking algorithm. No GUI,
renderer, async runtime, external Python package, or benchmark framework is used.

## What is measured

All variants compile the same `src/main.rs`. Only dependency checkout paths and
observation feature availability differ. `prepare.py` writes standalone manifest
variants and does not run Cargo. Keep dependency commits, lockfiles, compiler,
profile, feature flags, and final executable hashes with the results. The source
and executables must remain unchanged during a comparison.

The runner normalizes `argv[0]` to `performance-audit` and uses this harness
directory as the working directory for every variant. It selects the actual
binary with Python's separate `executable` parameter and records that path beside
the command. Diagnostic controls found materially different timings for one
unchanged executable when launched with different `argv[0]` path forms. The
internal cause is unproven; this is a launch-condition confounder, not evidence
of a library or compiler defect. Earlier unnormalized results and the controls
are retained in `results/20260929T130620Z`; use normalized paired runs for the
comparison, without treating normalization as a proven explanation of the cause.

`schedule_elapsed_ns` is **elapsed wall-clock time**, not process CPU time. Each
sample sums complete `PostUpdate` schedule executions plus `World::flush`, so
deferred observation commands and observers are included. The receive scenario
uses `PreUpdate` instead. Setup, warmup, message enqueue, result draining, payload
verification, and printing occur outside the timed region. A single-task-thread
pool configuration is fixed for all variants; these measurements do not estimate
scaling on a many-worker production server.

A downstream no-op send stage is present in every variant: it is in
`LinkSystems::Send` after Transport packetization, or ordered after UDP's send
set. This retains the schedule dependency that can introduce an automatic
`ApplyDeferred` barrier for observation commands. A benchmark with only the
producer system could otherwise miss that scheduling cost.

Every run first completes 100 untimed warmup batches. It then completes the
requested number of identical work batches (1–10,000). The processes are serial,
and the Python runner alternates baseline/candidate order across at least seven
paired repetitions. The practical regression threshold is declared in advance
as **5%**. Raw measurements, median elapsed time per batch, paired ratios, and a
deterministic 10,000-resample percentile bootstrap 95% interval are retained.
An interval crossing the threshold is inconclusive; it is not evidence of equal
performance. A small sample on one machine does not establish a universal bound.
`--require-within-threshold` makes an uncertain or excessive interval exit 1
after preserving the report; use it for default-path regression checks, not for
measuring the expected cost of enabled observers. Existing output paths are
rejected, and workload failures are recorded alongside completed samples.

| Scenario | Real work per batch | Verification outside timing |
| --- | --- | --- |
| `udp-single` | `UdpPlugin`, one socket, 16 × 512-byte sends | Receive every exact datagram on localhost; verify queue empty |
| `udp-endpoint` | `UdpEndpointPlugin`, one shared socket, 16 child peers, 16 × 512-byte sends per peer | Drain each peer socket, verify exact payloads/counts and empty queues |
| `udp-receive` | One endpoint receives a bounded batch of 16 × 512-byte datagrams from a real localhost socket across one or more timed passes | Drain `Link.recv`, require the complete batch |
| `transport-single` | `TransportPlugin`, one link, messages of 48, 97, and 2048 bytes; MTU 512 | Feed actual packets to a receiving `TransportPlugin`; reconstruct all three messages exactly once |
| `transport-many` | Same packetization, 64 links | Same reconstruction on every link |
| `empty-udp` | UDP single-peer and endpoint send systems with no socket entities | No send observations |
| `empty-transport` | Transport send schedule with no link entities | No packet-admission observations |

The UDP receive queues contain only 8 KiB per peer per batch. Receiver draining
occurs after each timed send batch. A missing datagram fails with a one-second
receive timeout rather than silently producing a faster benchmark. Extra UDP
datagrams are checked at the end of the run. Transport output includes packet,
byte, message, and checksum totals; comparisons reject different work totals.
There are no sleeps in the measured path. The runner enforces a 180-second
whole-process timeout per workload.

`udp-receive` inserts a fixed **untimed 1ms prefill pause** after sending the
batch. macOS diagnostics still found incomplete nonblocking receives after that
pause. The harness therefore runs and times complete `PreUpdate` passes until
all 16 datagrams are consumed, with a fixed untimed 1ms pause between incomplete
passes and a one-second absolute deadline. It never resends packets, accepts
partial work, or discards an empty pass's timing. Exact count and payload checks
remain mandatory. `receive_schedule_calls` and `extra_receive_schedule_calls`
record measured passes, allowing differences in readiness to remain visible.
Their times and allocations are all accumulated. This measures aggregate receive
processing cost across nonblocking passes, not one-pass readiness, receive
latency, or end-to-end throughput. Failed earlier runs are retained. The send
scenarios instead drain with blocking receive timeouts outside timing.

When observations are enabled, a small resource-counting observer validates the
event packet and byte totals. It does not clone events or store an unbounded
history. The library's own event metadata allocations and deferred command costs
remain measured. The observer does a fixed small amount of work and therefore
does not represent arbitrary application callbacks.

## Allocation accounting

All executables use the same `System` allocator wrapper. During **primary timing
runs**, its counters are disabled. There is still a uniform enabled-flag check in
the wrapper, so these binaries are not byte-for-byte production executables.

Separate `--count-allocations` runs enable global atomic counters only during
the measured schedule. Counts and requested bytes for allocations, reallocations,
and deallocations are reported separately. Reallocation bytes mean the requested
new size, not bytes copied or a net size delta. Deallocation may release storage
allocated during untimed setup/enqueue, and allocation may remain live after the
schedule. These statistics are neither RSS nor peak/live heap memory. Counted-run
elapsed times are not used in the primary timing ratio or confidence interval.

## Prepare and build

From `/Users/gw/wally`:

```sh
python3 contributions/performance-audit/prepare.py --name baseline \
  --source-root contributions/lightyear-deferral-design --api-mode upstream
python3 contributions/performance-audit/prepare.py --name old \
  --source-root contributions/lightyear-observation-docs --api-mode old
```

For the revised implementation use `--api-mode gated` and its coherent combined
checkout path. This forwards the local `udp_observation` feature to
`lightyear_udp/send_observation`, and `transport_observation` to
`lightyear_transport/packet_admission_observation`. The old implementation already
exposes its APIs without crate feature gates, so its local feature aliases are
empty. The baseline must be built without either local observation feature.

Build each variant serially, then copy the resulting executable to a distinct
immutable path before another build can overwrite it. For example:

```sh
cargo build --release --manifest-path contributions/performance-audit/variants/baseline/Cargo.toml
cargo build --release --manifest-path contributions/performance-audit/variants/old/Cargo.toml \
  --features udp_observation,transport_observation
```

After initial lockfile generation, use `--locked`. Pin identical registry package
versions across manifests; do not compare runs that changed unrelated packages.
Build a candidate with both features off to assess the default path, then a
separate executable with the relevant observation features on. `--observe`
controls runtime markers independently of compilation. The same feature-enabled
binary can compare marker absent versus marker present.

## Run paired comparisons

Using saved executables (substitute their actual paths):

```sh
python3 contributions/performance-audit/compare.py \
  --baseline /path/to/baseline-bin --candidate /path/to/candidate-default-bin \
  --repetitions 9 --iterations 1000 --output /path/to/default-comparison.json

python3 contributions/performance-audit/compare.py \
  --baseline /path/to/old-observation-bin --candidate /path/to/candidate-observation-bin \
  --baseline-observe --candidate-observe --scenario udp-endpoint \
  --scenario transport-many --repetitions 9 --iterations 1000 \
  --output /path/to/enabled-comparison.json
```

Run runtime-marker-off comparisons on the feature-enabled executable separately
from feature-disabled comparisons. Do not average those configurations together.
Do not run two harness processes simultaneously or compile while timing. Preserve
raw JSON when a result is noisy; increase independent repetitions or batch size
before interpreting an uncertain interval. This harness makes no RTT, loss-rate,
connection-capacity, Netcode-encryption, or end-to-end game-performance claim.
