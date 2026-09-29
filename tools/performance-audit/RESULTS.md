# Lightyear Draft performance validation — 2026-09-29

## Result and scope

The default-off observation changes retain the upstream schedule structure and allocation counts. All seven measured default-path workloads meet the predeclared 5% practical regression criterion in the completed comparisons, including a separate precision run for the initially inconclusive 64-link workload. This is a local, single-task-thread elapsed-time comparison, not exact equality or a universal performance guarantee. Enabled observation is not free: packet construction with admission events costs about 44% more in this fixture.

Upstream source: `320f825bbeeab62715f0158d07d03320fb1c9cd0`. Candidate core: `20f7087` (integration of UDP observation `eec9266`, receive limit `59cfb86`, transport observation `c22acf0`). Source manifests are Lightyear 0.30.0 at that pinned commit; Bevy 0.19.1 and Rust 1.95.0. Wally runtime/vendor code and its lockfile were not changed.

Machine: MacBookPro18,1 / M1 Pro, 10 physical/logical cores, 16 GiB. Release O3, thin LTO, one codegen unit; identical 171 registry-package versions/checksums in all variants. One task-pool thread, no Bevy multithreaded feature. No OS or socket-buffer settings changed. Read defaults: SO_RCVBUF 786896, SO_SNDBUF 9216 bytes. No thermal/performance warning was reported by pmset before the comparisons.

## Default path

Each primary comparison alternates baseline/candidate execution order. 21 paired processes × 2,000 measured batches follow 100 warmup batches; the precision run uses 31 pairs × 5,000 batches. The statistic is the median paired candidate/baseline ratio with a deterministic 10,000-resample percentile bootstrap 95% interval. Setup, enqueuing, receiver verification, waiting and printing are outside timing; complete send/receive schedules, deferred command application and the downstream no-op stage are inside. These are wall-clock schedule durations, not CPU time, RTT or full application throughput.

| Workload | Paired change | 95% paired change interval | Result |
|---|---:|---:|---|
| udp-single | -0.60% | -3.95% … +3.02% | within 5% |
| udp-endpoint | -0.66% | -1.68% … +0.34% | within 5% |
| udp-receive | +0.47% | -9.96% … +2.40% | within 5% |
| transport-single | +2.55% | +0.79% … +3.67% | within 5% |
| transport-many (precision run) | +2.23% | +1.23% … +2.91% | within 5% |
| empty-udp | +1.69% | -0.28% … +2.88% | within 5% |
| empty-transport | -2.17% | -3.42% … -0.44% | within 5% |

The first complete final-source run correctly returned exit 1: transport-many had median +2.32%, but its upper interval was +6.08%. That result remains in `final-default.json`. A single announced longer precision run kept the same binaries and threshold; its upper interval is +2.91%, and its gate returned 0. Results were not dropped or relabeled, and the original all-scenario gate is not reported as having returned 0.

Allocation runs are separate from timing. For 100 batches, baseline and default candidate are identical: UDP/empty schedules allocate zero; transport-single has 101 allocations / 46,440 requested bytes / 2 reallocations; transport-many has 6,464 allocations / 2,972,160 requested bytes / 128 reallocations. This is allocator-call/requested-byte accounting, not live or peak RSS. All actual datagrams and all reconstructed fragmented messages were checked; work totals match.

The receive fixture aggregates every timed nonblocking receive pass until the entire 16-datagram batch arrives, with a one-second failure deadline and untimed 1ms prefill/waits. No packet is resent and no partial batch or processing time is discarded. The final 84,000 measured receive batches needed zero extra passes. It measures aggregate receive processing, not arrival latency or one-pass readiness.

## Enabled observation cost

| Workload | Feature + marker on versus feature off | 95% change interval |
|---|---:|---:|
| udp-endpoint | +1.38% | +0.60% … +4.40% |
| transport-many | +44.33% | +41.47% … +53.73% |

These are nine paired runs of 1,000 batches using a small counter observer. For transport-many, enabled observation adds 32,000 allocations / 89,600 requested bytes over 100 batches: one owned channel-list allocation per admitted packet. UDP observation adds no steady-state allocator calls in this fixture, but it still adds command/event work. Other observers, entity counts and thread configurations can cost differently. The transport percentage is not an end-to-end network slowdown.

## Changes that preserve the default

- UDP `send_observation` and transport `packet_admission_observation` are default-off Cargo features; the facade forwards them. Disabled features remove added query fields, marker checks and Commands/ParallelCommands parameters.
- The transport default retains adaptive iteration even without its std feature. Observation-enabled no_std uses the documented serial command path.
- New system-metadata tests guard default non-deferred behavior. The actual combined PostUpdate probe now has zero inserted ApplyDeferred nodes, with or without an ordered following system; the old observer drafts had one/two. Enabled features retain their documented scheduling cost even without markers. Cargo feature unification can turn them on transitively.
- No existing upstream public signatures, default features, dependency versions, packet encoding, retry or error policy changed. The unreleased Draft APIs now require explicit features; draft consumers must opt in.

## Measurement failures and diagnostics retained

Early differing argv[0] launch paths produced apparent transport slowdowns around 64–86%. Factor controls reproduced a large difference when the same candidate executable was invoked by absolute rather than relative argv[0]; matching argv[0] and working directory removed most of it. The internal OS/allocator/compiler mechanism is not proven. The runner now selects executable separately and fixes argv[0] to performance-audit. Hash checks, output capture and timeout controls were also compared.

An upstream-function substitution and a tuple-pattern macro experiment were diagnostic only. The macro did not resolve the apparent slowdown and was reverted; no such production optimization was accepted. One-second sample profiles are diagnostic wall-stack samples, not part of the timing comparisons. Initial fixed-prefill receive fixtures failed when a batch was not ready in one pass; those failures motivated bounded complete-work accumulation, not weakening packet checks. Initial results, failures, controls and executable hashes are retained in the local run directory.

## Functional validation

- UDP observation Draft: all-features 11 unit + 2 doc passed; no-default 2 unit; p2p/server without observations 7 unit.
- Transport: current full suite 87 passed / 2 failed. The failures are the previously reproduced upstream NACK exact-duration comparisons (59.999999ms versus 60ms and 79.999999ms versus 80ms). They remain visible. Bare all-features also needs existing lz4_flex/alloc for its compression test; no manifest workaround was added.
- Transport plugin subset: default 5 / all-features 10 passed; doctests 2 passed. no_std off/on checks and all-feature/default Clippy passed.
- Default combined schedule probe and feature-enabled guide example passed. Four Python evidence/launch guard tests passed.

## Reproduce

Use the adjacent README and prepare.py with the pinned reference lockfile. Build one upstream binary, one candidate with features off, and one candidate with udp_observation,transport_observation. Save each executable separately. Then run:

```sh
python3 compare.py --baseline binaries/baseline-final --candidate binaries/candidate-default-final --iterations 2000 --repetitions 21 --require-within-threshold --output results/new-default.json
python3 compare.py --baseline binaries/baseline-final --candidate binaries/candidate-default-final --scenario transport-many --iterations 5000 --repetitions 31 --require-within-threshold --output results/new-transport-precision.json
python3 compare.py --baseline binaries/candidate-default-final --candidate binaries/candidate-observed-final --candidate-observe --scenario udp-endpoint --scenario transport-many --iterations 1000 --repetitions 9 --output results/new-enabled-cost.json
python3 -m unittest -v test_compare.py
```

The performance gate refuses an existing result path and returns nonzero for either regression or uncertainty. Do not rerun merely to obtain a passing sample; preserve failures and diagnose the method/code first.

## Evidence and limits

Local raw evidence: `/Users/gw/wally/contributions/performance-audit/results/20260929T130620Z`. Final harness SHA256: `c0a749950a2c0c1decc330be4073e54d7537d22d634cf0c05c68eaf0586a864b`. JSON records executable hashes, launch mode, raw pairs, actual work counts and allocations.

Not validated here: many-thread contention/ParallelCommands scaling, Linux/Windows, the full workspace CI, rendered mdBook, Netcode encryption, RTT/tail latency, or Wally’s 1,000-session workload. #4/#5 are documentation/design and have no runtime implementation cost. This is performance evidence for the fork Draft changes, not a new Wally capacity result.
