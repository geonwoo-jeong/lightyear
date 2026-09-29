# Proposal: bounded UDP send deferral

**Status: design review draft, not an implemented feature or an upstream-approved plan.**
This document asks whether Lightyear should offer an opt-in queue for UDP datagrams
that cannot immediately be handed to the operating system. It makes no performance
claim. It does not prescribe an application's replication or movement-input policy.

The proposed first implementation would address short periods of socket
backpressure, preserve existing wire bytes, and bound the datagrams retained by
the UDP backend. It would not promise delivery under sustained overload. Before an
implementation PR, agreement is needed on the ownership boundary, overflow and
shutdown behavior, and whether pacing belongs in this change at all.

## Current send path

These observations refer to commit
[`320f825bbeeab62715f0158d07d03320fb1c9cd0`](https://github.com/cBournhonesque/lightyear/tree/320f825bbeeab62715f0158d07d03320fb1c9cd0).
Paths below are relative to the repository root.

1. `crates/transport/transport/src/plugin.rs` stages a transport packet, consumes
   the configured transport bandwidth quota, and commits packet IDs, channel retry
   timestamps, and acknowledgement tracking when it pushes the payload into
   `Link.send`. That commit point is not a successful socket write.
   [Source](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/transport/transport/src/plugin.rs#L475-L548)
2. Both Netcode plugins schedule `TransportSystems::Send`,
   `ConnectionSystems::Send`, then `LinkSystems::Send`. Netcode removes the current
   user payloads from `Link.send`, encrypts them, and appends wire datagrams to the
   **same queue**. It then appends its own control packets.
   [Client source](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/client_plugin.rs#L102-L127),
   [server source](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/server_plugin.rs#L181-L268)
3. Netcode increments its sequence and updates send timestamps when constructing
   these datagrams. Encryption derives a nonce from the packet sequence. Retrying
   an already encrypted datagram must not serialize or encrypt it again.
   [Client serialization](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/client.rs#L350-L397),
   [server serialization](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/server.rs#L636-L662),
   [nonce construction](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/crypto.rs#L70-L94)
4. `UdpPlugin::send` and `UdpEndpointPlugin::send` drain their links completely,
   call nonblocking `send_to`, and log and discard errors. The endpoint owns one
   socket shared by its child links. `ServerUdpIo` is a role marker requiring
   `UdpEndpoint`, not a separate server socket implementation.
   [Single-peer UDP](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/io/udp/src/lib.rs#L166-L187),
   [endpoint UDP](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/io/udp/src/endpoint.rs#L142-L175),
   [server marker](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/io/udp/src/server.rs)
5. `LinkSender` already exposes FIFO `pop`, `push`, and `push_front`; it has no
   capacity limit or admission result. Although `push_front` supports retrying a
   packet locally, it does not distinguish plaintext from post-Netcode bytes.
   Leaving ciphertext there until the next frame would expose it to Netcode's
   payload processing again.
   [Queue API](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/io/link/src/lib.rs#L240-L283)

## Proposed ownership boundary

Keep a private deferred queue with the UDP **socket owner**: `UdpIo` for a
single-peer socket, or `UdpEndpoint` for a shared socket. Do not store deferred
wire datagrams back in `Link.send`. Enable the behavior explicitly per socket;
absence of configuration keeps the current behavior. Names and public component
layout are deliberately not fixed by this document.

```text
channel-owned pending messages
    -> TransportSystems::Send: construct and commit transport packet
    -> Link.send: plaintext transport packet
    -> ConnectionSystems::Send: Netcode serializes/encrypts once
    -> Link.send: complete wire datagram, including Netcode control packets
    -> LinkSystems::Send: bounded UDP admission
    -> UDP-owned deferred queue: immutable wire datagram
    -> send_to: operating-system acceptance, not remote acknowledgement
```

Each retained item identifies the child link, its socket/session generation, the
destination captured at admission, the original `SendPayload`, and its enqueue
time. A single-peer socket has one such FIFO. An endpoint partitions its bounded
storage into FIFOs by live child link. Moving a payload into this queue does not
copy its contents or expose a key to UDP. The complete byte sequence, including
the Netcode packet sequence, authentication tag, and associated nonce, remains
unchanged across attempts.

Generation matching matters even when a reconnect reuses the same entity and
remote address. Old ciphertext must never be sent through a new session. A raw
UDP link without Netcode uses the same opaque-datagram rule; UDP does not parse
packet kinds to select policy.

### Bounds and admission

The opt-in configuration needs finite limits for total retained datagrams,
retained payload bytes, and maximum residence time. Count and byte bounds apply
to the **whole socket**, not independently to an unlimited number of child
queues. Metadata remains bounded by the datagram count; empty peer partitions
are removed. Zero-sized or inconsistent configurations should be rejected rather
than interpreted as unlimited. The queue holds at most the configured byte
budget in logical payload bytes, plus a single candidate currently being admitted
or sent. `SendPayload` is a `Bytes` handle, so a short payload may retain a larger
backing allocation. An exact allocation-memory cap would require buffer ownership
accounting or a bounded-storage copy policy; payload-length accounting alone must
not be advertised as that cap.

Admission uses checked arithmetic and takes whole datagrams only. It cannot split
a UDP payload, replace a queued state update, or evict an older reliable packet.
The conservative proposal is to reject an item that cannot fit and report a
terminal send failure for the contributing link, clearing that link's deferred
items. Expiry has the same explicit failure outcome. Other links remain usable.
This is an overload failure, not successful backpressure or a promise that the
reliable message was delivered. Choosing a public error/event and its connection
lifecycle mapping is a prerequisite for implementation.

Before returning from the send stage, every wire item produced for that link in
this update must have been sent, admitted, or disposed of as part of an explicit
link failure. A capacity failure must not leave a ciphertext tail in `Link.send`
for the next Netcode pass. Deferred lifecycle commands therefore need an immediate
local failed-link guard within the send stage.

These bounds cover UDP-owned deferred storage. They do not retroactively bound
an application's channel queues or a large `Link.send` batch produced earlier
in the same update. Claims of end-to-end bounded memory would need upstream
admission/backpressure work as well.

## Sending and fairness

Use the existing Bevy send stage; do not add a background thread, sleeping IO
loop, or callback that applies a global application policy. A failed attempt
remains pending for a later update. A finite per-update socket-attempt allowance
bounds retries, including repeated `Interrupted` results. This does not bound the
cost of consuming a batch already produced upstream or clearing failed items.

For an endpoint, visit active child queues round-robin, attempting at most one
head datagram per visit and retaining the cursor between updates. Admit new
items in peer rounds as well, rather than draining the first child's complete
batch into the endpoint queue. Preserve FIFO within each child. A socket-wide
`WouldBlock` stops that socket's work for the update; advance the cursor so the
same peer does not always run first when it becomes writable. Independent
sockets continue. A permanently failing peer must not block every other child.

This provides scheduling fairness among admitted work, not equal throughput or
guaranteed admission under overload. A peer that already occupies the aggregate
capacity can still prevent a new peer's admission. Per-peer sublimits or reserved
capacity would be a separate API decision; silently growing the aggregate bound
is not an acceptable solution.

The following are proposed state transitions, not an existing API:

| Current state | Event | Next state / owner |
| --- | --- | --- |
| Complete wire item in `Link.send` | Capacity available | Pending in UDP-owned FIFO |
| Complete wire item in `Link.send` | Capacity exceeded | Explicit link failure; dispose of its wire items |
| Pending | Budget unavailable, `WouldBlock`, or `Interrupted` | Pending, same bytes and generation |
| Pending | Full `send_to` success | Removed; OS owns the accepted datagram |
| Pending | Expiry, fatal error, unlink, or generation replacement | Removed with failure/teardown reason |

Conceptual pseudocode:

```text
each LinkSystems::Send, for each enabled UDP socket:
    reject expired items and purge ended/mismatched link generations
    service pending peer heads in round-robin order, within work allowance
    admit this update's complete wire datagrams in peer rounds, within limits
    service remaining work if allowance permits

attempt(head):
    assert head still belongs to this live socket/link generation
    if optional_pacing_budget cannot cover head.wire_length:
        keep head; try another eligible peer without spending tokens
    else:
        match socket.send_to(head.bytes, head.destination):
            Ok(n) where n == head.wire_length:
                debit optional_pacing_budget by n
                remove head; decrement retained bytes/count
            Err(WouldBlock):
                keep the exact head; yield this socket until a later update
            Err(Interrupted):
                keep head; count the attempt against the work allowance
            Ok(short) or Err(other):
                report terminal send failure; purge affected link's queue
                never retry a suffix or silently treat it as sent
```

`WouldBlock` is the narrow temporary-backpressure case. The first version should
not guess that every OS error is retryable. Exact classification of errors that
invalidate the entire socket versus only an affected link needs platform review;
a terminal socket error closes the endpoint and clears all its queues. A short
successful return is treated as an invariant/error condition, not a stream-style
partial write.

### Pacing tokens are a separate decision

A token bucket is not required to retry `WouldBlock`, and should not be included
merely because the deferred queue could also support pacing. If maintainers want
an optional socket-level byte budget, the proposed contract is: check eligibility
without an irreversible debit, and consume the complete **wire datagram length**
only after a full successful `send_to`. Enqueue, budget deferral, `WouldBlock`,
`Interrupted`, expiry, and fatal failure consume no tokens. This measures bytes
accepted by the local OS, including Netcode overhead but not IP/UDP headers; it
does not measure delivery or physical wire traffic. A burst capacity smaller than
an eligible datagram must be rejected or explicitly diagnosed, not starve forever.

The current `BandwidthLimiter::consume_packet_quota` uses `governor` at transport
packet admission. Its accounting and message-priority semantics should remain
unchanged; do not refund it or repurpose it as evidence of actual UDP sends.
Whether two budgets would be useful or confusing is an open design question.
An endpoint-local serialized attempt can commit its own budget after success;
this does not require a generic global policy callback.
[Existing limiter](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/transport/transport/src/packet/priority_manager.rs)

## Reliability, state freshness, and shutdown

The queue cannot infer whether an opaque datagram contains a reliable message,
several unreliable messages, acknowledgements, or Netcode control traffic. Keep
latest-state replacement and application rate reduction before serialization,
where the producer knows what can be superseded. Real producer backpressure
would need an admission contract at the channel/transport boundary, before
committing packet IDs and retry timestamps. This proposal supplies neither a
new reliability layer nor UDP-level latest-state coalescing.

Reliable retries and Netcode keepalive timing currently start before UDP
acceptance. Deferral can therefore increase observed latency, create retransmit
duplicates, or cause a connection timeout while valid packets are still queued.
FIFO avoids local overtaking within a peer, but cannot remove those effects.
The residence-time bound must be reviewed against these timers; no universally
safe default duration is asserted. Do not extend authentication lifetimes, pause
connection timeouts, or rewrite ack/RTT timestamps to hide queue delay.
[Reliable retry state](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/transport/transport/src/channel/send_reliable.rs#L140-L223)

On child unlink/despawn, purge only that child's deferred items and scheduling
entries. On socket unlink/removal, purge every deferred item before discarding
the socket. A subsequent bind starts with empty storage and a new generation.
Endpoint lifecycle already unlinks/despawns its children; the new queue must
participate in both child and endpoint teardown rather than depending solely on
the next send query.
[Endpoint teardown](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/io/link/src/endpoint.rs#L83-L111)

Immediate `Unlink` should remain immediate and discard pending sends with an
observable reason. Netcode currently queues redundant disconnect packets, and
server stop also proceeds to unlink. A single send-stage pass would no longer
guarantee those packets were handed to the socket when deferral is active. A
bounded graceful-drain request, if desired, needs an explicit lifecycle contract
and deadline; it must not silently turn every unlink into a wait. Admission or
error handling must also distinguish a new connection generation from final
packets intentionally produced for the old generation. This is a design blocker
to settle, not a claim that current shutdown already supports draining.
[Client disconnect packets](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/client.rs#L616-L629),
[server stop lifecycle](https://github.com/cBournhonesque/lightyear/blob/320f825bbeeab62715f0158d07d03320fb1c9cd0/crates/connection/netcode/src/server_plugin.rs#L386-L408)

## Decisions requested before implementation

1. Is an opt-in, post-Netcode UDP queue worthwhile for transient `WouldBlock`,
   or should a staged plaintext/wire queue distinction come first in `Link`?
2. Are explicit link failure on overflow/expiry and immediate purge on unlink
   acceptable? Which existing or new structured error should applications see?
3. What finite bounds and work allowance belong in the public configuration?
   Is aggregate scheduling fairness sufficient without per-peer reservations?
4. Should pacing be excluded from the initial change? If included, how should
   its wire-byte budget be explained alongside `PriorityConfig`?
5. How is a connection generation exposed to UDP, and what bounded shutdown
   contract permits old-session disconnect packets without stale-session sends?

The small UDP-only approach avoids changing channel reliability, but accepts
early transport accounting and limited admission fairness. A separate typed
wire queue plus backpressure upstream would provide stronger semantics at the
cost of a broader cross-crate change. Neither tradeoff should be hidden behind
an application-specific send policy.

## Deterministic validation plan

An implementation should use a fake clock and a scripted datagram sender for
policy tests. No sleeps, socket-buffer saturation benchmark, or background
worker is needed to validate these rules.

- Script `WouldBlock`, then success over successive updates. Assert identical
  bytes/destination, one removal, FIFO, and unchanged Netcode sequence/nonce.
  Include a second Netcode send-stage pass to catch accidental re-encryption.
- Exhaust byte and count capacity independently, including oversized input and
  checked-arithmetic edges. Assert bounded storage, explicit failure, and no
  leftover ciphertext returned to `Link.send`.
- Advance the fake clock to expiry. Assert failure and reclamation, with no
  successful-send accounting. Test repeated `Interrupted`, fatal errors, and a
  short return without spinning or retrying a datagram suffix.
- Exercise two child FIFOs, continuously refill one, and use a one-attempt work
  allowance. Assert cursor rotation and per-peer ordering. A blocked socket must
  yield while a separate endpoint continues.
- Unlink one child with queued traffic; the other remains usable. Unlink and
  restart the endpoint, reuse an address/entity with a new session generation,
  and verify that old ciphertext cannot be emitted. Cover immediate stop and
  any separately agreed graceful-drain deadline.
- If pacing is accepted, verify no token debit on enqueue, budget denial,
  `WouldBlock`, interruption, or terminal error; exact wire-byte debit on full
  success; and progress for the largest permitted datagram.
- Keep the disabled path behavior covered and add a small scheduled
  Transport -> Netcode -> UDP integration fixture. Successful `send_to` is not
  asserted to be remote delivery.

Performance, memory-allocation costs, default limits, and cross-platform error
behavior would require separate evidence after the semantics are agreed. This
draft contains no benchmark results and no implemented API.
