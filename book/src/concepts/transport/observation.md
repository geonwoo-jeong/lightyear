# Observing outgoing traffic

Choose the observation point that answers your question. Enqueuing an application message,
admitting a transport packet, sending a UDP datagram, and receiving an acknowledgement describe
different steps:

| Observation | What it tells you |
| --- | --- |
| Successful application enqueue | A message entered the local send queue. Count this at the application call site; it may still wait for channel scheduling or bandwidth admission. |
| `PacketAdmitted` | An encoded transport packet entered `Link.send`. Data packets passed transport bandwidth admission; ACK-only packets bypass that limit. This precedes connection encryption and socket I/O. |
| `UdpSendOutcome` | One local `UdpSocket::send_to` call returned a byte count or an error. `Ok(n)` reports the exact accepted count, including a short result if one occurs. It does not establish peer receipt. |
| `PacketAcked` | The transport processed an acknowledgement for a sent packet. This does not establish that the remote application handled its messages. |
| `PacketLost` | The transport presumed a packet lost because its acknowledgement did not arrive before the nack timeout. It does not identify where a packet or its acknowledgement was lost. |

`PacketAdmitted.bytes` includes the transport header and any packet compression, before connection
encryption. Its `channels` contains one entry per message or fragment, retaining repeated channel
IDs; an ACK-only packet has an empty list. Rejected packets and host-client messages that bypass
`Link.send` produce no admission event. See [packets](packet.md) and
[bandwidth management](../advanced_replication/bandwidth_management.md).

`UdpSendOutcome.attempted_bytes` is the datagram length supplied to the socket. Its `result` carries
the returned count or `ErrorKind` and optional raw OS error code. Sum successful returned counts
separately from attempted bytes. These counts exclude UDP, IP, and link-layer headers, so they are
not full wire bandwidth. Netcode adds encryption overhead and connection traffic: transport packet
counts and UDP attempt counts have no guaranteed one-to-one correspondence. A UDP outcome also has
no transport packet ID. Transport packet IDs wrap and are local to a link.

`PacketAdmitted` and `UdpSendOutcome` require default-off Cargo features. Enable them on the
direct dependencies used by the example:

```toml
[dependencies]
lightyear_udp = { version = "0.30", features = ["send_observation"] }
lightyear_transport = { version = "0.30", features = ["packet_admission_observation"] }
```

With the `lightyear` facade, enable `udp` together with `udp_send_observation` for UDP outcomes,
and `packet_admission_observation` for transport admission events. Another dependency can enable
these features through Cargo feature unification; leaving a feature out of one dependency
declaration does not ensure it is disabled in the final build.

When a feature is disabled, its observation APIs, extra query fields, system parameters, and
deferred commands are compiled out, preserving the default send-system scheduling. Enabling a
feature retains its observation parameters and deferred-command scheduling barriers even when
no sockets or links are marked. The markers control per-packet observation work; removing them
does not restore the feature-disabled scheduling path.

The following helper installs observers on an existing link and keeps fixed-size, saturating
counters. Call it once per selected link, alongside your normal connection setup. The resource
aggregates those links without retaining packet payloads or a growing packet history.

```rust
use bevy_app::App;
use bevy_ecs::prelude::*;
use lightyear_transport::plugin::{PacketAcked, PacketLost};
use lightyear_transport::prelude::{ObservePacketAdmissions, PacketAdmitted};
use lightyear_udp::prelude::{ObserveUdpSends, UdpSendOutcome};

#[derive(Resource, Default)]
struct TrafficTotals {
    admitted_packets: usize,
    admitted_bytes: usize,
    udp_attempts: usize,
    udp_attempted_bytes: usize,
    udp_sent_bytes: usize,
    udp_errors: usize,
    acked_packets: usize,
    presumed_lost_packets: usize,
}

fn observe_udp_link(app: &mut App, link_entity: Entity, socket_entity: Entity) {
    app.init_resource::<TrafficTotals>();
    app.world_mut()
        .entity_mut(socket_entity)
        .insert(ObserveUdpSends);
    app.world_mut()
        .entity_mut(link_entity)
        .insert(ObservePacketAdmissions)
        .observe(
            |event: On<PacketAdmitted>, mut totals: ResMut<TrafficTotals>| {
                totals.admitted_packets = totals.admitted_packets.saturating_add(1);
                totals.admitted_bytes = totals.admitted_bytes.saturating_add(event.bytes);
            },
        )
        .observe(
            |event: On<UdpSendOutcome>, mut totals: ResMut<TrafficTotals>| {
                totals.udp_attempts = totals.udp_attempts.saturating_add(1);
                totals.udp_attempted_bytes = totals
                    .udp_attempted_bytes
                    .saturating_add(event.attempted_bytes);
                match event.result {
                    Ok(bytes) => {
                        totals.udp_sent_bytes = totals.udp_sent_bytes.saturating_add(bytes)
                    }
                    Err(_) => totals.udp_errors = totals.udp_errors.saturating_add(1),
                }
            },
        )
        .observe(|_: On<PacketAcked>, mut totals: ResMut<TrafficTotals>| {
            totals.acked_packets = totals.acked_packets.saturating_add(1);
        })
        .observe(|_: On<PacketLost>, mut totals: ResMut<TrafficTotals>| {
            totals.presumed_lost_packets = totals.presumed_lost_packets.saturating_add(1);
        });
}
```

The two entity arguments depend on the UDP layout:

| Layout | `link_entity`: transport marker and observers | `socket_entity`: UDP marker |
| --- | --- | --- |
| Single-peer `UdpIo` | Entity containing `Transport`, `Link`, and `UdpIo` | The same entity |
| Multi-peer `UdpEndpoint` | Per-peer child containing `Transport` and `Link` | Endpoint owning the UDP socket |

For an endpoint child, `LinkOf.endpoint` identifies the socket entity. Install observation while
configuring each new child link. Marking the endpoint does not enable transport admission events
on its children. UDP events target the sending child through `event.entity` and separately expose
`event.socket_entity`; the targeted observers above select only the requested links. A global
`app.add_observer` can instead filter those fields explicitly.

Both opt-in event types use deferred commands. Their observers run when Bevy applies those commands,
after the producing system's work; an observer timestamp measures notification handling, not the
admission or socket-call completion time. Do not infer ordering across different entities. Removing
an opt-in marker stops new events of that type from being queued, but does not cancel queued events.
The ACK/loss observers remain active independently of these markers.

Marked transports allocate a channel list for each admitted data packet and queue one event;
ACK-only events have an empty list. Marked UDP sockets queue one command per send attempt,
including all peers of a marked endpoint even if only some have targeted observers. Unmarked
sockets and links queue no observation events and allocate no admission channel lists, but the
enabled feature's system and scheduling costs remain. Neither event copies payload bytes. Keep
observers small and read or reset the aggregate resource from an application system. Snapshot
intervals describe when notifications were processed; ACKs can arrive
in a later interval, so subtracting these counters does not directly measure packet loss or latency.
