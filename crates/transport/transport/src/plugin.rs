use crate::channel::builder::Transport;
use crate::channel::registry::{ChannelId, ChannelRegistry};
use crate::channel::send::SendFlushOutcome;
use crate::error::TransportError;
use crate::packet::PacketId;
use crate::packet::compression::decompress_payload;
use crate::packet::error::PacketError;
use crate::packet::header::PacketHeader;
use crate::packet::message::{FragmentData, MessageAck, ReceiveMessage, SingleData};
use crate::packet::packet_type::PacketType;
#[cfg(feature = "test_utils")]
use crate::prelude::{AppChannelExt, ChannelMode, ChannelSettings};
use alloc::vec::Vec;
use bevy_app::prelude::*;
use bevy_ecs::prelude::*;
use bevy_ecs::schedule::IntoScheduleConfigs;
use bevy_time::{Real, Time};
#[cfg(feature = "test_utils")]
use bevy_utils::default;
#[cfg(test)]
use bytes::Bytes;
use core::time::Duration;
use lightyear_connection::host::HostClient;
#[cfg(any(feature = "client", feature = "server"))]
use lightyear_connection::prelude::Disconnected;
use lightyear_core::prelude::LocalTimeline;
use lightyear_core::tick::Tick;
use lightyear_link::{Link, LinkPlugin, LinkSystems, Linked};
use lightyear_serde::reader::{ReadInteger, Reader};
use lightyear_serde::{SerializationError, ToBytes};
#[cfg(feature = "std")]
use lightyear_utils::adaptive_for_each_mut;
#[cfg(feature = "metrics")]
use lightyear_utils::timer_gauge;
#[allow(unused_imports)]
use tracing::{debug, error, info, trace, warn};

#[deprecated(note = "Use TransportSystems instead")]
pub type TransportSet = TransportSystems;

#[derive(SystemSet, Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub enum TransportSystems {
    // PRE UPDATE
    /// Receive messages from the Link and buffer them into the receive channels.
    Receive,

    // PostUpdate
    /// Flush the messages buffered in the send channels to the Link.
    Send,
}

/// Event triggered on a [`Transport`] entity when it receives a new packet
#[derive(EntityEvent)]
pub struct PacketReceived {
    pub entity: Entity,
    pub remote_tick: Tick,
}

/// Event triggered on a [`Transport`] entity when a sent packet is acknowledged.
#[derive(EntityEvent)]
pub struct PacketAcked {
    pub entity: Entity,
    pub packet_id: PacketId,
    pub rtt_sample: Duration,
}

/// Event triggered on a [`Transport`] entity when a sent packet is presumed lost
/// (its acknowledgement did not arrive before the nack timeout).
#[derive(EntityEvent)]
pub struct PacketLost {
    pub entity: Entity,
    pub packet_id: PacketId,
}

/// Enables [`PacketAdmitted`] events for packets queued by this [`Transport`].
///
/// Without this component, admission does not allocate event metadata or queue events.
/// When enabled, each data packet allocates a channel list and queues one deferred ECS
/// event; ACK-only packets queue an event with an empty list. No packet payload is
/// copied or exposed. Removing the component disables subsequent notifications, but
/// does not cancel events already queued.
///
/// ```
/// use bevy_ecs::prelude::*;
/// use lightyear_transport::prelude::{ObservePacketAdmissions, PacketAdmitted, Transport};
///
/// fn count_admitted(event: On<PacketAdmitted>, mut count: ResMut<PacketCount>) {
///     count.0 += 1;
///     // Repeated channel IDs represent multiple messages in the same packet.
///     for channel in &event.channels {
///         let _ = channel;
///     }
/// }
///
/// #[derive(Resource, Default)]
/// struct PacketCount(usize);
///
/// let mut world = World::new();
/// world.init_resource::<PacketCount>();
/// world.add_observer(count_admitted);
/// world.spawn((Transport::default(), ObservePacketAdmissions));
/// ```
#[derive(Component, Default, Debug, Clone, Copy)]
pub struct ObservePacketAdmissions;

/// Metadata captured after a transport packet enters [`Link::send`].
///
/// Emitted only for entities with [`ObservePacketAdmissions`]. Data packets have
/// passed transport bandwidth admission; ACK-only packets bypass that limit as
/// usual. Denied or unsuccessfully staged packets emit no event. Host-client
/// messages do not enter `Link.send` and emit no event either.
///
/// Metadata is captured before connection-layer encryption and I/O submission, not evidence of
/// a successful socket send, peer receipt, or an acknowledgement. [`PacketAcked`]
/// and [`PacketLost`] report separate, later outcomes.
///
/// Transport may prepare packets on parallel workers. Events are queued through
/// commands, and Bevy observers run when those commands are applied, after the
/// send system finishes. Events for one entity follow admission order; no order
/// across entities is promised. Observers may borrow the event during their call
/// or retain their own metadata copy. The event carries no payload or admission controls.
#[derive(EntityEvent, Debug, Clone)]
pub struct PacketAdmitted {
    /// The entity containing the transport and link that admitted the packet.
    pub entity: Entity,
    /// Transport-local packet ID, which wraps and is not a global identifier.
    pub packet_id: PacketId,
    /// Encoded packet bytes, including the transport header and any packet
    /// compression, before connection encryption or I/O framing.
    pub bytes: usize,
    /// One channel ID per message or message fragment carried by this packet,
    /// in packet order. Repeated channel IDs are retained; this is not a set.
    /// The list is empty for an ACK-only packet.
    pub channels: Vec<ChannelId>,
}

pub struct TransportPlugin;

impl TransportPlugin {
    /// Receives packets from the [`Link`],
    /// Depending on the [`ChannelId`], buffer the messages in the packet
    /// in the appropriate channel receiver
    fn buffer_receive(
        time: Res<Time<Real>>,
        #[cfg(feature = "std")] par_commands: ParallelCommands,
        #[cfg(not(feature = "std"))] mut commands: Commands,
        channel_registry: Res<ChannelRegistry>,
        mut query: Query<(Entity, &mut Link, &mut Transport), (With<Linked>, Without<HostClient>)>,
    ) {
        #[cfg(feature = "metrics")]
        let _timer = timer_gauge!("transport/recv");

        #[cfg(feature = "std")]
        let query = adaptive_for_each_mut!(query);
        #[cfg(not(feature = "std"))]
        let query = query.iter_mut();

        query.for_each(|(entity, mut link, mut transport)| {
            // enable split borrows
            let transport = &mut *transport;
            // update with the latest time
            transport.senders.values_mut().for_each(|channel_send| {
                channel_send.update(&time, &link.stats);
                channel_send.clear_frame_events();
            });
            transport
                .receivers
                .values_mut()
                .for_each(|channel_receive| {
                    channel_receive.update(time.elapsed());
                });
            link.recv
                .drain()
                .try_for_each(|packet| {
                    let packet_len = packet.len();
                    #[cfg(feature = "metrics")]
                    metrics::gauge!("transport/recv_bytes").increment(packet_len as f64);

                    // Connection layers are the last receive stage that needs in-place mutation.
                    // Freeze here so transport parsing can retain cheap immutable subslices.
                    let mut cursor = Reader::from(packet.freeze());

                    // Parse the packet
                    let header = PacketHeader::from_bytes(&mut cursor)?;
                    let tick = header.tick;
                    trace!(
                        target: "lightyear_debug::transport",
                        kind = "packet_recv",
                        schedule = "PreUpdate",
                        sample_point = "PreUpdate",
                        entity = ?entity,
                        packet_id = ?header.packet_id,
                        remote_tick = tick.0,
                        bytes = packet_len,
                        packet_type = ?header.get_packet_type(),
                        "received transport packet"
                    );

                    // Update the packet acks before triggering PacketReceived, so timeline
                    // observers can consume RTT samples from this packet first.
                    let newly_acked_packets = transport
                        .packet_manager
                        .header_manager
                        .process_recv_packet_header(&header, time.elapsed());

                    #[cfg(feature = "std")]
                    par_commands.command_scope(|mut commands| {
                        newly_acked_packets
                            .iter()
                            .for_each(|(packet_id, rtt_sample)| {
                                commands.trigger(PacketAcked {
                                    entity,
                                    packet_id: *packet_id,
                                    rtt_sample: *rtt_sample,
                                });
                            });
                        commands.trigger(PacketReceived {
                            entity,
                            remote_tick: tick,
                        });
                    });
                    #[cfg(not(feature = "std"))]
                    {
                        newly_acked_packets
                            .iter()
                            .for_each(|(packet_id, rtt_sample)| {
                                commands.trigger(PacketAcked {
                                    entity,
                                    packet_id: *packet_id,
                                    rtt_sample: *rtt_sample,
                                });
                            });
                        commands.trigger(PacketReceived {
                            entity,
                            remote_tick: tick,
                        });
                    }

                    let mut packet_type = header.get_packet_type();
                    if packet_type.is_compressed() {
                        let compressed_payload = cursor.take_remaining();
                        let decompressed_payload =
                            decompress_payload(compressed_payload.as_ref(), transport.compression)?;
                        cursor = Reader::from(decompressed_payload);
                        packet_type = packet_type.uncompressed_variant();
                    }

                    // Parse the payload into messages, put them in the internal buffers for each channel
                    // we read directly from the packet and don't create intermediary datastructures to avoid allocations
                    // TODO: maybe do this in a helper function?
                    if packet_type == PacketType::DataFragment {
                        // read the fragment data
                        let channel_id = ChannelId::from_bytes(&mut cursor)?;
                        let fragment_data = FragmentData::from_bytes(&mut cursor)?;
                        let channel_name = channel_registry.get_name_from_net_id(channel_id);
                        #[cfg(feature = "metrics")]
                        {
                            channel_registry.record_recv_messages(channel_id, channel_name, 1.0);
                            channel_registry.record_recv_bytes(
                                channel_id,
                                channel_name,
                                fragment_data.bytes.len() as f64,
                            );
                        }
                        trace!(
                            target: "lightyear_debug::transport",
                            kind = "channel_recv_fragment",
                            schedule = "PreUpdate",
                            sample_point = "PreUpdate",
                            entity = ?entity,
                            packet_id = ?header.packet_id,
                            remote_tick = tick.0,
                            channel_id,
                            channel = channel_name,
                            bytes = fragment_data.bytes.len(),
                            "received channel fragment"
                        );
                        transport
                            .receivers
                            .get_mut(&channel_id)
                            .ok_or(PacketError::ChannelNotFound)?
                            .buffer_recv(ReceiveMessage {
                                data: fragment_data.into(),
                                remote_sent_tick: tick,
                                compression: transport.compression,
                            })?;
                    }
                    // read single message data
                    while cursor.has_remaining() {
                        let channel_id = ChannelId::from_bytes(&mut cursor)?;
                        let channel_name = channel_registry.get_name_from_net_id(channel_id);
                        let num_messages = cursor.read_u8().map_err(SerializationError::from)?;
                        #[cfg(feature = "metrics")]
                        channel_registry.record_recv_messages(
                            channel_id,
                            channel_name,
                            num_messages as f64,
                        );
                        trace!(?channel_id, ?num_messages);
                        trace!(
                            target: "lightyear_debug::transport",
                            kind = "channel_recv_batch",
                            schedule = "PreUpdate",
                            sample_point = "PreUpdate",
                            entity = ?entity,
                            packet_id = ?header.packet_id,
                            remote_tick = tick.0,
                            channel_id,
                            channel = channel_name,
                            num_messages,
                            "received channel message batch"
                        );
                        for _ in 0..num_messages {
                            let single_data = SingleData::from_bytes(&mut cursor)?;
                            #[cfg(feature = "metrics")]
                            channel_registry.record_recv_bytes(
                                channel_id,
                                channel_name,
                                single_data.bytes.len() as f64,
                            );
                            trace!(
                                target: "lightyear_debug::transport",
                                kind = "channel_recv_message",
                                schedule = "PreUpdate",
                                sample_point = "PreUpdate",
                                entity = ?entity,
                                packet_id = ?header.packet_id,
                                remote_tick = tick.0,
                                channel_id,
                                channel = channel_name,
                                bytes = single_data.bytes.len(),
                                "received channel message bytes"
                            );
                            transport
                                .receivers
                                .get_mut(&channel_id)
                                .ok_or(PacketError::ChannelNotFound)?
                                .buffer_recv(ReceiveMessage {
                                    data: single_data.into(),
                                    remote_sent_tick: tick,
                                    compression: transport.compression,
                                })?;
                        }
                    }
                    Ok::<(), TransportError>(())
                })
                .inspect_err(|e| {
                    error!("Error processing packet: {e:?}");
                })
                .ok();

            // Consume ACKs already queued for this frame before expiring packets. Otherwise a
            // packet can lose its message-ACK mapping immediately before its ACK is processed.
            transport
                .packet_manager
                .header_manager
                .update(time.elapsed(), &link.stats);
            let packet_message_acks = &mut transport.packet_message_acks;
            let senders = &mut transport.senders;
            transport
                .packet_manager
                .header_manager
                .lost_packets
                .drain(..)
                .try_for_each(|lost_packet| {
                    #[cfg(feature = "metrics")]
                    metrics::counter!("transport/packets_lost").increment(1);
                    trace!(
                        target: "lightyear_debug::transport",
                        kind = "packet_lost",
                        schedule = "PreUpdate",
                        sample_point = "PreUpdate",
                        entity = ?entity,
                        packet_id = ?lost_packet,
                        packet_loss = true,
                        "transport packet marked lost"
                    );
                    #[cfg(feature = "std")]
                    par_commands.command_scope(|mut commands| {
                        commands.trigger(PacketLost {
                            entity,
                            packet_id: lost_packet,
                        });
                    });
                    #[cfg(not(feature = "std"))]
                    commands.trigger(PacketLost {
                        entity,
                        packet_id: lost_packet,
                    });
                    if let Some(mut message_acks) = packet_message_acks.take(&lost_packet) {
                        let result =
                            message_acks
                                .drain(..)
                                .try_for_each(|(channel_kind, message_ack)| {
                                    let channel_send = senders
                                        .get_mut(&channel_kind)
                                        .ok_or(PacketError::ChannelNotFound)?;
                                    // TODO: batch the messages?
                                    trace!(
                                        ?lost_packet,
                                        ?channel_kind,
                                        "message lost: {:?}",
                                        message_ack.message_id
                                    );
                                    channel_send.message_nacks.push(message_ack.message_id);
                                    channel_send.receive_nack(&message_ack);
                                    Ok::<(), TransportError>(())
                                });
                        packet_message_acks.recycle(message_acks);
                        result?;
                    }
                    Ok::<(), TransportError>(())
                })
                .ok();

            // Update the list of messages that have been acked
            let packet_message_acks = &mut transport.packet_message_acks;
            let senders = &mut transport.senders;
            transport
                .packet_manager
                .header_manager
                .newly_acked_packets
                .drain(..)
                .try_for_each(|(acked_packet, rtt_sample)| {
                    trace!("Acked packet {:?}", acked_packet);
                    trace!(
                        target: "lightyear_debug::transport",
                        kind = "packet_acked",
                        schedule = "PreUpdate",
                        sample_point = "PreUpdate",
                        entity = ?entity,
                        packet_id = ?acked_packet,
                        rtt_sample_ms = rtt_sample.as_secs_f64() * 1000.0,
                        "transport packet acked"
                    );
                    if let Some(mut message_acks) = packet_message_acks.take(&acked_packet) {
                        let result =
                            message_acks
                                .drain(..)
                                .try_for_each(|(channel_kind, message_ack)| {
                                    let channel_send = senders
                                        .get_mut(&channel_kind)
                                        .ok_or(PacketError::ChannelNotFound)?;
                                    if channel_send.receive_ack(&message_ack) {
                                        trace!(
                                            "Acked message: channel={:?},message_ack={:?}",
                                            channel_send.name(),
                                            message_ack
                                        );
                                        channel_send.message_acks.push(message_ack.message_id);
                                    }
                                    Ok::<(), TransportError>(())
                                });
                        packet_message_acks.recycle(message_acks);
                        result?;
                    }
                    Ok::<(), TransportError>(())
                })
                .ok();
        });
    }

    /// Builds packets from the entity's send channels and uploads them to the [`Link`].
    fn buffer_send(
        real_time: Res<Time<Real>>,
        timeline: Res<LocalTimeline>,
        #[cfg(feature = "std")] par_commands: ParallelCommands,
        #[cfg(not(feature = "std"))] mut commands: Commands,
        mut query: Query<
            (
                Entity,
                &mut Link,
                &mut Transport,
                Option<&mut HostClient>,
                Has<ObservePacketAdmissions>,
            ),
            With<Linked>,
        >,
        channel_registry: Res<ChannelRegistry>,
    ) {
        #[cfg(feature = "metrics")]
        let _timer = timer_gauge!("transport/send");
        let tick = timeline.tick();
        #[cfg(feature = "std")]
        let query = adaptive_for_each_mut!(query);
        #[cfg(not(feature = "std"))]
        let query = query.iter_mut();
        query.for_each(|(entity, mut link, mut transport, host_client, observe_admissions)| {
            // allow split borrows
            let transport = &mut *transport;
            let mtu = link.mtu();

            // buffer all new messages in the Sender
            if let Some(mut host_client) = host_client {
                // for a host-client, we write the bytes directly to the HostClient buffer
                transport.recv_channel.try_iter().try_for_each(|(channel_kind, bytes, priority)| {
                    host_client.buffer.push((bytes, channel_kind.0, tick));
                    Ok::<(), TransportError>(())
                }).inspect_err(|e| error!("error buffering host-client message: {e:?}")).ok();
                return
            }
            transport.packet_manager.begin_send(mtu);
            (|| {
                while let Ok((channel_kind, bytes, priority)) = transport.recv_channel.try_recv() {
                    let channel_send = transport.senders.get(&channel_kind).ok_or(
                        TransportError::ChannelNotFound(channel_kind),
                    )?;
                    trace!(
                        target: "lightyear_debug::transport",
                        kind = "channel_send_buffer",
                        schedule = "PostUpdate",
                        sample_point = "PostUpdate",
                        tick = ?tick,
                        tick_id = u64::from(tick.0),
                        channel = %channel_send.name(),
                        channel_kind = ?channel_kind,
                        bytes = bytes.len(),
                        priority = priority,
                        "buffered channel message for transport send"
                    );
                    // TODO: do we need the message_id?
                    transport.send_mut_erased(channel_kind, bytes, priority)?;
                }
                Ok::<(), TransportError>(())
            })()
            .inspect_err(|e| error!("error sending message: {e:?}"))
            .ok();

            // Collect cheap snapshots while each channel retains ownership of its pending queues.
            transport.priority_manager.clear();
            {
                let candidates = transport.priority_manager.candidates_mut();
                transport.senders.values_mut().for_each(|channel| {
                    channel.collect_send_candidates(candidates);
                });
            }
            transport.priority_manager.prioritize(&channel_registry);

            let mut candidate_cursor = crate::packet::packet_builder::CandidateCursor::default();
            let mut total_bytes_sent = 0;
            let mut flush_outcome = SendFlushOutcome::Complete;
            loop {
                let staged = transport.packet_manager.build_next_packet(
                    tick,
                    transport.priority_manager.candidates(),
                    &mut candidate_cursor,
                    transport.compression,
                    mtu,
                    &mut transport.compression_scratch,
                );
                let mut packet = match staged {
                    Ok(Some(packet)) => packet,
                    Ok(None) => break,
                    Err(error) => {
                        flush_outcome = SendFlushOutcome::StagingFailed;
                        error!(?error, "failed to stage transport packet");
                        break;
                    }
                };
                trace!(packet_id = ?packet.packet_id, num_messages = ?packet.num_messages(), "sending packet");
                let packet_id = packet.packet_id;
                let num_messages = packet.num_messages();
                let packet_len = packet.payload.len();
                let packet_compression = packet.compression;
                if !transport
                    .bandwidth_limiter
                    .consume_packet_quota(packet_len)
                {
                    // Staging has no channel or packet-header side effects. Reliable messages are
                    // retained; each unreliable channel applies its retry-unsent policy when this
                    // flush finishes. Since bandwidth limiting also enables priority ordering,
                    // later candidates are not higher priority than this packet.
                    flush_outcome = SendFlushOutcome::BandwidthLimited;
                    transport.packet_manager.recycle_packet(packet);
                    break;
                }
                if let Some(compression_info) = packet.compression {
                    trace!(
                        original_len = compression_info.original_len,
                        compressed_len = compression_info.compressed_len,
                        "transport packet was compressed by packet builder"
                    );
                }
                trace!(
                    target: "lightyear_debug::transport",
                    kind = "packet_send",
                    schedule = "PostUpdate",
                    sample_point = "PostUpdate",
                    packet_id = ?packet_id,
                    local_tick = tick.0,
                    bytes = packet_len,
                    num_messages,
                    compression_enabled = transport.compression.is_enabled(),
                    compression_algorithm = ?transport.compression.algorithm,
                    packet_compressed = packet_compression.is_some(),
                    compression_original_len = packet_compression.map_or(0, |info| info.original_len),
                    compression_compressed_len = packet_compression.map_or(0, |info| info.compressed_len),
                    "sending transport packet"
                );

                // Acceptance into Link.send is the transactional boundary. Packet ids, retry
                // timestamps, ack maps, and metrics are committed only after this point.
                total_bytes_sent += packet.payload.len() as u32;
                let payload = transport.packet_manager.take_send_payload(&mut packet);
                link.send.push(payload);
                transport
                    .packet_manager
                    .header_manager
                    .commit_send_packet(packet_id, real_time.elapsed());
                if observe_admissions {
                    let event = PacketAdmitted {
                        entity,
                        packet_id,
                        bytes: packet_len,
                        channels: packet.messages.iter().map(|metadata| metadata.channel).collect(),
                    };
                    #[cfg(feature = "std")]
                    par_commands.command_scope(|mut commands| commands.trigger(event));
                    #[cfg(not(feature = "std"))]
                    commands.trigger(event);
                }

                #[cfg(feature = "metrics")]
                if let Some(compression_info) = packet.compression {
                    metrics::counter!("transport/compression_saved_bytes").increment(
                        (compression_info.original_len - compression_info.compressed_len) as u64,
                    );
                }

                let mut packet_messages = core::mem::take(&mut packet.messages);
                for metadata in packet_messages.drain(..) {
                    let commit = metadata.commit;
                    let watches_acks = {
                        let channel_send = transport
                            .senders
                            .get_mut(&commit.channel_kind)
                            .expect("staged candidate channel must remain registered during flush");
                        channel_send.commit_send(commit.key, real_time.elapsed());
                        channel_send.watches_acks()
                    };

                    #[cfg(feature = "metrics")]
                    {
                        let channel_name =
                            channel_registry.get_name_from_net_id(metadata.channel);
                        channel_registry.record_send_message(
                            metadata.channel,
                            channel_name,
                            metadata.num_bytes as f64,
                        );
                    }

                    let Some(message_id) = metadata.message else {
                        continue;
                    };
                    transport
                        .senders
                        .get_mut(&commit.channel_kind)
                        .expect("staged candidate channel must remain registered during flush")
                        .messages_sent
                        .push(message_id);
                    if watches_acks {
                        trace!(
                            "Registering message ack (ChannelId:{:?} {:?}) for packet {:?}",
                            metadata.channel, metadata, packet.packet_id
                        );

                        transport.packet_message_acks.track(
                            packet.packet_id,
                            commit.channel_kind,
                            MessageAck {
                                message_id,
                                fragment_id: metadata.fragment_index,
                            },
                        );
                        trace!(?transport.packet_message_acks, "packet to message");
                    }
                }
                transport
                    .packet_manager
                    .recycle_message_metadata_list(packet_messages);
            }
            transport
                .senders
                .values_mut()
                .for_each(|channel| channel.finish_send(flush_outcome));
            transport.priority_manager.clear();

            // Every data packet carries the latest ACK state. When ACK information is pending but
            // no data packet entered the link this frame, send one header-only packet so the remote
            // peer learns about delivered packets without waiting for more application traffic.
            // Control traffic deliberately bypasses application bandwidth admission, and ACK-only
            // packets do not elicit another ACK.
            if total_bytes_sent == 0
                && transport
                    .packet_manager
                    .header_manager
                    .has_pending_ack()
            {
                match transport.packet_manager.build_ack_only_packet(tick, mtu) {
                    Ok(mut packet) => {
                        let packet_id = packet.packet_id;
                        let packet_len = packet.payload.len();
                        trace!(?packet_id, packet_len, "sending ACK-only packet");
                        let payload = transport.packet_manager.take_send_payload(&mut packet);
                        link.send.push(payload);
                        transport
                            .packet_manager
                            .header_manager
                            .commit_send_ack_only(packet_id);
                        if observe_admissions {
                            let event = PacketAdmitted {
                                entity,
                                packet_id,
                                bytes: packet_len,
                                channels: Vec::new(),
                            };
                            #[cfg(feature = "std")]
                            par_commands.command_scope(|mut commands| commands.trigger(event));
                            #[cfg(not(feature = "std"))]
                            commands.trigger(event);
                        }
                        total_bytes_sent += packet_len as u32;
                    }
                    Err(error) => error!(?error, "failed to stage ACK-only packet"),
                }
            }
            if total_bytes_sent > 0 {
                trace!(
                    target: "lightyear_debug::transport",
                    kind = "send_flush",
                    schedule = "PostUpdate",
                    sample_point = "PostUpdate",
                    local_tick = tick.0,
                    send_bytes = total_bytes_sent,
                    "flushed transport packets to link"
                );
            }

            #[cfg(feature = "metrics")]
            metrics::gauge!("transport/send_bytes").increment(total_bytes_sent as f64);
        });
    }

    /// On disconnection, reset the Transport to its original state.
    #[cfg(any(feature = "client", feature = "server"))]
    fn handle_disconnection(
        trigger: On<Add, Disconnected>,
        mut query: Query<&mut Transport>,
        registry: Res<ChannelRegistry>,
    ) {
        if let Ok(mut transport) = query.get_mut(trigger.entity) {
            transport.reset(&registry);
        }
    }
}

impl Plugin for TransportPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<LinkPlugin>() {
            app.add_plugins(LinkPlugin);
        }
        #[cfg(any(feature = "client", feature = "server"))]
        app.add_observer(Self::handle_disconnection);
    }

    fn finish(&self, app: &mut App) {
        if !app.world().contains_resource::<ChannelRegistry>() {
            warn!("TransportPlugin: ChannelRegistry not found, adding it");
            app.world_mut().init_resource::<ChannelRegistry>();
        }
        app.configure_sets(
            PreUpdate,
            TransportSystems::Receive.after(LinkSystems::Receive),
        );
        app.configure_sets(PostUpdate, TransportSystems::Send.before(LinkSystems::Send));
        app.add_systems(
            PreUpdate,
            Self::buffer_receive.in_set(TransportSystems::Receive),
        );
        app.add_systems(PostUpdate, Self::buffer_send.in_set(TransportSystems::Send));
    }
}

#[cfg(feature = "test_utils")]
pub struct TestChannel;

#[cfg(feature = "test_utils")]
pub struct TestTransportPlugin;

#[cfg(feature = "test_utils")]
impl Plugin for TestTransportPlugin {
    fn build(&self, app: &mut App) {
        // add all channels before adding the TransportPlugin
        app.init_resource::<ChannelRegistry>();
        app.add_channel::<TestChannel>(ChannelSettings {
            mode: ChannelMode::UnorderedUnreliable,
            ..default()
        });
        // add required resources
        app.init_resource::<Time<Real>>();
        // add the TransportPlugin
        app.add_plugins(TransportPlugin);
    }
}

#[cfg(test)]
mod tests {
    use alloc::{vec, vec::Vec};

    use super::*;
    use crate::channel::builder::{ChannelMode, ChannelSettings};
    use crate::channel::registry::ChannelKind;
    use crate::packet::header::PacketHeaderManager;
    use crate::packet::packet::PacketId;
    use crate::packet::priority_manager::PriorityConfig;
    use bevy_ecs::system::RunSystemOnce;

    struct RetryChannel;
    struct DiscardChannel;
    struct SmallMtuChannel;
    struct AckBeforeTimeoutChannel;

    #[derive(Resource, Default)]
    struct Admissions(Vec<PacketAdmitted>);

    fn admission_test_app(registry: ChannelRegistry) -> App {
        let mut app = App::new();
        app.add_plugins(bevy_app::TaskPoolPlugin::default());
        app.insert_resource(registry);
        app.init_resource::<Time<Real>>();
        app.init_resource::<LocalTimeline>();
        app.init_resource::<Admissions>();
        app.add_observer(
            |event: On<PacketAdmitted>, mut events: ResMut<Admissions>| {
                events.0.push(event.event().clone());
            },
        );
        app
    }

    /// Decode actual wire contents independently of packet-builder metadata.
    fn packet_channels(packet: Bytes) -> (PacketId, Vec<ChannelId>) {
        let mut reader = Reader::from(packet);
        let header = PacketHeader::from_bytes(&mut reader).unwrap();
        assert!(!header.get_packet_type().is_compressed());
        let mut channels = Vec::new();
        if header.get_packet_type() == PacketType::DataFragment {
            channels.push(ChannelId::from_bytes(&mut reader).unwrap());
            FragmentData::from_bytes(&mut reader).unwrap();
        }
        while reader.has_remaining() {
            let channel = ChannelId::from_bytes(&mut reader).unwrap();
            let count = reader.read_u8().unwrap();
            for _ in 0..count {
                SingleData::from_bytes(&mut reader).unwrap();
                channels.push(channel);
            }
        }
        (header.packet_id, channels)
    }

    #[test]
    fn admission_events_preserve_bytes_and_report_each_packets_channels() {
        let settings = ChannelSettings::default();
        let mut registry = ChannelRegistry::default();
        let (_, first_id) = registry.add_channel::<RetryChannel>(settings);
        let (_, second_id) = registry.add_channel::<DiscardChannel>(settings);
        let mut app = admission_test_app(registry);
        let world = app.world_mut();
        // Two identical transports prove observation does not alter bytes or ordering.
        // A third, differently populated transport exercises per-entity event metadata.
        let entities = [true, false, true].map(|observe| {
            let mut transport = Transport::default();
            transport.add_channel_send::<RetryChannel>(settings, first_id);
            transport.add_channel_send::<DiscardChannel>(settings, second_id);
            let mut entity = world.spawn((
                Link::default().with_mtu(lightyear_link::LinkMtu::new(256)),
                Linked,
                transport,
            ));
            if observe {
                entity.insert(ObservePacketAdmissions);
            }
            entity.id()
        });
        for &entity in &entities[..2] {
            let transport = world.get::<Transport>(entity).unwrap();
            transport
                .send::<RetryChannel>(Bytes::from(vec![1; 700]))
                .unwrap();
            transport
                .send::<RetryChannel>(Bytes::from_static(b"first"))
                .unwrap();
            transport
                .send::<RetryChannel>(Bytes::from_static(b"second"))
                .unwrap();
            transport
                .send::<DiscardChannel>(Bytes::from_static(b"other channel"))
                .unwrap();
        }
        world
            .get::<Transport>(entities[2])
            .unwrap()
            .send::<DiscardChannel>(Bytes::from_static(b"separate entity"))
            .unwrap();

        world.run_system_once(TransportPlugin::buffer_send).unwrap();
        let packets = entities.map(|entity| {
            world
                .get_mut::<Link>(entity)
                .unwrap()
                .send
                .drain()
                .collect::<Vec<_>>()
        });
        assert_eq!(packets[0], packets[1]);
        assert!(
            packets[0].len() > 1,
            "large message must span several packets"
        );
        let events = &world.resource::<Admissions>().0;
        assert_eq!(events.len(), packets[0].len() + packets[2].len());
        assert!(events.iter().all(|event| event.entity != entities[1]));
        let mut mixed_packet = false;
        let mut repeated_channel = false;
        for index in [0, 2] {
            let observed: Vec<_> = events
                .iter()
                .filter(|event| event.entity == entities[index])
                .collect();
            assert_eq!(observed.len(), packets[index].len());
            for (event, packet) in observed.into_iter().zip(&packets[index]) {
                let (packet_id, channels) = packet_channels(packet.clone());
                assert_eq!(event.packet_id, packet_id);
                assert_eq!(event.bytes, packet.len());
                assert_eq!(event.channels, channels);
                mixed_packet |= channels.contains(&first_id) && channels.contains(&second_id);
                repeated_channel |= channels.iter().filter(|&&id| id == first_id).count() > 1;
            }
        }
        assert!(
            mixed_packet,
            "final fragment packet also contains another channel"
        );
        assert!(repeated_channel, "metadata retains repeated channel IDs");

        let previous_count = events.len();
        world
            .entity_mut(entities[0])
            .remove::<ObservePacketAdmissions>();
        world
            .get::<Transport>(entities[0])
            .unwrap()
            .send::<RetryChannel>(Bytes::from_static(b"after opt-out"))
            .unwrap();
        world.run_system_once(TransportPlugin::buffer_send).unwrap();
        assert_eq!(world.get::<Link>(entities[0]).unwrap().send.len(), 1);
        assert_eq!(world.resource::<Admissions>().0.len(), previous_count);
    }

    #[test]
    fn bandwidth_denied_packets_do_not_emit_admission_events() {
        let settings = ChannelSettings {
            retry_unsent_messages: true,
            ..Default::default()
        };
        let mut registry = ChannelRegistry::default();
        let (kind, id) = registry.add_channel::<RetryChannel>(settings);
        let mut app = admission_test_app(registry);
        let world = app.world_mut();
        let entity = spawn_transport::<RetryChannel>(world, settings, kind, id);
        world.entity_mut(entity).insert(ObservePacketAdmissions);
        world.run_system_once(TransportPlugin::buffer_send).unwrap();

        let packet = world.get_mut::<Link>(entity).unwrap().send.pop().unwrap();
        assert_eq!(world.get::<Link>(entity).unwrap().send.len(), 0);
        assert_eq!(pending_candidates::<RetryChannel>(world, entity), 1);
        let events = &world.resource::<Admissions>().0;
        assert_eq!(events.len(), 1, "only the admitted packet is observed");
        assert_eq!(events[0].entity, entity);
        assert_eq!(events[0].packet_id, PacketId(0));
        assert_eq!(events[0].bytes, packet.len());
        assert_eq!(events[0].channels, [id]);
    }

    #[test]
    fn ack_only_admission_has_empty_channels_and_is_emitted_once() {
        let mut app = admission_test_app(ChannelRegistry::default());
        let world = app.world_mut();
        let entity = world
            .spawn((
                Link::default(),
                Linked,
                Transport::default(),
                ObservePacketAdmissions,
            ))
            .id();
        let header =
            PacketHeaderManager::default().preview_send_packet_header(PacketType::Data, Tick(7));
        let mut packet = Vec::new();
        header.to_bytes(&mut packet).unwrap();
        world
            .get_mut::<Link>(entity)
            .unwrap()
            .recv
            .push_raw(lightyear_link::recv_payload_from_bytes(Bytes::from(packet)));
        world
            .run_system_once(TransportPlugin::buffer_receive)
            .unwrap();
        world.run_system_once(TransportPlugin::buffer_send).unwrap();

        let packet = world.get_mut::<Link>(entity).unwrap().send.pop().unwrap();
        let header = PacketHeader::from_bytes(&mut Reader::from(packet.clone())).unwrap();
        assert_eq!(header.get_packet_type(), PacketType::AckOnly);
        let events = &world.resource::<Admissions>().0;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].entity, entity);
        assert_eq!(events[0].packet_id, header.packet_id);
        assert_eq!(events[0].bytes, packet.len());
        assert!(events[0].channels.is_empty());

        world.run_system_once(TransportPlugin::buffer_send).unwrap();
        assert_eq!(world.resource::<Admissions>().0.len(), 1);
        assert_eq!(world.get::<Link>(entity).unwrap().send.len(), 0);
    }

    #[test]
    fn host_client_messages_do_not_emit_packet_admission_events() {
        let settings = ChannelSettings::default();
        let mut registry = ChannelRegistry::default();
        let (_, id) = registry.add_channel::<RetryChannel>(settings);
        let mut transport = Transport::default();
        transport.add_channel_send::<RetryChannel>(settings, id);
        let mut app = admission_test_app(registry);
        let world = app.world_mut();
        let entity = world
            .spawn((
                Link::default(),
                Linked,
                transport,
                ObservePacketAdmissions,
                HostClient { buffer: Vec::new() },
            ))
            .id();
        world
            .get::<Transport>(entity)
            .unwrap()
            .send::<RetryChannel>(Bytes::from_static(b"local"))
            .unwrap();
        world.run_system_once(TransportPlugin::buffer_send).unwrap();
        assert_eq!(world.get::<HostClient>(entity).unwrap().buffer.len(), 1);
        assert_eq!(world.get::<Link>(entity).unwrap().send.len(), 0);
        assert!(world.resource::<Admissions>().0.is_empty());
    }

    #[cfg(feature = "compression_lz4")]
    #[test]
    fn admission_byte_count_uses_compressed_packet_length() {
        let settings = ChannelSettings::default();
        let mut registry = ChannelRegistry::default();
        let (_, id) = registry.add_channel::<RetryChannel>(settings);
        let mut transport = Transport::default()
            .with_compression(crate::packet::compression::CompressionConfig::LZ4);
        transport.add_channel_send::<RetryChannel>(settings, id);
        let mut app = admission_test_app(registry);
        let world = app.world_mut();
        let entity = world
            .spawn((Link::default(), Linked, transport, ObservePacketAdmissions))
            .id();
        world
            .get::<Transport>(entity)
            .unwrap()
            .send::<RetryChannel>(Bytes::from(vec![7; 800]))
            .unwrap();
        world.run_system_once(TransportPlugin::buffer_send).unwrap();
        let packet = world.get_mut::<Link>(entity).unwrap().send.pop().unwrap();
        let header = PacketHeader::from_bytes(&mut Reader::from(packet.clone())).unwrap();
        assert!(header.get_packet_type().is_compressed());
        let events = &world.resource::<Admissions>().0;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].bytes, packet.len());
        assert!(events[0].bytes < 800);
        assert_eq!(events[0].channels, [id]);
    }

    fn spawn_transport<C: crate::channel::Channel>(
        world: &mut World,
        settings: ChannelSettings,
        channel_kind: ChannelKind,
        channel_id: ChannelId,
    ) -> Entity {
        let mut transport = Transport::new(PriorityConfig::new(1).with_burst_size(1200));
        transport.add_channel_send::<C>(settings, channel_id);
        for value in [1, 2] {
            transport
                .send_mut_erased(channel_kind, Bytes::from(vec![value; 1000]), 1.0)
                .unwrap();
        }
        world.spawn((Link::default(), Linked, transport)).id()
    }

    fn pending_candidates<C: crate::channel::Channel>(world: &mut World, entity: Entity) -> usize {
        let mut entity = world.entity_mut(entity);
        let mut transport = entity.get_mut::<Transport>().unwrap();
        let channel_kind = ChannelKind::of::<C>();
        let mut candidates = Vec::new();
        let metadata = transport.senders.get_mut(&channel_kind).unwrap();
        metadata.collect_send_candidates(&mut candidates);
        candidates.len()
    }

    #[test]
    fn bandwidth_limited_flush_applies_each_channels_retry_policy() {
        let retry_settings = ChannelSettings {
            mode: ChannelMode::UnorderedUnreliable,
            retry_unsent_messages: true,
            ..Default::default()
        };
        let discard_settings = ChannelSettings {
            retry_unsent_messages: false,
            ..retry_settings
        };
        let mut registry = ChannelRegistry::default();
        let (retry_kind, retry_id) = registry.add_channel::<RetryChannel>(retry_settings);
        let (discard_kind, discard_id) = registry.add_channel::<DiscardChannel>(discard_settings);

        let mut app = App::new();
        app.add_plugins(bevy_app::TaskPoolPlugin::default());
        let world = app.world_mut();
        world.insert_resource(registry);
        world.init_resource::<Time<Real>>();
        world.init_resource::<LocalTimeline>();
        let retry_entity =
            spawn_transport::<RetryChannel>(world, retry_settings, retry_kind, retry_id);
        let discard_entity =
            spawn_transport::<DiscardChannel>(world, discard_settings, discard_kind, discard_id);

        world.run_system_once(TransportPlugin::buffer_send).unwrap();

        assert_eq!(world.get::<Link>(retry_entity).unwrap().send.len(), 1);
        assert_eq!(world.get::<Link>(discard_entity).unwrap().send.len(), 1);
        assert_eq!(pending_candidates::<RetryChannel>(world, retry_entity), 1);
        assert_eq!(
            pending_candidates::<DiscardChannel>(world, discard_entity),
            0
        );
    }

    #[test]
    fn idle_receiver_sends_one_prompt_ack_only_packet() {
        let mut remote = PacketHeaderManager::default();
        let data_header = remote.preview_send_packet_header(PacketType::Data, Tick(7));
        let mut data_packet = Vec::new();
        data_header.to_bytes(&mut data_packet).unwrap();
        remote.commit_send_packet(data_header.packet_id, Duration::ZERO);

        let mut world = World::new();
        world.insert_resource(ChannelRegistry::default());
        world.init_resource::<Time<Real>>();
        world.init_resource::<LocalTimeline>();
        let entity = world
            .spawn((Link::default(), Linked, Transport::default()))
            .id();
        world.get_mut::<Link>(entity).unwrap().recv.push_raw(
            lightyear_link::recv_payload_from_bytes(Bytes::from(data_packet)),
        );

        world
            .run_system_once(TransportPlugin::buffer_receive)
            .unwrap();
        world.run_system_once(TransportPlugin::buffer_send).unwrap();

        let ack_packet = world
            .get_mut::<Link>(entity)
            .unwrap()
            .send
            .pop()
            .expect("an idle receiver must promptly emit an ACK-only packet");
        let ack_header = PacketHeader::from_bytes(&mut Reader::from(ack_packet.clone())).unwrap();
        assert_eq!(ack_header.get_packet_type(), PacketType::AckOnly);
        assert_eq!(
            remote.process_recv_packet_header(&ack_header, Duration::from_millis(1)),
            vec![(PacketId(0), Duration::from_millis(1))]
        );

        world
            .get_mut::<Link>(entity)
            .unwrap()
            .recv
            .push_raw(lightyear_link::recv_payload_from_bytes(ack_packet));
        world
            .run_system_once(TransportPlugin::buffer_receive)
            .unwrap();
        world.run_system_once(TransportPlugin::buffer_send).unwrap();
        assert_eq!(world.get::<Link>(entity).unwrap().send.len(), 0);
    }

    #[test]
    fn queued_ack_is_processed_before_packet_loss_timeout() {
        let settings = ChannelSettings {
            mode: ChannelMode::UnorderedReliable(Default::default()),
            ..Default::default()
        };
        let mut registry = ChannelRegistry::default();
        let (channel_kind, channel_id) = registry.add_channel::<AckBeforeTimeoutChannel>(settings);

        let mut sender_transport = Transport::default();
        sender_transport.add_channel_send::<AckBeforeTimeoutChannel>(settings, channel_id);
        let mut receiver_transport = Transport::default();
        receiver_transport.add_channel_receive::<AckBeforeTimeoutChannel>(settings, channel_id);

        let mut sender = World::new();
        sender.insert_resource(registry.clone());
        sender.init_resource::<Time<Real>>();
        sender.init_resource::<LocalTimeline>();
        let sender_entity = sender
            .spawn((Link::default(), Linked, sender_transport))
            .id();

        let mut receiver = World::new();
        receiver.insert_resource(registry);
        receiver.init_resource::<Time<Real>>();
        receiver.init_resource::<LocalTimeline>();
        let receiver_entity = receiver
            .spawn((Link::default(), Linked, receiver_transport))
            .id();

        sender
            .get::<Transport>(sender_entity)
            .unwrap()
            .send_erased(channel_kind, Bytes::from_static(b"delivered"), 1.0)
            .unwrap();
        sender
            .run_system_once(TransportPlugin::buffer_send)
            .unwrap();
        let data = sender
            .get_mut::<Link>(sender_entity)
            .unwrap()
            .send
            .pop()
            .unwrap();
        receiver
            .get_mut::<Link>(receiver_entity)
            .unwrap()
            .recv
            .push_raw(lightyear_link::recv_payload_from_bytes(data));
        receiver
            .run_system_once(TransportPlugin::buffer_receive)
            .unwrap();
        receiver
            .run_system_once(TransportPlugin::buffer_send)
            .unwrap();
        let ack = receiver
            .get_mut::<Link>(receiver_entity)
            .unwrap()
            .send
            .pop()
            .unwrap();

        // The default packet NACK floor is 10 ms. Queue an ACK after that timeout so this
        // receive pass would mark the packet lost first with the previous processing order.
        sender
            .resource_mut::<Time<Real>>()
            .advance_by(Duration::from_millis(11));
        sender
            .get_mut::<Link>(sender_entity)
            .unwrap()
            .recv
            .push_raw(lightyear_link::recv_payload_from_bytes(ack));
        sender
            .run_system_once(TransportPlugin::buffer_receive)
            .unwrap();

        let transport = sender.get::<Transport>(sender_entity).unwrap();
        let channel = transport.channel_send(channel_kind).unwrap();
        assert_eq!(channel.message_acks().len(), 1);
        assert!(channel.message_nacks().is_empty());
    }

    #[test]
    fn packet_builder_uses_link_mtu_for_fragmentation_and_packet_size() {
        let settings = ChannelSettings::default();
        let mut registry = ChannelRegistry::default();
        let (channel_kind, channel_id) = registry.add_channel::<SmallMtuChannel>(settings);

        let mut transport = Transport::default();
        transport.add_channel_send::<SmallMtuChannel>(settings, channel_id);
        transport.add_channel_receive::<SmallMtuChannel>(settings, channel_id);

        let mut world = World::new();
        world.insert_resource(registry);
        world.init_resource::<Time<Real>>();
        world.init_resource::<LocalTimeline>();
        let entity = world
            .spawn((
                Link::default().with_mtu(lightyear_link::LinkMtu::new(256)),
                Linked,
                transport,
            ))
            .id();
        let expected = Bytes::from(vec![5; 700]);
        world
            .get::<Transport>(entity)
            .unwrap()
            .send_erased(channel_kind, expected.clone(), 1.0)
            .unwrap();

        world.run_system_once(TransportPlugin::buffer_send).unwrap();

        let packets = world
            .get_mut::<Link>(entity)
            .unwrap()
            .send
            .drain()
            .collect::<Vec<_>>();
        assert!(packets.len() > 1);
        assert!(packets.iter().all(|packet| packet.len() <= 256));

        {
            let mut link = world.get_mut::<Link>(entity).unwrap();
            packets.into_iter().for_each(|packet| {
                link.recv
                    .push_raw(lightyear_link::recv_payload_from_bytes(packet));
            });
        }
        world
            .run_system_once(TransportPlugin::buffer_receive)
            .unwrap();

        let mut transport = world.get_mut::<Transport>(entity).unwrap();
        let received = transport
            .receivers
            .get_mut(&channel_id)
            .unwrap()
            .read_message()
            .unwrap()
            .1;
        assert_eq!(received, expected);
    }
}
