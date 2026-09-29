//! Multi-peer UDP endpoint.
//!
//! A [`UdpEndpoint`](crate::endpoint::UdpEndpoint) owns one non-blocking UDP socket bound to a
//! [`LocalAddr`](aeronet_io::connection::LocalAddr). It is the address
//! other peers reach this one at, and it fans out to one child [`Link`] per remote address, related
//! to it through [`LinkOf`](lightyear_link::prelude::LinkOf). This is what gives a peer a single stable identity: the socket it is
//! reachable on, regardless of how many peers it talks to.
//!
//! The endpoint is not tied to any role. A game server and a P2P peer both use it; only the markers
//! layered on top differ — `ServerUdpIo` (available with `server`) adds the server role.
//!
//! # Addressing
//!
//! Child links are created for inbound datagrams, because UDP has no connection to accept: the
//! source address of a datagram *is* the peer. A peer that dials another one therefore reaches it
//! without any handshake, and sending back uses the same address the datagram came from.
//!
//! A link the caller creates for a peer it dialed must be registered with
//! [`UdpEndpoint::register_link`](crate::endpoint::UdpEndpoint::register_link), or the peer's first reply will spawn a second link for the same
//! address.

use bevy_app::{App, Plugin, PostUpdate, PreUpdate};
use bevy_ecs::prelude::*;
use bevy_ecs::relationship::RelationshipTarget;
use bevy_ecs::system::ParallelCommands;
use tracing::{debug, error, info};

use crate::UdpError;
#[cfg(feature = "send_observation")]
use crate::{ObserveUdpSends, UdpSendOutcome};
use aeronet_io::connection::{LocalAddr, PeerAddr};
use bevy_platform::collections::{HashMap, hash_map::Entry};
use bytes::BufMut;
use core::net::SocketAddr;
use lightyear_core::buffer_pool::BufferPool;
use lightyear_core::time::Instant;
use lightyear_link::endpoint::EndpointLinkPlugin;
use lightyear_link::prelude::{Endpoint, LinkOf};
use lightyear_link::{Link, LinkStart, LinkSystems, Linked, Linking, Unlink, Unlinked};

/// UDP endpoint component.
///
/// Insert this on the entity that should own a UDP socket. A [`LocalAddr`] component is required
/// before [`LinkStart`] is triggered; the plugin binds one socket to that address and creates child
/// link entities for remote addresses as datagrams arrive. After binding, [`LocalAddr`] is updated to
/// the address reported by the socket, including the OS-assigned port when binding to port `0`.
///
/// Each child link receives [`PeerAddr`] for its remote socket address and [`UdpLinkOfIO`] to mark it
/// as owned by this transport.
#[derive(Component)]
#[require(Endpoint)]
pub struct UdpEndpoint {
    socket: Option<std::net::UdpSocket>,
    recv_buffers: BufferPool,
    connected_addresses: HashMap<SocketAddr, LinkOfStatus>,
}

/// Marker for child link entities owned by a [`UdpEndpoint`].
///
/// Send systems use this marker to distinguish this transport's [`LinkOf`] children from child links
/// that may belong to another transport attached to the same endpoint.
#[derive(Component)]
pub struct UdpLinkOfIO;

#[derive(Debug)]
enum LinkOfStatus {
    // we just received a packet from a new address and are in the process of spawning a new entity
    // to avoid race conditions, other connection packets from that address will be dropped for the rest of the frame
    //
    // we also won't process packets for this entity this frame, but only on the next frame (which is ok because the
    // client should be sending multiple connection packets)
    Spawning(Entity),
    // the link has been created
    Spawned(Entity),
}

impl Default for UdpEndpoint {
    fn default() -> Self {
        UdpEndpoint {
            socket: None,
            recv_buffers: crate::recv_buffer_pool(),
            connected_addresses: HashMap::with_capacity(1),
        }
    }
}

impl UdpEndpoint {
    /// Returns receive-buffer pool misses for allocation regression tests.
    #[cfg(feature = "test_utils")]
    pub fn recv_buffer_pool_misses(&self) -> usize {
        self.recv_buffers.misses()
    }

    /// Records `entity` as the link for `address`, before any datagram has arrived from it.
    ///
    /// The endpoint otherwise learns remote addresses only from inbound datagrams, so a link created
    /// for a peer that was dialed would not be found on that peer's first reply: a second link would
    /// be spawned for the same address, and the peer's datagrams would land in it while replies are
    /// sent from the first.
    pub fn register_link(&mut self, address: SocketAddr, entity: Entity) {
        self.connected_addresses
            .insert(address, LinkOfStatus::Spawned(entity));
    }
}

/// Bevy plugin that integrates the multi-peer UDP endpoint with Lightyear links.
///
/// The plugin installs:
/// - [`EndpointLinkPlugin`] for child unlink/despawn and receive-conditioner inheritance;
/// - a [`LinkStart`] observer that binds the endpoint socket and marks the endpoint [`Linked`];
/// - an [`Unlink`] observer that closes the socket;
/// - a receive system that creates or finds a child link for each remote address and queues the
///   datagram in that child [`Link::recv`];
/// - a send system that drains each UDP child [`Link::send`] to its [`PeerAddr`].
pub struct UdpEndpointPlugin;

impl UdpEndpointPlugin {
    fn link(
        trigger: On<LinkStart>,
        mut query: Query<
            (&mut UdpEndpoint, Option<&mut LocalAddr>),
            (Without<Linking>, Without<Linked>),
        >,
        mut commands: Commands,
    ) -> Result {
        if let Ok((mut udp_endpoint, local_addr)) = query.get_mut(trigger.entity) {
            let mut local_addr = local_addr.ok_or(UdpError::LocalAddrMissing)?;
            let socket = std::net::UdpSocket::bind(local_addr.0)?;
            socket.set_nonblocking(true)?;
            local_addr.0 = socket.local_addr()?;
            info!("UDP endpoint bound to {}", local_addr.0);
            udp_endpoint.socket = Some(socket);
            commands.entity(trigger.entity).insert(Linked);
        }
        Ok(())
    }

    fn unlink(trigger: On<Unlink>, mut query: Query<&mut UdpEndpoint, Without<Unlinked>>) {
        if let Ok(mut udp_endpoint) = query.get_mut(trigger.entity) {
            info!("UDP endpoint socket closed");
            udp_endpoint.socket = None;
        }
    }

    fn send(
        #[cfg(feature = "send_observation")] mut commands: Commands,
        #[cfg(not(feature = "send_observation"))] mut endpoint_query: Query<
            (&mut UdpEndpoint, &Endpoint),
            With<Linked>,
        >,
        #[cfg(feature = "send_observation")] mut endpoint_query: Query<
            (Entity, &mut UdpEndpoint, &Endpoint, Has<ObserveUdpSends>),
            With<Linked>,
        >,
        mut link_query: Query<(&mut Link, &PeerAddr), With<UdpLinkOfIO>>,
    ) {
        // TODO: parallelize
        endpoint_query.iter_mut().for_each(|item| {
            #[cfg(feature = "send_observation")]
            let (endpoint_entity, mut udp_endpoint, endpoint, observe) = item;
            #[cfg(not(feature = "send_observation"))]
            let (mut udp_endpoint, endpoint) = item;
            endpoint.collection().iter().for_each(|peer_entity| {
                let Some((mut link, remote_addr)) = link_query.get_mut(*peer_entity).ok() else {
                    // Not all links are UDP links, so we might not want this to ever print
                    debug!("Peer entity {} not found in link query", peer_entity);
                    return;
                };

                link.send.drain().for_each(|send_payload| {
                    let result = udp_endpoint
                        .socket
                        .as_mut()
                        .unwrap()
                        .send_to(send_payload.as_ref(), remote_addr.0)
                        .inspect_err(|e| {
                            error!("Error sending UDP packet to {}: {}", remote_addr.0, e);
                        });
                    #[cfg(not(feature = "send_observation"))]
                    let _ = result;
                    #[cfg(feature = "send_observation")]
                    if observe {
                        commands.trigger(UdpSendOutcome::from_result(
                            endpoint_entity,
                            *peer_entity,
                            remote_addr.0,
                            send_payload.len(),
                            &result,
                        ));
                    }
                });
            });
        });
    }

    fn receive(
        commands: ParallelCommands,
        mut endpoint_query: Query<(Entity, &mut UdpEndpoint), With<Linked>>,
        // TODO: we want to have With<Linked> here, but that would mean that if a client sends 2 packets in a row
        //  for the first one we spawn them, and for the second one the query will return False.
        //  maybe have a separate Vec for new addresses, and for these we don't require Linked?
        link_query: Query<Option<&mut Link>>,
    ) {
        endpoint_query
            // TODO: would par_iter_mut be better here?
            .iter_mut()
            .for_each(|(endpoint_entity, mut udp_endpoint)| {
                // SAFETY: we know that each UdpEndpoint will target different Link entities, so there won't be any aliasing
                let mut link_query = unsafe { link_query.reborrow_unsafe() };

                // enable split borrows
                let udp_endpoint = &mut *udp_endpoint;
                udp_endpoint.recv_buffers.reclaim_pending();

                loop {
                    let mut buffer = udp_endpoint.recv_buffers.take();
                    // Check how much actual uninitialized space we have at the end
                    let capacity = buffer.capacity();
                    let current_len = buffer.len();
                    assert_eq!(current_len, 0);
                    let available_uninit = capacity - current_len;
                    let max_recv_len = core::cmp::min(available_uninit, crate::MTU);

                    // We get a raw pointer to the start of the uninitialized region.
                    // SAFETY: `take` returns a buffer with at least `MTU` bytes of writable capacity.
                    let buf_slice: &mut [u8] = unsafe {
                        let ptr = buffer.as_mut_ptr().add(current_len);
                        core::slice::from_raw_parts_mut(ptr, max_recv_len)
                    };
                    match udp_endpoint.socket.as_mut().unwrap().recv_from(buf_slice) {
                        Ok((recv_len, address)) => {
                            // Mark the received bytes as initialized
                            // SAFETY: we know that the buffer is large enough to hold the received data.
                            unsafe {
                                buffer.advance_mut(recv_len);
                            }
                            let payload = udp_endpoint.recv_buffers.split_for_handoff(buffer);
                            match udp_endpoint.connected_addresses.entry(address) {
                                Entry::Occupied(mut entry) => {
                                    match *entry.get_mut() {
                                        LinkOfStatus::Spawning(_) => {
                                            // we are still spawning the entity, so we will drop this packet
                                            // and wait for the next one
                                            continue;
                                        }
                                        LinkOfStatus::Spawned(entity) => {
                                            match link_query.get_mut(entity) {
                                                Ok(mut link) => {
                                                    match link.as_mut() {
                                                        None => {
                                                            debug!("despawning entity {} because it has no udp link", entity);
                                                            // the entity exists but has not link.
                                                            // this is a weird state, let's despawn it
                                                            entry.remove();
                                                            commands.command_scope(|mut c| {
                                                                if let Ok(mut e) = c.get_entity(entity) {
                                                                    e.try_despawn();
                                                                }
                                                            });
                                                        }
                                                        Some(link) => {
                                                            link.recv.push(payload, Instant::now());
                                                        }
                                                    }
                                                }
                                                Err(_) => {
                                                    error!(
                                                        "Received UDP packet for unknown entity: {}",
                                                        entity
                                                    );
                                                    // this might because the remote entity has disconnected and is trying to reconnect.
                                                    // Remove the entry so that the next packet can be processed
                                                    entry.remove();
                                                    continue;
                                                }
                                            }
                                        }
                                    }
                                }
                                Entry::Vacant(vacant) => {
                                    // we are spawning a new entity but the initial packets will be dropped
                                    let mut link = Link::default();
                                    link.recv.push(payload, Instant::now());
                                    commands.command_scope(|mut c| {
                                        let entity = c
                                            .spawn((
                                                LinkOf {
                                                    endpoint: endpoint_entity,
                                                },
                                                link,
                                                Linked,
                                                PeerAddr(address),
                                                UdpLinkOfIO,
                                                // TODO: should we add LocalAddr?
                                            ))
                                            .id();
                                        info!(?entity, ?endpoint_entity, "Received UDP packet from new address {address}, Spawn new LinkOf");
                                        vacant.insert(LinkOfStatus::Spawning(entity));
                                    });
                                    continue;
                                }
                            };
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            udp_endpoint.recv_buffers.recycle(buffer);
                            break;
                        }
                        // Windows-specific: when a UDP client disconnects, the OS sends an
                        // ICMP "port unreachable" back, which surfaces as ConnectionReset on
                        // the next recv. This is harmless — just skip to the next packet.
                        Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                            udp_endpoint.recv_buffers.recycle(buffer);
                            continue;
                        }
                        Err(e) => {
                            udp_endpoint.recv_buffers.recycle(buffer);
                            error!("Error receiving UDP packet: {}", e);
                            break;
                        }
                    }
                }

                // set every spawning to spawned
                udp_endpoint.connected_addresses.iter_mut().for_each(|(addr, status)| {
                    if let LinkOfStatus::Spawning(entity) = status {
                        *status = LinkOfStatus::Spawned(*entity);
                    }
                });
            });
    }
}

impl Plugin for UdpEndpointPlugin {
    fn build(&self, app: &mut App) {
        if !app.is_plugin_added::<EndpointLinkPlugin>() {
            app.add_plugins(EndpointLinkPlugin);
        }
        app.add_observer(Self::link);
        app.add_observer(Self::unlink);
        app.add_systems(PreUpdate, Self::receive.in_set(LinkSystems::Receive));
        app.add_systems(PostUpdate, Self::send.in_set(LinkSystems::Send));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "send_observation")]
    use bytes::Bytes;
    use core::net::Ipv4Addr;
    #[cfg(feature = "send_observation")]
    use core::time::Duration;
    use lightyear_link::prelude::LinkConditionerConfig;
    use lightyear_link::{RecvLinkConditioner, UnlinkReason};
    #[cfg(feature = "send_observation")]
    use std::io::ErrorKind;
    #[cfg(feature = "send_observation")]
    use std::net::UdpSocket;

    #[derive(Resource, Default)]
    struct UnlinkedChildren(Vec<Entity>);

    #[test]
    fn send_only_has_deferred_work_when_observation_is_compiled_in() {
        let mut world = World::new();
        let mut system = IntoSystem::into_system(UdpEndpointPlugin::send);
        system.initialize(&mut world);
        assert_eq!(system.has_deferred(), cfg!(feature = "send_observation"));
    }

    #[cfg(feature = "send_observation")]
    #[derive(Resource, Default)]
    struct SendOutcomes(Vec<UdpSendOutcome>);

    #[cfg(feature = "send_observation")]
    #[test]
    fn send_reports_endpoint_and_peer_context_with_or_without_observer() {
        for observe in [false, true] {
            let mut app = App::new();
            app.add_plugins(UdpEndpointPlugin);
            app.init_resource::<SendOutcomes>();
            app.add_observer(
                |event: On<UdpSendOutcome>, mut outcomes: ResMut<SendOutcomes>| {
                    outcomes.0.push(*event);
                },
            );
            let endpoint = app
                .world_mut()
                .spawn((
                    UdpEndpoint::default(),
                    LocalAddr("127.0.0.1:0".parse().unwrap()),
                ))
                .id();
            if observe {
                app.world_mut().entity_mut(endpoint).insert(ObserveUdpSends);
            }
            app.world_mut().trigger(LinkStart { entity: endpoint });
            app.world_mut().flush();

            let peers: Vec<_> = (0..2)
                .map(|_| {
                    let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
                    receiver
                        .set_read_timeout(Some(Duration::from_secs(1)))
                        .unwrap();
                    let remote_addr = receiver.local_addr().unwrap();
                    let mut link = Link::default();
                    link.send.push(Bytes::from_static(b"first"));
                    link.send.push(Bytes::from(vec![0; 65_536]));
                    link.send.push(Bytes::from_static(b"last"));
                    let entity = app
                        .world_mut()
                        .spawn((
                            LinkOf { endpoint },
                            link,
                            PeerAddr(remote_addr),
                            UdpLinkOfIO,
                            Linked,
                        ))
                        .id();
                    (entity, remote_addr, receiver)
                })
                .collect();
            app.world_mut().run_schedule(PostUpdate);
            app.world_mut().run_schedule(PostUpdate);

            let outcomes = &app.world().resource::<SendOutcomes>().0;
            assert_eq!(outcomes.len(), if observe { 6 } else { 0 });
            for (entity, remote_addr, receiver) in peers {
                assert_eq!(app.world().get::<Link>(entity).unwrap().send.len(), 0);
                if observe {
                    let peer_outcomes: Vec<_> = outcomes
                        .iter()
                        .filter(|outcome| outcome.entity == entity)
                        .collect();
                    assert_eq!(peer_outcomes.len(), 3);
                    for outcome in &peer_outcomes {
                        assert_eq!(outcome.socket_entity, endpoint);
                        assert_eq!(outcome.remote_addr, remote_addr);
                    }
                    assert_eq!(peer_outcomes[0].attempted_bytes, 5);
                    assert_eq!(peer_outcomes[0].result, Ok(5));
                    assert_eq!(peer_outcomes[1].attempted_bytes, 65_536);
                    assert!(peer_outcomes[1].result.unwrap_err().raw_os_error.is_some());
                    assert_eq!(peer_outcomes[2].attempted_bytes, 4);
                    assert_eq!(peer_outcomes[2].result, Ok(4));
                }

                let mut buffer = [0; 16];
                for expected in [&b"first"[..], &b"last"[..]] {
                    let (len, _) = receiver.recv_from(&mut buffer).unwrap();
                    assert_eq!(&buffer[..len], expected);
                }
                receiver.set_nonblocking(true).unwrap();
                assert_eq!(
                    receiver.recv_from(&mut buffer).unwrap_err().kind(),
                    ErrorKind::WouldBlock
                );
            }
        }
    }

    #[test]
    fn standalone_endpoint_unlinks_and_despawns_children() {
        let mut app = App::new();
        app.add_plugins(UdpEndpointPlugin);
        app.init_resource::<UnlinkedChildren>();
        app.add_observer(
            |trigger: On<Unlink>, mut unlinked: ResMut<UnlinkedChildren>| {
                unlinked.0.push(trigger.entity);
            },
        );

        let endpoint = app.world_mut().spawn((UdpEndpoint::default(), Linked)).id();
        let child = app
            .world_mut()
            .spawn((LinkOf { endpoint }, Link::default(), UdpLinkOfIO, Linked))
            .id();

        app.world_mut().entity_mut(endpoint).insert(Unlinked {
            reason: UnlinkReason::UserRequested(None),
        });
        app.world_mut().flush();

        assert_eq!(app.world().resource::<UnlinkedChildren>().0, [child]);
        assert!(app.world().get_entity(child).is_err());
        assert!(app.world().get::<UdpEndpoint>(endpoint).is_some());
    }

    #[test]
    fn standalone_endpoint_conditions_initial_and_subsequent_child_packets() {
        let mut app = App::new();
        app.add_plugins(UdpEndpointPlugin);
        let endpoint = app
            .world_mut()
            .spawn((
                UdpEndpoint::default(),
                Endpoint::new(Some(RecvLinkConditioner::new(
                    LinkConditionerConfig::default().with_fixed_loss(1.0),
                ))),
            ))
            .id();

        // UDP queues the first datagram before spawning the child relationship.
        let mut link = Link::default();
        link.recv
            .push(bytes::BytesMut::from(&b"initial"[..]), Instant::now());
        let child = app
            .world_mut()
            .spawn((LinkOf { endpoint }, link, UdpLinkOfIO, Linked))
            .id();
        app.update();

        {
            let mut link = app.world_mut().get_mut::<Link>(child).unwrap();
            assert!(
                link.recv.pop().is_none(),
                "the first datagram must be dropped"
            );
            link.recv
                .push(bytes::BytesMut::from(&b"subsequent"[..]), Instant::now());
        }
        app.update();
        assert!(
            app.world_mut()
                .get_mut::<Link>(child)
                .unwrap()
                .recv
                .pop()
                .is_none(),
            "later datagrams must inherit packet loss",
        );
    }

    #[test]
    fn link_updates_local_addr_with_os_assigned_port() {
        let mut app = App::new();
        app.add_plugins(UdpEndpointPlugin);

        let requested_addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 0);
        let endpoint = app
            .world_mut()
            .spawn((LocalAddr(requested_addr), UdpEndpoint::default()))
            .id();

        app.world_mut().trigger(LinkStart { entity: endpoint });
        app.world_mut().flush();

        let bound_addr = app.world().get::<LocalAddr>(endpoint).unwrap().0;
        assert_eq!(bound_addr.ip(), requested_addr.ip());
        assert_ne!(bound_addr.port(), 0);
        assert!(app.world().get::<Linked>(endpoint).is_some());
    }
}
