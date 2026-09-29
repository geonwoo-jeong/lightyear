use bevy_ecs::prelude::{Component, Entity, EntityEvent};
use core::net::SocketAddr;
use std::io::{self, ErrorKind};

/// Opts a UDP socket into [`UdpSendOutcome`] events.
///
/// Insert this component on a [`crate::UdpIo`] or `UdpEndpoint` entity.
/// For an endpoint, outcomes target its child links and identify the socket separately.
/// Remove the component to stop queuing new events; already queued events still run.
///
/// Events capture each local `send_to` result and are triggered when deferred commands are
/// applied. Observers run as normal Bevy observers, not inline on the socket send path.
/// Delivery order across sockets is unspecified. Enabling observation adds one deferred
/// command per send attempt. Without this component no outcome events are queued.
/// An observer's execution time is not the socket call's completion time.
///
/// ```
/// use bevy_app::App;
/// use bevy_ecs::prelude::*;
/// use lightyear_udp::{UdpPlugin, prelude::*};
///
/// #[derive(Resource, Default)]
/// struct SentBytes(usize);
///
/// let mut app = App::new();
/// app.add_plugins(UdpPlugin).init_resource::<SentBytes>();
/// app.add_observer(|event: On<UdpSendOutcome>, mut bytes: ResMut<SentBytes>| {
///     if let Ok(sent) = event.result {
///         bytes.0 += sent;
///     }
/// });
/// // Add LocalAddr/PeerAddr and trigger LinkStart as usual to connect this socket.
/// app.world_mut().spawn((UdpIo::default(), ObserveUdpSends));
/// ```
#[derive(Component, Clone, Copy, Debug, Default)]
pub struct ObserveUdpSends;

/// Event carrying the actual local result returned by `UdpSocket::send_to`.
///
/// Triggered only for sockets with [`ObserveUdpSends`]. A successful result reports
/// bytes accepted by the local socket, not peer delivery or acknowledgement.
/// Observation does not retry failed sends or change outgoing queue draining.
#[derive(EntityEvent, Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpSendOutcome {
    /// Entity owning the UDP socket: a `UdpIo` or `UdpEndpoint` entity.
    pub socket_entity: Entity,
    /// Entity whose `Link::send` queue supplied the datagram.
    ///
    /// Equal to [`socket_entity`](Self::socket_entity) for a single-peer `UdpIo`.
    pub entity: Entity,
    /// Destination passed to `send_to`.
    pub remote_addr: SocketAddr,
    /// Datagram length passed to `send_to`.
    pub attempted_bytes: usize,
    /// Exact returned byte count or error metadata, without inferring delivery or an ACK.
    ///
    /// A successful count is preserved even if it differs from [`attempted_bytes`](Self::attempted_bytes).
    pub result: Result<usize, UdpSendError>,
}

/// Copyable error metadata from a failed UDP send attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpSendError {
    /// Error category reported by [`io::Error::kind`].
    pub kind: ErrorKind,
    /// Platform-specific error code, if supplied by [`io::Error::raw_os_error`].
    pub raw_os_error: Option<i32>,
}

impl UdpSendOutcome {
    pub(crate) fn from_result(
        socket_entity: Entity,
        entity: Entity,
        remote_addr: SocketAddr,
        attempted_bytes: usize,
        result: &io::Result<usize>,
    ) -> Self {
        Self {
            socket_entity,
            entity,
            remote_addr,
            attempted_bytes,
            result: result.as_ref().copied().map_err(|error| UdpSendError {
                kind: error.kind(),
                raw_os_error: error.raw_os_error(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_ecs::world::World;

    #[test]
    fn preserves_send_results_and_context() {
        let mut world = World::new();
        let socket_entity = world.spawn_empty().id();
        let link_entity = world.spawn_empty().id();
        let remote_addr = "127.0.0.1:12345".parse().unwrap();
        let os_error = io::Error::from_raw_os_error(123);
        let expected_error = UdpSendError {
            kind: os_error.kind(),
            raw_os_error: Some(123),
        };
        let results = [
            Ok(8),
            Ok(3),
            Ok(0),
            Err(io::Error::from(ErrorKind::WouldBlock)),
            Err(os_error),
        ];

        let outcomes: Vec<_> = results
            .iter()
            .map(|result| {
                UdpSendOutcome::from_result(socket_entity, link_entity, remote_addr, 8, result)
            })
            .collect();

        assert_eq!(outcomes.len(), results.len());
        for outcome in outcomes.iter() {
            assert_eq!(outcome.socket_entity, socket_entity);
            assert_eq!(outcome.entity, link_entity);
            assert_eq!(outcome.remote_addr, remote_addr);
            assert_eq!(outcome.attempted_bytes, 8);
        }
        assert_eq!(outcomes[0].result, Ok(8));
        assert_eq!(outcomes[1].result, Ok(3));
        assert_eq!(outcomes[2].result, Ok(0));
        assert_eq!(
            outcomes[3].result,
            Err(UdpSendError {
                kind: ErrorKind::WouldBlock,
                raw_os_error: None,
            })
        );
        assert_eq!(outcomes[4].result, Err(expected_error));
    }
}
