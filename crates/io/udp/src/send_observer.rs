use alloc::boxed::Box;
use bevy_ecs::prelude::{Entity, Resource};
use core::net::SocketAddr;
use std::io::{self, ErrorKind};

/// Optional observer of the local result of each UDP send attempt.
///
/// Insert this resource to observe both single-peer sockets and multi-peer endpoints. The callback
/// runs synchronously after each `UdpSocket::send_to` call and may run concurrently for different
/// sockets. It must be nonblocking and should not panic, since it runs inside the send system.
///
/// Outcomes contain metadata only, without access to the datagram payload. Successful sends report
/// the bytes accepted by the local socket; they do not confirm peer delivery or acknowledgement.
/// Observation does not retry failed sends or change how outgoing queues are drained. Without this
/// resource, the transport performs no per-datagram allocation or payload copy for observation.
///
/// ```
/// use bevy_app::App;
/// use lightyear_udp::{UdpPlugin, prelude::UdpSendObserver};
/// use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
///
/// let sent_bytes = Arc::new(AtomicUsize::new(0));
/// let counter = Arc::clone(&sent_bytes);
/// let mut app = App::new();
/// app.add_plugins(UdpPlugin);
/// app.insert_resource(UdpSendObserver::new(move |outcome| {
///     if let Ok(bytes) = outcome.result {
///         counter.fetch_add(bytes, Ordering::Relaxed);
///     }
/// }));
/// ```
#[derive(Resource)]
pub struct UdpSendObserver(Box<dyn Fn(UdpSendOutcome) + Send + Sync + 'static>);

impl UdpSendObserver {
    /// Creates an observer whose callback receives one outcome per socket send attempt.
    pub fn new(observer: impl Fn(UdpSendOutcome) + Send + Sync + 'static) -> Self {
        Self(Box::new(observer))
    }
}

/// Metadata for the actual result returned by `UdpSocket::send_to`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpSendOutcome {
    /// Entity owning the UDP socket: a `UdpIo` or `UdpEndpoint` entity.
    pub socket_entity: Entity,
    /// Entity whose `Link::send` queue supplied the datagram.
    ///
    /// Equal to [`socket_entity`](Self::socket_entity) for a single-peer `UdpIo`.
    pub link_entity: Entity,
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

pub(crate) fn observe_send_result(
    observer: Option<&UdpSendObserver>,
    socket_entity: Entity,
    link_entity: Entity,
    remote_addr: SocketAddr,
    attempted_bytes: usize,
    result: &io::Result<usize>,
) {
    if let Some(observer) = observer {
        (observer.0)(UdpSendOutcome {
            socket_entity,
            link_entity,
            remote_addr,
            attempted_bytes,
            result: result.as_ref().copied().map_err(|error| UdpSendError {
                kind: error.kind(),
                raw_os_error: error.raw_os_error(),
            }),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::sync::Arc;
    use bevy_ecs::world::World;
    use std::sync::Mutex;

    #[test]
    fn preserves_send_results_and_context() {
        let mut world = World::new();
        let socket_entity = world.spawn_empty().id();
        let link_entity = world.spawn_empty().id();
        let remote_addr = "127.0.0.1:12345".parse().unwrap();
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&outcomes);
        let observer = UdpSendObserver::new(move |outcome| captured.lock().unwrap().push(outcome));
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

        for result in &results {
            observe_send_result(
                Some(&observer),
                socket_entity,
                link_entity,
                remote_addr,
                8,
                result,
            );
        }

        let outcomes = outcomes.lock().unwrap();
        assert_eq!(outcomes.len(), results.len());
        for outcome in outcomes.iter() {
            assert_eq!(outcome.socket_entity, socket_entity);
            assert_eq!(outcome.link_entity, link_entity);
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
