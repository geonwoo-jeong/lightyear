use bevy_ecs::prelude::Component;
use core::num::NonZeroUsize;

/// Optional limit on receive attempts per UDP socket and receive-system invocation.
///
/// Insert this on the entity owning a `UdpIo` or `UdpEndpoint` socket. Each call to
/// `UdpSocket::recv_from` counts, including errors and datagrams that the endpoint discards. An
/// endpoint shares its limit across all remote peers; inserting this on a child link has no effect.
///
/// Once the limit is reached, receiving yields with unread datagrams left in the socket's queue.
/// The next invocation (normally the next `PreUpdate`) starts with a fresh limit. Other sockets
/// still get their own receive turn. To bound work across all sockets, configure each socket.
///
/// Missing or removing this component preserves the unbounded receive loop. Zero is excluded by
/// the field's type. The component does not change socket startup, shutdown, or error handling.
///
/// ```
/// use bevy_ecs::world::World;
/// use core::num::NonZeroUsize;
/// use lightyear_udp::prelude::{UdpIo, UdpReceiveLimit};
///
/// let mut world = World::new();
/// let entity = world.spawn((
///     UdpIo::default(),
///     UdpReceiveLimit(NonZeroUsize::new(64).unwrap()),
/// )).id();
/// // Remove the component to receive until the socket would block again.
/// world.entity_mut(entity).remove::<UdpReceiveLimit>();
/// ```
#[derive(Component, Clone, Copy, Debug, PartialEq, Eq)]
pub struct UdpReceiveLimit(pub NonZeroUsize);

/// Kept separate from receive results so every attempt consumes budget, including `continue`s.
pub(crate) struct ReceiveBudget {
    remaining: Option<usize>,
}

impl ReceiveBudget {
    pub(crate) fn new(limit: Option<&UdpReceiveLimit>) -> Self {
        Self {
            remaining: limit.map(|limit| limit.0.get()),
        }
    }

    pub(crate) fn take_attempt(&mut self) -> bool {
        match self.remaining.as_mut() {
            Some(0) => false,
            Some(remaining) => {
                *remaining -= 1;
                true
            }
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn continuously_ready_or_resetting_receiver_exhausts_budget() {
        for result in [Ok(()), Err(ErrorKind::ConnectionReset)] {
            let limit = UdpReceiveLimit(NonZeroUsize::new(3).unwrap());
            let mut budget = ReceiveBudget::new(Some(&limit));
            let mut attempts = 0;
            // A receiver that never returns WouldBlock must still yield, including when every
            // result follows the ConnectionReset `continue` path.
            while budget.take_attempt() {
                attempts += 1;
                assert!(attempts <= 3);
                match result {
                    Ok(()) => {}
                    Err(ErrorKind::ConnectionReset) => continue,
                    Err(_) => break,
                }
            }
            assert_eq!(attempts, 3);
            assert!(!budget.take_attempt());
            assert!(ReceiveBudget::new(Some(&limit)).take_attempt());
        }
    }

    #[test]
    fn missing_limit_does_not_exhaust_budget() {
        let mut budget = ReceiveBudget::new(None);
        for _ in 0..10_000 {
            assert!(budget.take_attempt());
        }
    }
}
