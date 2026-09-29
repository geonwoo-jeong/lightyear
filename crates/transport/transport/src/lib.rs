/*! # Lightyear Packet

Packet handling for the lightyear networking library.
This crate builds up on top of lightyear-io, to add packet fragmentation, channels, and reliability.

## Optional packet admission observation

The default-disabled `packet_admission_observation` feature exposes
`ObservePacketAdmissions` and `PacketAdmitted` in the prelude. Enable the feature,
then add the marker to the transport entities whose admitted packets should emit
metadata events. This observes transport queue admission, not socket send success
or peer receipt.

Without the feature, the send system retains its original query and adaptive
iteration, with no admission-event parameters, marker checks, or event queues.
With the feature, the system has deferred commands even on unmarked entities.
Marked entities additionally allocate channel metadata and queue one event per
admitted packet. Bevy applies the events after the send system; observer work and
command synchronization therefore contribute to send-path CPU time. No packet
payload is copied or exposed.
Observation-enabled builds without this crate's `std` feature use serial send
iteration for `Commands`; observation-disabled builds retain adaptive iteration
in both configurations.
*/
#![no_std]

extern crate alloc;
#[cfg(feature = "std")]
extern crate std;

pub mod channel;

pub mod error;

#[cfg(feature = "client")]
mod client;
pub mod packet;
pub mod plugin;
#[cfg(feature = "server")]
mod server;

pub mod prelude {
    pub use crate::channel::Channel;
    pub use crate::channel::builder::{ChannelMode, ChannelSettings, ReliableSettings, Transport};
    pub use crate::channel::receive::ChannelReceive;
    pub use crate::channel::registry::AppChannelExt;
    pub use crate::channel::registry::ChannelRegistry;
    pub use crate::channel::send::ChannelSend;
    pub use crate::packet::compression::{CompressionAlgorithm, CompressionConfig};
    pub use crate::packet::nack::PacketNackSettings;
    pub use crate::packet::priority_manager::{PriorityConfig, PriorityManager};
    #[cfg(feature = "packet_admission_observation")]
    pub use crate::plugin::{ObservePacketAdmissions, PacketAdmitted};
}
