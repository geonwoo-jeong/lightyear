//! Runs real Lightyear systems. No GUI, simulated networking, or benchmark-only send helper.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    net::UdpSocket,
    sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    time::{Duration, Instant},
};

use aeronet_io::connection::{LocalAddr, PeerAddr};
use bevy_app::{App, PostUpdate, PreUpdate, TaskPoolOptions, TaskPoolPlugin};
use bevy_ecs::prelude::*;
use bevy_time::{Real, Time};
use bytes::{Bytes, BytesMut};
use lightyear_core::prelude::LocalTimeline;
use lightyear_link::{Link, LinkMtu, LinkStart, LinkSystems, Linked, prelude::LinkOf};
use lightyear_transport::{plugin::TransportPlugin, prelude::*};
use lightyear_udp::{
    UdpIo, UdpPlugin,
    endpoint::{UdpEndpoint, UdpEndpointPlugin, UdpLinkOfIO},
};

struct CountingAllocator;
static ACTIVE: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static REALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

// All variants use this wrapper. Timing runs leave ACTIVE=false. Counting runs
// are separate samples; allocator instrumentation changes the measured program.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc(layout) };
        if !result.is_null() && ACTIVE.load(Relaxed) {
            ALLOCATIONS.fetch_add(1, Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        result
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let result = unsafe { System.alloc_zeroed(layout) };
        if !result.is_null() && ACTIVE.load(Relaxed) {
            ALLOCATIONS.fetch_add(1, Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        result
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ACTIVE.load(Relaxed) {
            DEALLOCATIONS.fetch_add(1, Relaxed);
            DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(ptr, layout, new_size) };
        if !result.is_null() && ACTIVE.load(Relaxed) {
            REALLOCATIONS.fetch_add(1, Relaxed);
            REALLOCATED_BYTES.fetch_add(new_size as u64, Relaxed);
        }
        result
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Resource, Default)]
struct Observations {
    packets: u64,
    bytes: u64,
    #[allow(dead_code)] // Read by the admission observer only in API-enabled variants.
    channels: u64,
    errors: u64,
}

#[derive(Default)]
struct Totals {
    schedule_elapsed_ns: u128,
    receive_schedule_calls: u64,
    packets: u64,
    bytes: u64,
    messages: u64,
    checksum: u64,
    observation_packets: u64,
}

struct Options {
    scenario: String,
    iterations: usize,
    observe: bool,
    count_allocations: bool,
}

fn options() -> Options {
    let mut args = std::env::args().skip(1);
    let mut result = Options {
        scenario: String::new(),
        iterations: 1000,
        observe: false,
        count_allocations: false,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--scenario" => result.scenario = args.next().expect("--scenario needs a name"),
            "--iterations" => {
                result.iterations = args
                    .next()
                    .expect("--iterations needs a count")
                    .parse()
                    .expect("invalid iterations")
            }
            "--observe" => result.observe = true,
            "--count-allocations" => result.count_allocations = true,
            _ => panic!("unknown argument {arg}"),
        }
    }
    assert!(
        (1..=10000).contains(&result.iterations),
        "iterations must be 1..=10000"
    );
    result
}

fn base_app() -> App {
    let mut app = App::new();
    app.add_plugins(TaskPoolPlugin {
        task_pool_options: TaskPoolOptions::with_num_threads(1),
    });
    app.init_resource::<Observations>();
    app
}

fn finish(app: &mut App) {
    app.finish();
    app.cleanup();
    app.world_mut().flush();
}

// Keep a downstream consumer stage present so automatic ApplyDeferred barriers
// induced by observation commands are included, even without a real IO backend.
fn downstream_send_stage() {}

#[allow(unused_variables)] // Baseline intentionally has no observation API.
fn udp_observer(app: &mut App, enabled: bool) {
    if !enabled {
        return;
    }
    #[cfg(feature = "udp_observation")]
    app.add_observer(
        |event: On<lightyear_udp::UdpSendOutcome>, mut stats: ResMut<Observations>| {
            stats.packets += 1;
            match event.result {
                Ok(bytes) if bytes == event.attempted_bytes => stats.bytes += bytes as u64,
                _ => stats.errors += 1,
            }
        },
    );
    #[cfg(not(feature = "udp_observation"))]
    panic!("UDP observation API was not enabled for this binary");
}

#[allow(unused_variables)]
fn mark_udp(app: &mut App, entity: Entity, enabled: bool) {
    if !enabled {
        return;
    }
    #[cfg(feature = "udp_observation")]
    app.world_mut()
        .entity_mut(entity)
        .insert(lightyear_udp::ObserveUdpSends);
    #[cfg(not(feature = "udp_observation"))]
    unreachable!();
}

#[allow(unused_variables)]
fn transport_observer(app: &mut App, enabled: bool) {
    if !enabled {
        return;
    }
    #[cfg(feature = "transport_observation")]
    app.add_observer(
        |event: On<lightyear_transport::plugin::PacketAdmitted>,
         mut stats: ResMut<Observations>| {
            stats.packets += 1;
            stats.bytes += event.bytes as u64;
            stats.channels += event.channels.len() as u64;
        },
    );
    #[cfg(not(feature = "transport_observation"))]
    panic!("transport observation API was not enabled for this binary");
}

#[allow(unused_variables)]
fn mark_transport(app: &mut App, entity: Entity, enabled: bool) {
    if !enabled {
        return;
    }
    #[cfg(feature = "transport_observation")]
    app.world_mut()
        .entity_mut(entity)
        .insert(lightyear_transport::plugin::ObservePacketAdmissions);
    #[cfg(not(feature = "transport_observation"))]
    unreachable!();
}

fn measure(app: &mut App, receive: bool, timed: bool, count: bool) -> u128 {
    ACTIVE.store(timed && count, Relaxed);
    let started = Instant::now();
    if receive {
        app.world_mut().run_schedule(PreUpdate);
    } else {
        app.world_mut().run_schedule(PostUpdate);
    }
    // Include any deferred observer commands in the measured schedule cost.
    app.world_mut().flush();
    let elapsed = started.elapsed().as_nanos();
    ACTIVE.store(false, Relaxed);
    if timed { elapsed } else { 0 }
}

fn receive_exact(socket: &UdpSocket, expected: &[u8], count: usize) -> u64 {
    let mut checksum = 0u64;
    let mut buffer = [0u8; 2048];
    for _ in 0..count {
        let (len, _) = socket
            .recv_from(&mut buffer)
            .expect("UDP correctness failure: missing datagram (1s timeout)");
        assert_eq!(&buffer[..len], expected, "UDP payload changed");
        checksum += buffer[..len].iter().map(|&x| x as u64).sum::<u64>();
    }
    // No blocking probe for an extra packet; the next batch also verifies payloads.
    checksum
}

fn verify_observations(app: &App, enabled: bool, packets: u64, bytes: u64) -> u64 {
    let stats = app.world().resource::<Observations>();
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.packets, if enabled { packets } else { 0 });
    assert_eq!(stats.bytes, if enabled { bytes } else { 0 });
    stats.packets
}

fn udp_send(options: &Options, peers: usize) -> Totals {
    let mut app = base_app();
    if peers == 1 {
        app.add_plugins(UdpPlugin);
    } else {
        app.add_plugins(UdpEndpointPlugin);
    }
    app.add_systems(PostUpdate, downstream_send_stage.after(LinkSystems::Send));
    udp_observer(&mut app, options.observe);
    let mut sockets = Vec::new();
    let mut links = Vec::new();
    let mut payloads = Vec::new();
    let endpoint = (peers > 1).then(|| {
        app.world_mut()
            .spawn((
                UdpEndpoint::default(),
                LocalAddr("127.0.0.1:0".parse().unwrap()),
            ))
            .id()
    });
    for peer in 0..peers {
        let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let remote = socket.local_addr().unwrap();
        let entity = match endpoint {
            Some(endpoint) => app
                .world_mut()
                .spawn((
                    Link::default(),
                    Linked,
                    LinkOf { endpoint },
                    UdpLinkOfIO,
                    PeerAddr(remote),
                ))
                .id(),
            None => app
                .world_mut()
                .spawn((
                    UdpIo::default(),
                    LocalAddr("127.0.0.1:0".parse().unwrap()),
                    PeerAddr(remote),
                ))
                .id(),
        };
        sockets.push(socket);
        links.push(entity);
        payloads.push(Bytes::from(vec![(peer + 1) as u8; 512]));
    }
    let socket_entity = endpoint.unwrap_or(links[0]);
    mark_udp(&mut app, socket_entity, options.observe);
    app.world_mut().trigger(LinkStart {
        entity: socket_entity,
    });
    finish(&mut app);
    let mut totals = Totals::default();
    // 16 datagrams per peer (8KiB per receiver) avoids measuring socket overflow.
    for batch in 0..(100 + options.iterations) {
        let timed = batch >= 100;
        *app.world_mut().resource_mut::<Observations>() = Observations::default();
        for (&entity, payload) in links.iter().zip(&payloads) {
            let mut link = app.world_mut().get_mut::<Link>(entity).unwrap();
            for _ in 0..16 {
                link.send.push(payload.clone());
            }
        }
        totals.schedule_elapsed_ns += measure(&mut app, false, timed, options.count_allocations);
        let mut checksum = 0;
        for ((socket, payload), &entity) in sockets.iter().zip(&payloads).zip(&links) {
            assert_eq!(app.world().get::<Link>(entity).unwrap().send.len(), 0);
            checksum += receive_exact(socket, payload, 16);
        }
        let packets = (peers * 16) as u64;
        let bytes = packets * 512;
        let observed = verify_observations(&app, options.observe, packets, bytes);
        if timed {
            totals.packets += packets;
            totals.bytes += bytes;
            totals.checksum += checksum;
            totals.observation_packets += observed;
        }
    }
    for socket in &sockets {
        socket.set_nonblocking(true).unwrap();
        let mut buffer = [0u8; 2048];
        assert_eq!(
            socket.recv_from(&mut buffer).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock,
            "unexpected extra UDP datagram"
        );
    }
    totals
}

fn udp_receive(options: &Options) -> Totals {
    assert!(
        !options.observe,
        "receive workload has no send observations"
    );
    let mut app = base_app();
    app.add_plugins(UdpEndpointPlugin);
    let endpoint = app
        .world_mut()
        .spawn((
            UdpEndpoint::default(),
            LocalAddr("127.0.0.1:0".parse().unwrap()),
        ))
        .id();
    app.world_mut().trigger(LinkStart { entity: endpoint });
    finish(&mut app);
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    sender
        .set_write_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let child = app
        .world_mut()
        .spawn((
            Link::default(),
            Linked,
            LinkOf { endpoint },
            UdpLinkOfIO,
            PeerAddr(sender.local_addr().unwrap()),
        ))
        .id();
    app.world_mut()
        .get_mut::<UdpEndpoint>(endpoint)
        .unwrap()
        .register_link(sender.local_addr().unwrap(), child);
    app.world_mut().flush();
    assert!(
        app.world().get::<Linked>(endpoint).is_some(),
        "receive endpoint must be Linked before timing"
    );
    assert!(
        app.world().get::<Linked>(child).is_some(),
        "receive child must be Linked before timing"
    );
    let address = app.world().get::<LocalAddr>(endpoint).unwrap().0;
    let payload = [0x5au8; 512];
    let mut totals = Totals::default();
    for batch in 0..(100 + options.iterations) {
        for _ in 0..16 {
            assert_eq!(sender.send_to(&payload, address).unwrap(), 512);
        }
        // Untimed prefill only. A successful send does not guarantee that all
        // datagrams are already visible to this nonblocking receiver.
        let receive_deadline = Instant::now() + Duration::from_secs(1);
        std::thread::sleep(Duration::from_millis(1));
        let timed = batch >= 100;
        let mut count = 0;
        let mut checksum = 0;
        loop {
            assert!(
                Instant::now() < receive_deadline,
                "receive batch {batch} timed out after one second with {count}/16 datagrams"
            );
            // Keep every pass's time, including passes that find no packets.
            totals.schedule_elapsed_ns += measure(&mut app, true, timed, options.count_allocations);
            if timed {
                totals.receive_schedule_calls += 1;
            }
            {
                let mut link = app.world_mut().get_mut::<Link>(child).unwrap();
                for packet in link.recv.drain() {
                    assert_eq!(packet.as_ref(), payload);
                    count += 1;
                    checksum += packet.iter().map(|&x| x as u64).sum::<u64>();
                }
            }
            assert!(
                count <= 16,
                "receive batch {batch} contained extra datagrams"
            );
            assert!(
                Instant::now() < receive_deadline,
                "receive batch {batch} exceeded its one-second deadline"
            );
            if count == 16 {
                break;
            }
            // Outside measured schedules and allocation counting. No resend,
            // dropped timing, partial success, or busy-wait is permitted.
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            count, 16,
            "receive workload must consume the entire bounded batch {batch}; child={child:?}, endpoint={endpoint:?}"
        );
        if timed {
            totals.packets += count;
            totals.bytes += count * 512;
            totals.checksum += checksum;
        }
    }
    totals
}

struct BenchChannel;

fn transport_app(links: usize, observe: bool, receiving: bool) -> (App, Vec<Entity>) {
    let mut app = base_app();
    app.init_resource::<Time<Real>>();
    app.init_resource::<LocalTimeline>();
    let mut registry = ChannelRegistry::default();
    let settings = ChannelSettings {
        mode: ChannelMode::UnorderedUnreliable,
        ..Default::default()
    };
    let (_, id) = registry.add_channel::<BenchChannel>(settings);
    app.insert_resource(registry);
    app.add_plugins(TransportPlugin);
    app.add_systems(PostUpdate, downstream_send_stage.in_set(LinkSystems::Send));
    transport_observer(&mut app, observe);
    let entities = (0..links)
        .map(|_| {
            let mut transport = Transport::default();
            if receiving {
                transport.add_channel_receive::<BenchChannel>(settings, id);
            } else {
                transport.add_channel_send::<BenchChannel>(settings, id);
            }
            let entity = app
                .world_mut()
                .spawn((
                    Link::default().with_mtu(LinkMtu::new(512)),
                    Linked,
                    transport,
                ))
                .id();
            mark_transport(&mut app, entity, observe);
            entity
        })
        .collect();
    finish(&mut app);
    (app, entities)
}

fn transport_send(options: &Options, links: usize) -> Totals {
    let (mut app, entities) = transport_app(links, options.observe, false);
    let (mut receiver, receiver_entities) = transport_app(links, false, true);
    let payloads = [
        Bytes::from(vec![11; 48]),
        Bytes::from(vec![22; 97]),
        Bytes::from(vec![33; 2048]),
    ];
    let mut totals = Totals::default();
    let mut expected_packets = None;
    for batch in 0..(100 + options.iterations) {
        *app.world_mut().resource_mut::<Observations>() = Observations::default();
        for &entity in &entities {
            for payload in &payloads {
                app.world()
                    .get::<Transport>(entity)
                    .unwrap()
                    .send::<BenchChannel>(payload.clone())
                    .unwrap();
            }
        }
        let timed = batch >= 100;
        totals.schedule_elapsed_ns += measure(&mut app, false, timed, options.count_allocations);
        let mut packets = 0;
        let mut bytes = 0;
        for (&entity, &target) in entities.iter().zip(&receiver_entities) {
            let mut link = app.world_mut().get_mut::<Link>(entity).unwrap();
            let mut target_link = receiver.world_mut().get_mut::<Link>(target).unwrap();
            for packet in link.send.drain() {
                packets += 1;
                bytes += packet.len() as u64;
                target_link.recv.push_raw(BytesMut::from(packet.as_ref()));
            }
        }
        assert!(
            packets >= (links * 4) as u64,
            "fragmentation must be exercised"
        );
        assert_eq!(
            *expected_packets.get_or_insert(packets),
            packets,
            "work per batch changed"
        );
        receiver.world_mut().run_schedule(PreUpdate);
        receiver.world_mut().flush();
        let mut checksum = 0;
        let mut messages = 0;
        for &entity in &receiver_entities {
            let mut transport = receiver.world_mut().get_mut::<Transport>(entity).unwrap();
            let mut seen = [0; 3];
            for channel in transport.channel_receives_mut() {
                while let Some((_, bytes, _)) = channel.read_message() {
                    let index = payloads
                        .iter()
                        .position(|expected| *expected == bytes)
                        .expect("packetized message changed");
                    seen[index] += 1;
                    checksum += bytes.iter().map(|&x| x as u64).sum::<u64>();
                    messages += 1;
                }
            }
            assert_eq!(
                seen,
                [1, 1, 1],
                "all three messages must round-trip exactly once"
            );
        }
        let observed = verify_observations(&app, options.observe, packets, bytes);
        if timed {
            totals.packets += packets;
            totals.bytes += bytes;
            totals.messages += messages;
            totals.checksum += checksum;
            totals.observation_packets += observed;
        }
    }
    totals
}

fn empty(options: &Options, transport: bool) -> Totals {
    let mut app = if transport {
        transport_app(0, options.observe, false).0
    } else {
        let mut app = base_app();
        app.add_plugins((UdpPlugin, UdpEndpointPlugin));
        app.add_systems(PostUpdate, downstream_send_stage.after(LinkSystems::Send));
        udp_observer(&mut app, options.observe);
        finish(&mut app);
        app
    };
    let mut totals = Totals::default();
    for batch in 0..(100 + options.iterations) {
        totals.schedule_elapsed_ns +=
            measure(&mut app, false, batch >= 100, options.count_allocations);
    }
    assert_eq!(app.world().resource::<Observations>().packets, 0);
    totals
}

fn main() {
    let options = options();
    let totals = match options.scenario.as_str() {
        "udp-single" => udp_send(&options, 1),
        "udp-endpoint" => udp_send(&options, 16),
        "udp-receive" => udp_receive(&options),
        "transport-single" => transport_send(&options, 1),
        "transport-many" => transport_send(&options, 64),
        "empty-udp" => empty(&options, false),
        "empty-transport" => empty(&options, true),
        _ => panic!("unknown scenario"),
    };
    println!(
        "{{\"harness_version\":2,\"scenario\":\"{}\",\"iterations\":{},\"warmup_batches\":100,\"observe\":{},\"allocation_counting\":{},\"udp_observation_api\":{},\"transport_observation_api\":{},\"schedule_elapsed_ns\":{},\"receive_schedule_calls\":{},\"extra_receive_schedule_calls\":{},\"packets\":{},\"bytes\":{},\"messages\":{},\"checksum\":{},\"observation_packets\":{},\"allocations\":{},\"allocated_bytes\":{},\"reallocations\":{},\"reallocated_bytes\":{},\"deallocations\":{},\"deallocated_bytes\":{}}}",
        options.scenario,
        options.iterations,
        options.observe,
        options.count_allocations,
        cfg!(feature = "udp_observation"),
        cfg!(feature = "transport_observation"),
        totals.schedule_elapsed_ns,
        totals.receive_schedule_calls,
        totals
            .receive_schedule_calls
            .saturating_sub(options.iterations as u64),
        totals.packets,
        totals.bytes,
        totals.messages,
        totals.checksum,
        totals.observation_packets,
        ALLOCATIONS.load(Relaxed),
        ALLOCATED_BYTES.load(Relaxed),
        REALLOCATIONS.load(Relaxed),
        REALLOCATED_BYTES.load(Relaxed),
        DEALLOCATIONS.load(Relaxed),
        DEALLOCATED_BYTES.load(Relaxed)
    );
}
