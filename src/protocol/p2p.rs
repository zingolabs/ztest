//! Zcash P2P peer discovery, for a validator that must *join* its network.
//!
//! - ZIP-204 §Peer Discovery: DNS seeds → `addr` relay. A node past an upgrade MUST disconnect
//!   peers below that epoch's protocol version (§Network Upgrade Epoch Enforcement)
//! - Bootstrap trap: the seeders answer mostly with un-upgraded nodes (measured 2026-09-15: 0-2 of
//!   45 seed addresses spoke NU6.3, varying by the hour)
//! - So the seed set is filtered here — handshake each candidate, keep the ones at the epoch's
//!   version — then widened from the address books of *any* peer that answered (an un-upgraded
//!   node still relays addresses of upgraded ones)
//! - Survivors are handed to the node as its initial peers; its own crawler takes over from there
//! - Enough of the protocol to ask two questions ("what version" / "which peers"), no further

use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Network;

/// ZIP-204 §DNS Seeds. `mainnet.is.yolo.money` is listed there but has NXDOMAIN'd since at least
/// 2026-09; a seed that does not resolve costs one lookup
const MAINNET_SEEDS: &[&str] = &[
    "dnsseed.z.cash",
    "dnsseed.str4d.xyz",
    "mainnet.seeder.zfnd.org",
    "mainnet.seeder.shieldedinfra.net",
    "mainnet.is.yolo.money",
];

const TESTNET_SEEDS: &[&str] =
    &["dnsseed.testnet.z.cash", "testnet.seeder.zfnd.org", "testnet.is.yolo.money"];

/// ZIP-204 §Network Upgrade Epoch Enforcement, mainnet column, newest first
const MAINNET_EPOCHS: &[(u32, u32)] = &[(3_428_143, 170_160), (3_364_600, 170_150)];

/// Same table, testnet column
const TESTNET_EPOCHS: &[(u32, u32)] = &[(4_134_000, 170_160), (4_052_000, 170_150)];

/// Peer's `version` message must arrive inside this, or it is no use to a node whose own
/// handshake timeout is 3 s
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(6);

/// Candidates dialled together. The seed set is tens of addresses and most never answer
const DIAL_CONCURRENCY: usize = 32;

/// Gossiped addresses probed in all. Most are unreachable and most of the rest are un-upgraded, so
/// this is the budget that decides whether discovery finds a handful or one
const GOSSIP_DIAL_CAP: usize = 384;

/// `getaddr` is answered when the peer feels like it, and never on some implementations
const GOSSIP_TIMEOUT: Duration = Duration::from_secs(10);

/// Address books asked for before giving up. Each is one dial plus a probe round
const GOSSIP_SOURCES: usize = 6;

/// Protocol version a node at `height` requires of its peers.
///
/// - Below every listed upgrade → 0, i.e. no floor this table can state
pub fn epoch_min_version(network: Network, height: u32) -> u32 {
    let table = match network {
        Network::Mainnet => MAINNET_EPOCHS,
        Network::Testnet | Network::Regtest => TESTNET_EPOCHS,
    };
    table.iter().find(|(activation, _)| height >= *activation).map_or(0, |(_, version)| *version)
}

/// Peers that speak at least `min_version`: the seed set, then what those peers gossip.
///
/// - Both ZIP-204 discovery mechanisms, in its order — seeds bootstrap, `addr` relay widens. The
///   seed set alone yields 1-2 upgraded nodes, too thin to start a multi-day run on
/// - Gossip is filtered the same way: an upgraded peer's address book still holds old nodes
/// - Empty = this network has no reachable peer a node at that epoch may talk to; the caller
///   reports it rather than leaving the node to crawl forever
pub async fn compatible_peers(
    network: Network,
    min_version: u32,
    want: usize,
) -> Result<Vec<SocketAddr>, PeerDiscoveryError> {
    let seeds = seed_addresses(network).await;
    if seeds.is_empty() {
        return Err(PeerDiscoveryError::NoSeedAddresses { seeds: seeds_of(network).len() });
    }
    let probed = probe_all(network, &seeds).await;
    let mut compatible: Vec<SocketAddr> =
        probed.iter().filter(|(_, v)| *v >= min_version).map(|(addr, _)| *addr).collect();

    // Gossip from *any* peer that answered, not just a compatible one: an un-upgraded node relays
    // addresses of the whole network, and on some days the seed set holds no upgraded node at all
    if compatible.len() < want {
        let sources = probed.iter().map(|(addr, _)| *addr).take(GOSSIP_SOURCES);
        let books = futures::future::join_all(sources.map(|a| address_book(network, a))).await;

        let mut seen: Vec<SocketAddr> = probed.iter().map(|(addr, _)| *addr).collect();
        let mut candidates = Vec::new();
        for addr in books.into_iter().flatten().flatten() {
            if !seen.contains(&addr) {
                seen.push(addr);
                candidates.push(addr);
            }
        }
        candidates.truncate(GOSSIP_DIAL_CAP);

        for chunk in candidates.chunks(DIAL_CONCURRENCY) {
            if compatible.len() >= want {
                break;
            }
            let found = probe_all(network, chunk).await;
            compatible.extend(found.iter().filter(|(_, v)| *v >= min_version).map(|(a, _)| *a));
        }
    }
    compatible.truncate(want);

    if compatible.is_empty() {
        return Err(PeerDiscoveryError::NoCompatiblePeers {
            min_version,
            dialled: seeds.len(),
            answered: probed.len(),
            versions: {
                let mut seen: Vec<u32> = probed.iter().map(|(_, v)| *v).collect();
                seen.sort_unstable();
                seen.dedup();
                seen
            },
        });
    }
    Ok(compatible)
}

#[derive(Debug, thiserror::Error)]
pub enum PeerDiscoveryError {
    #[error("none of the {seeds} DNS seed(s) resolved to a peer address")]
    NoSeedAddresses { seeds: usize },

    #[error(
        "no seed peer speaks protocol version {min_version}: dialled {dialled}, {answered} \
         answered, versions seen {versions:?}. The network has not upgraded far enough for a node \
         at this height to peer with it"
    )]
    NoCompatiblePeers { min_version: u32, dialled: usize, answered: usize, versions: Vec<u32> },
}

fn seeds_of(network: Network) -> &'static [&'static str] {
    match network {
        Network::Mainnet => MAINNET_SEEDS,
        Network::Testnet | Network::Regtest => TESTNET_SEEDS,
    }
}

/// Every seed's A/AAAA records, deduped. A seed that does not resolve is skipped, not an error
async fn seed_addresses(network: Network) -> Vec<SocketAddr> {
    let port = default_port(network);
    let mut out = Vec::new();
    for seed in seeds_of(network) {
        let Ok(resolved) = tokio::net::lookup_host((*seed, port)).await else {
            continue;
        };
        out.extend(resolved);
    }
    out.sort_unstable();
    out.dedup();
    out
}

const fn default_port(network: Network) -> u16 {
    match network {
        Network::Mainnet => 8233,
        Network::Testnet | Network::Regtest => 18233,
    }
}

/// `(addr, advertised version)` for every candidate that completed a `version` exchange
async fn probe_all(network: Network, candidates: &[SocketAddr]) -> Vec<(SocketAddr, u32)> {
    let mut answered = Vec::new();
    for chunk in candidates.chunks(DIAL_CONCURRENCY) {
        let dials = chunk
            .iter()
            .map(|addr| async move { (*addr, advertised_version(network, *addr).await.ok()) });
        for (addr, version) in futures::future::join_all(dials).await {
            if let Some(version) = version {
                answered.push((addr, version));
            }
        }
    }
    answered
}

/// One `version` exchange: send ours, read theirs, drop the connection.
///
/// - Peer's own `version` is its first message (ZIP-204 §Version Message), so nothing else is read
async fn advertised_version(network: Network, addr: SocketAddr) -> Result<u32, std::io::Error> {
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(&frame(network, "version", &version_payload(network))).await?;

        let mut header = [0u8; HEADER_LEN];
        stream.read_exact(&mut header).await?;
        let command = String::from_utf8_lossy(&header[4..16]).trim_end_matches('\0').to_string();
        let length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]) as usize;
        if command != "version" || !(4..=MAX_PAYLOAD).contains(&length) {
            return Err(std::io::Error::other(format!("first message was {command} ({length} B)")));
        }
        let mut payload = vec![0u8; length];
        stream.read_exact(&mut payload).await?;
        Ok(u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]))
    };
    match tokio::time::timeout(HANDSHAKE_TIMEOUT, exchange).await {
        Ok(result) => result,
        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
    }
}

/// Peer's own address book, via `getaddr` (ZIP-204 §Peer Discovery, address relay).
///
/// - Full handshake first: `version` both ways, then `verack` both ways, else `getaddr` is ignored
/// - `addr` only — `addrv2` is sent solely to a peer that asked with `sendaddrv2`, which this does
///   not, so the answer is the v1 form
async fn address_book(
    network: Network,
    addr: SocketAddr,
) -> Result<Vec<SocketAddr>, std::io::Error> {
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(&frame(network, "version", &version_payload(network))).await?;

        let mut sent_getaddr = false;
        let gossiped = loop {
            let (command, payload) = read_message(&mut stream).await?;
            match command.as_str() {
                "version" => {
                    stream.write_all(&frame(network, "verack", &[])).await?;
                }
                "verack" if !sent_getaddr => {
                    stream.write_all(&frame(network, "getaddr", &[])).await?;
                    sent_getaddr = true;
                }
                "addr" => break parse_addr(&payload, default_port(network)),
                // `ping` answered so the peer keeps the connection open long enough to reply
                "ping" => stream.write_all(&frame(network, "pong", &payload)).await?,
                _ => {}
            }
        };
        Ok(gossiped)
    };
    match tokio::time::timeout(GOSSIP_TIMEOUT, exchange).await {
        Ok(result) => result,
        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
    }
}

/// One framed message; payload capped so a hostile length cannot allocate the host away
async fn read_message(stream: &mut TcpStream) -> Result<(String, Vec<u8>), std::io::Error> {
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let command = String::from_utf8_lossy(&header[4..16]).trim_end_matches('\0').to_string();
    let length = u32::from_le_bytes([header[16], header[17], header[18], header[19]]) as usize;
    if length > MAX_GOSSIP_PAYLOAD {
        return Err(std::io::Error::other(format!("{command} payload is {length} B")));
    }
    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;
    Ok((command, payload))
}

/// `addr` entries: `CAddress` = time ‖ services ‖ 16-byte IP ‖ big-endian port, after a varint
/// count. IPv4 arrives IPv4-mapped; a zero port means the peer offered no listener
fn parse_addr(payload: &[u8], _default_port: u16) -> Vec<SocketAddr> {
    use std::net::{IpAddr, Ipv6Addr};

    let Some((count, mut rest)) = read_varint(payload) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for _ in 0..count.min(MAX_ADDR_ENTRIES) {
        if rest.len() < ADDR_ENTRY_LEN {
            break;
        }
        let (entry, tail) = rest.split_at(ADDR_ENTRY_LEN);
        rest = tail;
        let ip = Ipv6Addr::from(<[u8; 16]>::try_from(&entry[12..28]).expect("16 bytes"));
        let port = u16::from_be_bytes([entry[28], entry[29]]);
        if port == 0 {
            continue;
        }
        let ip = ip.to_ipv4_mapped().map_or(IpAddr::V6(ip), IpAddr::V4);
        out.push(SocketAddr::new(ip, port));
    }
    out
}

const ADDR_ENTRY_LEN: usize = 30;

/// zcashd sends at most 1,000 per `addr`; anything beyond is a peer being strange
const MAX_ADDR_ENTRIES: u64 = 1_000;

/// 1,000 entries x 30 B plus the count
const MAX_GOSSIP_PAYLOAD: usize = 64 * 1024;

/// Bitcoin-derived `CompactSize`, returning the value and the bytes after it
fn read_varint(bytes: &[u8]) -> Option<(u64, &[u8])> {
    let (first, rest) = bytes.split_first()?;
    let (value, width) = match first {
        0xfd => (u64::from(u16::from_le_bytes(rest.get(..2)?.try_into().ok()?)), 2),
        0xfe => (u64::from(u32::from_le_bytes(rest.get(..4)?.try_into().ok()?)), 4),
        0xff => (u64::from_le_bytes(rest.get(..8)?.try_into().ok()?), 8),
        small => (u64::from(*small), 0),
    };
    Some((value, &rest[width..]))
}

const HEADER_LEN: usize = 24;

/// A `version` payload is ~100 B; the cap only bounds a hostile peer's framing
const MAX_PAYLOAD: usize = 4 * 1024;

/// ZIP-204 §Message Header: magic ‖ command[12] ‖ length ‖ first 4 bytes of SHA256d(payload)
fn frame(network: Network, command: &str, payload: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};

    let checksum = Sha256::digest(Sha256::digest(payload));
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&magic(network));
    let mut name = [0u8; 12];
    name[..command.len()].copy_from_slice(command.as_bytes());
    out.extend_from_slice(&name);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&checksum[..4]);
    out.extend_from_slice(payload);
    out
}

const fn magic(network: Network) -> [u8; 4] {
    match network {
        Network::Mainnet => [0x24, 0xe9, 0x27, 0x64],
        Network::Testnet => [0xfa, 0x1a, 0xf9, 0xbf],
        Network::Regtest => [0xaa, 0xe8, 0x3f, 0x5f],
    }
}

/// ZIP-204 §Version Message. Addresses zeroed and `relay` false: this connection asks one question
fn version_payload(network: Network) -> Vec<u8> {
    let user_agent = b"/ztest-peer-discovery:0.1/";
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;

    let mut out = Vec::with_capacity(128);
    // Ours, not the peer's: a version below the epoch floor would be rejected before it answers
    out.extend_from_slice(&(epoch_min_version(network, u32::MAX) as i32).to_le_bytes());
    out.extend_from_slice(&1u64.to_le_bytes());
    out.extend_from_slice(&now.to_le_bytes());
    out.extend_from_slice(&[0u8; 26]);
    out.extend_from_slice(&[0u8; 26]);
    out.extend_from_slice(&0u64.to_le_bytes());
    out.push(user_agent.len() as u8);
    out.extend_from_slice(user_agent);
    out.extend_from_slice(&0i32.to_le_bytes());
    out.push(0);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_floor_follows_the_upgrade_the_height_sits_in() {
        assert_eq!(epoch_min_version(Network::Mainnet, 3_428_143), 170_160);
        assert_eq!(epoch_min_version(Network::Mainnet, 3_428_142), 170_150);
        assert_eq!(epoch_min_version(Network::Mainnet, 3_484_722), 170_160);
        assert_eq!(epoch_min_version(Network::Testnet, 4_134_000), 170_160);
        // Below every listed upgrade: this table states no floor rather than inventing one
        assert_eq!(epoch_min_version(Network::Mainnet, 1_693_104), 0);
    }

    /// Header shape is what a peer parses before it answers at all
    #[test]
    fn a_framed_message_carries_the_networks_magic_and_a_sha256d_checksum() {
        use sha2::{Digest, Sha256};

        let payload = version_payload(Network::Mainnet);
        let framed = frame(Network::Mainnet, "version", &payload);

        assert_eq!(&framed[..4], &[0x24, 0xe9, 0x27, 0x64]);
        assert_eq!(&framed[4..16], b"version\0\0\0\0\0");
        assert_eq!(
            u32::from_le_bytes(framed[16..20].try_into().expect("4 bytes")),
            payload.len() as u32
        );
        assert_eq!(&framed[20..24], &Sha256::digest(Sha256::digest(&payload))[..4]);
        assert_eq!(&framed[24..], &payload[..]);
    }

    /// IPv4 arrives IPv4-mapped, ports are big-endian, and a zero port is a peer with no listener
    #[test]
    fn an_addr_answer_parses_into_dialable_addresses() {
        let mut payload = vec![3u8]; // CompactSize count
        let entry = |ip: [u8; 4], port: u16| {
            let mut e = vec![0u8; ADDR_ENTRY_LEN];
            e[12..22].copy_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
            e[22..24].copy_from_slice(&[0xff, 0xff]);
            e[24..28].copy_from_slice(&ip);
            e[28..30].copy_from_slice(&port.to_be_bytes());
            e
        };
        payload.extend(entry([38, 190, 136, 76], 8233));
        payload.extend(entry([142, 93, 27, 189], 0));
        payload.extend(entry([1, 2, 3, 4], 18233));

        assert_eq!(
            parse_addr(&payload, 8233),
            vec![
                "38.190.136.76:8233".parse().expect("addr"),
                "1.2.3.4:18233".parse::<SocketAddr>().expect("addr"),
            ],
            "a zero port is unusable, and the rest keep the port the peer gave"
        );
    }

    /// A truncated answer yields what it holds, never a panic on a peer's framing
    #[test]
    fn a_short_addr_answer_stops_at_the_last_whole_entry() {
        let payload = vec![9u8; 1 + ADDR_ENTRY_LEN + 5];
        assert_eq!(parse_addr(&payload, 8233).len(), 1);
        assert!(parse_addr(&[], 8233).is_empty());
    }

    #[test]
    fn compact_size_reads_each_of_its_widths() {
        assert_eq!(read_varint(&[0x05, 0xaa]).expect("varint"), (5, &[0xaa][..]));
        assert_eq!(read_varint(&[0xfd, 0x02, 0x01, 0xaa]).expect("varint"), (258, &[0xaa][..]));
        assert_eq!(
            read_varint(&[0xfe, 0x01, 0x00, 0x00, 0x00, 0xaa]).expect("varint"),
            (1, &[0xaa][..])
        );
        assert!(read_varint(&[0xfd, 0x01]).is_none(), "truncated width is not a value");
    }

    /// Advertising below the floor gets the prober disconnected by the peers it is there to find
    #[test]
    fn the_probe_advertises_the_newest_epoch_it_knows() {
        let payload = version_payload(Network::Mainnet);
        let advertised = i32::from_le_bytes(payload[..4].try_into().expect("4 bytes"));
        assert_eq!(advertised, 170_160);
        assert_eq!(payload[4 + 8 + 8 + 26 + 26 + 8] as usize, "/ztest-peer-discovery:0.1/".len());
    }
}
