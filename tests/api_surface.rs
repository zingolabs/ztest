//! Compile-shape tests for the public API. These exercise the builder
//! call-chains and the three mount macros so that any change to the
//! type surface fails fast.

use std::path::PathBuf;
use ztest::prelude::*;

#[test]
fn builder_chain_compiles_for_every_variant() {
    let mut t = TestEnv::builder();
    let _v = t.add_validator(
        Validator::zebrad("1.9.1")
            .named("alice")
            .mount(mount_config!("tests/assets/sample.toml", "/etc/zebrad/zebrad.toml"))
            .mount(mount_file!("tests/assets/blob.bin", "/seed.bin"))
            .mount(mount_archive!("tests/assets/archive.tar.zst", "/data"))
            .resources(Cpu::millis(500), Mem::mib(512))
            .expose("extra", 18080),
    );
    let _w = t.add_validator(Validator::zcashd("6.4.1").named("bob"));
    let _i = t.add_indexer(Indexer::zaino("0.4.0"));
}

#[test]
fn mount_macros_emit_expected_variants() {
    let m = mount_config!("tests/assets/sample.toml", "/etc/x");
    assert_eq!(m.kind, MountKind::Config);
    assert_eq!(m.destination, PathBuf::from("/etc/x"));
    assert!(matches!(m.source, MountSource::ConfigAbs(ref p) if p.is_absolute()));

    // Both seed macros emit the same `Seed(handle)` source — one identity for
    // a bucket artifact — and differ only in the `kind` that decides whether the
    // puller extracts the tar or copies the blob.
    let m = mount_file!("tests/assets/blob.bin", "/blob");
    assert_eq!(m.kind, MountKind::File);
    assert!(matches!(m.source, MountSource::Seed(h) if h.name == "blob.bin"));

    let m = mount_archive!("tests/assets/archive.tar.zst", "/data");
    assert_eq!(m.kind, MountKind::DirArchive);
    assert!(matches!(m.source, MountSource::Seed(h) if h.size == 19));
}

/// `artifact!` bakes identity out of a manifest at compile time, and a `ChainSnapshot`
/// declaration carries the chain facts beside it. `#[ztest::needs]` is what makes the
/// artifact provisionable and binds it to this test.
#[ztest::needs(ORCHARD_TESTNET)]
#[test]
fn a_snapshot_carries_its_manifest_identity_and_its_pin() {
    assert_eq!(ORCHARD_TESTNET.artifact.name, "zebra-v6.2.3-testnet-1848420.tar.zst");
    assert_eq!(ORCHARD_TESTNET.artifact.oid.len(), 64);
    assert_eq!(ORCHARD_TESTNET.tip_height, 1_848_420);
    assert_eq!(ORCHARD_TESTNET.network, Network::Testnet);
    let _validator = Validator::zebrad("6.2.3").snapshot(ORCHARD_TESTNET);
}

#[test]
fn endpoint_url_format() {
    let e = Endpoint { host: "127.0.0.1".parse().unwrap(), port: 38291 };
    assert_eq!(e.url("http"), "http://127.0.0.1:38291");
}
