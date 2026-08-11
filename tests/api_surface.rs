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
            .mount(mount_config!(
                "tests/assets/sample.toml",
                "/etc/zebrad/zebrad.toml"
            ))
            .mount(mount_file!("tests/assets/blob.bin", "/seed.bin"))
            .mount(mount_archive!("tests/assets/archive.tar.zst", "/data"))
            .resources("500m", "512Mi")
            .expose("extra", 18080),
    );
    let _w = t.add_validator(Validator::zcashd("6.4.1").named("bob"));
    let _i = t.add_indexer(Indexer::zaino("0.4.0"));
    #[cfg(feature = "zingo")]
    let _z = t.add_wallet(Wallet::zingo());
}

#[test]
fn mount_macros_emit_expected_variants() {
    let m = mount_config!("tests/assets/sample.toml", "/etc/x");
    assert_eq!(m.kind, MountKind::Config);
    assert_eq!(m.destination, PathBuf::from("/etc/x"));
    assert!(matches!(m.source, MountSource::ConfigAbs(ref p) if p.is_absolute()));

    // Both seed macros emit the same `Seed(handle)` source — one identity for
    // an LFS artifact — and differ only in the `kind` that decides whether the
    // puller extracts the tar or copies the blob.
    let m = mount_file!("tests/assets/blob.bin", "/blob");
    assert_eq!(m.kind, MountKind::File);
    assert!(matches!(m.source, MountSource::Seed(h) if h.name() == "blob.bin"));

    let m = mount_archive!("tests/assets/archive.tar.zst", "/data");
    assert_eq!(m.kind, MountKind::DirArchive);
    assert!(matches!(m.source, MountSource::Seed(h) if h.size() == 19));
}

ztest::archive!(MATURED_CHAIN = "tests/assets/archive.tar.zst");

/// `archive!` bakes the identity out of the sidecar manifest, and the handle
/// flows into `restore` (directly or, in real suites, passed into a helper as a
/// value). `#[ztest::needs]` is what makes it provisionable and binds it to this
/// test.
#[ztest::needs(MATURED_CHAIN)]
#[test]
fn an_archive_handle_carries_its_manifest_identity() {
    assert_eq!(MATURED_CHAIN.name(), "archive.tar.zst");
    assert_eq!(
        MATURED_CHAIN.oid(),
        "75cf3ec7023cf430a92326ba02554dfc1c7b5abc605e6d11bcb18470c8156783"
    );
    assert_eq!(MATURED_CHAIN.size(), 19);
    let _restored = Validator::zebrad("1.9.1").restore(MATURED_CHAIN);
}

/// A shipped snapshot is the same `ArchiveHandle` type as a locally declared
/// archive — the property that lets one `restore` serve both.
#[test]
fn a_shipped_snapshot_is_the_same_handle_type() {
    let _restored = Validator::zebrad("6.2.3").restore(ORCHARD);
}

#[test]
fn endpoint_url_format() {
    let e = Endpoint {
        host: "127.0.0.1".parse().unwrap(),
        port: 38291,
    };
    assert_eq!(e.url("http"), "http://127.0.0.1:38291");
}
