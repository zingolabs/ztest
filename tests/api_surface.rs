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

    let m = mount_file!("tests/assets/blob.bin", "/blob");
    assert_eq!(m.kind, MountKind::File);
    assert!(matches!(m.source, MountSource::FileAbs(_)));

    let m = mount_archive!("tests/assets/archive.tar.zst", "/data");
    assert_eq!(m.kind, MountKind::DirArchive);
    assert!(matches!(m.source, MountSource::ArchiveAbs(_)));
}

/// The per-test `#[ztest::archive]` binds a fn-local typed handle carrying the
/// resolved absolute source path, and that handle flows into `with_regtest_cache`
/// (directly or, in real suites, passed into a helper as a value).
#[ztest::archive(MATURED_CHAIN = "tests/assets/archive.tar.zst")]
#[test]
fn archive_attr_binds_a_typed_handle() {
    let src = MATURED_CHAIN.source();
    assert!(
        std::path::Path::new(src).is_absolute(),
        "handle source must be absolute: {src}"
    );
    assert!(
        src.ends_with("tests/assets/archive.tar.zst"),
        "handle source must resolve the declared path: {src}"
    );
    // The handle is an `ArchiveHandle` and is accepted by `with_regtest_cache`.
    let _cached = Validator::zebrad("1.9.1").with_regtest_cache(MATURED_CHAIN);
}

#[test]
fn endpoint_url_format() {
    let e = Endpoint {
        host: "127.0.0.1".parse().unwrap(),
        port: 38291,
    };
    assert_eq!(e.url("http"), "http://127.0.0.1:38291");
}
