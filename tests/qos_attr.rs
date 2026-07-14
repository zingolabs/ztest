//! The `#[ztest::qos::*]` tier attributes register a `QosDecl` in the
//! link-time inventory with the right class.
//!
//! This is an integration test (a downstream crate) on purpose: the macro
//! expands to `::ztest::…` paths, which resolve from a crate that depends on
//! `ztest` but not from inside the `ztest` lib itself.

use ztest::qos::QosClass;

// Each marker is a real test (so it isn't dead code); the attribute injects a
// harmless `__enter` and submits a `QosDecl` to the inventory at link time.
#[ztest::qos::basic]
#[test]
fn marker_basic() {}

#[ztest::qos::wallet]
#[test]
fn marker_wallet() {}

#[ztest::qos::integration]
#[test]
fn marker_integration() {}

#[ztest::qos::testnet]
#[test]
fn marker_testnet() {}

#[ztest::qos::sync]
#[test]
fn marker_sync() {}

#[test]
fn tier_attributes_register_in_inventory_with_the_right_class() {
    let entries: Vec<_> = ztest::inventory::qos_iter().collect();

    let class_of = |suffix: &str| {
        entries
            .iter()
            .find(|d| d.test_id.ends_with(suffix))
            .unwrap_or_else(|| panic!("no QosDecl for `{suffix}` in {entries:?}"))
            .class
    };

    assert_eq!(class_of("::marker_basic"), QosClass::Basic);
    assert_eq!(class_of("::marker_wallet"), QosClass::Wallet);
    assert_eq!(class_of("::marker_integration"), QosClass::Integration);
    assert_eq!(class_of("::marker_testnet"), QosClass::Testnet);
    assert_eq!(class_of("::marker_sync"), QosClass::Sync);

    // test_id is module-qualified (the §3 `module_path!()::fn` shape).
    let sync = entries
        .iter()
        .find(|d| d.test_id.ends_with("::marker_sync"))
        .unwrap();
    assert!(
        sync.test_id.contains("qos_attr"),
        "module-qualified: {}",
        sync.test_id
    );
}
