//! Pre-run summary of the selected tests' reserves, for the preflight banner.
//!
//! - Pure `(per-test reserves, capacity) -> plan`: count, total reserve, and the tests whose
//!   reserve exceeds the cluster outright (admission rejects them → fail-fast)
//! - Per *test*: a `footprint = ".."` override makes two tests of one tag differ
//! - No order or concurrency estimate: admission is one arrival-ordered queue popped as
//!   capacity frees ([`super::scheduler`], one letter apart)

use super::Resources;

/// One selected test + the `admitted` reserve it is submitted with
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTest {
    pub name: String,
    pub admitted: Resources,
}

/// Selected tests against probed capacity.
///
/// - `total` = Σ every test's reserve
/// - `free` `None` = probe unavailable → nothing judged unschedulable
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QosPlan {
    pub tests: u32,
    pub total: Resources,
    pub free: Option<Resources>,
    pub unschedulable: Vec<PlannedTest>,
}

pub fn plan(tests: &[PlannedTest], free: Option<Resources>) -> QosPlan {
    let total = tests.iter().fold(Resources::ZERO, |acc, t| acc.saturating_add(&t.admitted));
    let unschedulable = match free {
        Some(free) => tests.iter().filter(|t| !t.admitted.fits_within(&free)).cloned().collect(),
        None => Vec::new(),
    };
    QosPlan { tests: tests.len() as u32, total, free, unschedulable }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::qos::{GIB, QosClass};

    fn test(name: &str, admitted: Resources) -> PlannedTest {
        PlannedTest { name: name.into(), admitted }
    }

    #[test]
    fn total_sums_every_tests_own_reserve() {
        let base = QosClass::Integration.profile();
        let raised = base.with_footprint(Some(Resources::new(4_000, 8 * GIB, 0, 0)));
        let p = plan(&[test("a", base.admitted()), test("b", raised.admitted())], None);
        assert_eq!(p.tests, 2);
        assert_eq!(p.total, base.admitted().saturating_add(&raised.admitted()));
    }

    #[test]
    fn a_test_over_the_cluster_is_named_with_its_reserve() {
        let huge = Resources::new(16_000, 16 * GIB, 0, 0);
        let p = plan(
            &[test("small", Resources::new(3_000, 5 * GIB, 0, 0)), test("zaino_sync", huge)],
            Some(Resources::new(4_000, 8 * GIB, 0, 0)),
        );
        assert_eq!(p.unschedulable, [test("zaino_sync", huge)]);
    }

    #[test]
    fn no_capacity_judges_nothing_unschedulable() {
        let p = plan(&[test("t", Resources::new(64_000, 512 * GIB, 0, 0))], None);
        assert!(p.unschedulable.is_empty());
        assert_eq!(p.tests, 1);
    }

    #[test]
    fn empty_input_is_an_empty_plan() {
        let p = plan(&[], Some(Resources::new(8_000, 16 * GIB, 0, 0)));
        assert_eq!((p.tests, p.total), (0, Resources::ZERO));
    }
}
