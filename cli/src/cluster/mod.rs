//! `ztest cluster` — named cluster profiles + the ztest-owned resources in them.
//!
//! - Profile = kube-context + cluster class + image distribution under one name
//! - `ztest run --cluster <name>` / persisted default selects all three at once
//! - Store + selection precedence: [`ztest::api::cluster_config`]

use std::process::ExitCode;

use clap::{Args as ClapArgs, Subcommand};

use ztest::api::cluster_config::{self, Config, Profile};

mod csi_hostpath;
mod setup;

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Provision the namespaced resources ztest owns. Idempotent. The cluster
    /// itself is an operator's to provide — `ztest cluster check` reports
    /// whether one is usable, and docs/ops-cluster-requirements.md says how.
    Setup(setup::Args),

    /// Report whether a cluster provides what ztest needs, and what is lost if
    /// it doesn't. Read-only: nothing is created, changed, or installed.
    Check {
        /// Profile to check. Omitted, the persisted default is used, else the
        /// ambient kube-context.
        #[arg(long, value_name = "NAME")]
        cluster: Option<String>,
    },

    /// List cluster profiles, marking the active default.
    List,

    /// Print the active default profile.
    Current,

    /// Add or update a profile.
    Add(AddArgs),

    /// Make a profile the active default (used when `ztest run` gets no
    /// `--cluster`).
    Set {
        /// Profile name.
        name: String,
    },

    /// Remove a profile. Clears the default if it pointed here.
    Remove {
        /// Profile name.
        name: String,
    },
}

/// A profile has one of two sources: a local kind cluster (`--kind`, addressed
/// by name) or a kubeconfig-described remote (`--kubeconfig`, whose context and
/// registry config come from the file itself: its
/// current-context and its `ztest.io/registry` extension).
#[derive(Debug, ClapArgs)]
struct AddArgs {
    /// Profile name.
    name: String,

    /// Local kind cluster, addressed by name: images are `kind load`ed into
    /// `<cluster>-control-plane` and the context is derived as `kind-<cluster>`.
    /// The cluster name defaults to the profile name; pass a value to override
    /// (`--kind zkn`).
    #[arg(long, value_name = "CLUSTER", num_args = 0..=1, conflicts_with = "kubeconfig")]
    kind: Option<Option<String>>,

    /// Kubeconfig describing a remote cluster. Sets `KUBECONFIG` for the run; the
    /// context is the file's current-context and any `ztest.io/registry`
    /// extension supplies the registry config (the "ship one file" path).
    #[arg(long, value_name = "PATH")]
    kubeconfig: Option<String>,

    /// CSI driver seeding uses, e.g. `topolvm.io`. Omitted, ztest follows the
    /// cluster's default StorageClass. ztest never installs a driver — this
    /// selects among what the cluster already provides.
    #[arg(long, value_name = "DRIVER")]
    storage_driver: Option<String>,

    /// Also make this the active default.
    #[arg(long)]
    set_default: bool,
}

pub fn execute(args: Args) -> ExitCode {
    // `setup`/`check`/`add` reach a cluster (own runtimes); the rest edit a local file
    let result = match args.cmd {
        Cmd::Setup(a) => return setup::execute(a),
        Cmd::Check { cluster } => {
            return super::block_on("cluster check", super::Rt::Current, check(cluster));
        }
        Cmd::List => list(),
        Cmd::Current => current(),
        Cmd::Add(a) => {
            return super::block_on("cluster add", super::Rt::Current, add(a));
        }
        Cmd::Set { name } => set(name),
        Cmd::Remove { name } => remove(name),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ztest cluster: {e}");
            ExitCode::FAILURE
        }
    }
}

fn list() -> Result<(), String> {
    let cfg = cluster_config::load()?;
    if cfg.clusters.is_empty() {
        println!("no cluster profiles. Add one:\n  ztest cluster add zkn --kind");
        return Ok(());
    }
    for (name, profile) in &cfg.clusters {
        let marker = if cfg.current.as_deref() == Some(name.as_str()) { "*" } else { " " };
        println!("{marker} {name}  ({})", profile.summary());
    }
    Ok(())
}

fn current() -> Result<(), String> {
    let cfg = cluster_config::load()?;
    match cfg.current.as_deref() {
        Some(name) => {
            let summary = cfg
                .clusters
                .get(name)
                .map(Profile::summary)
                .unwrap_or_else(|| "<dangling: profile removed>".to_string());
            println!("{name}  ({summary})");
            Ok(())
        }
        None => {
            println!("no default cluster set (runs follow the ambient kube-context / env)");
            Ok(())
        }
    }
}

async fn add(a: AddArgs) -> Result<(), String> {
    // Bare `--kind` adopts the profile name, `--kind X` overrides.
    // Distribution never typed (a remote profile derives wholly from its kubeconfig).
    let mut profile = match (a.kind, &a.kubeconfig) {
        (Some(k), _) => Profile::local(&k.unwrap_or_else(|| a.name.clone())),
        (None, Some(kc)) => Profile::from_kubeconfig(std::path::Path::new(kc))?,
        (None, None) => return Err("pass --kind or --kubeconfig".to_string()),
    };
    let named_driver = a.storage_driver.is_some();
    profile.storage_driver = a.storage_driver;
    profile.validate()?;

    let mut cfg = cluster_config::load()?;
    let first = cfg.clusters.is_empty();
    let existed = cfg.clusters.insert(a.name.clone(), profile.clone()).is_some();
    // First profile defaults without `--set-default` (no ambiguity at one profile)
    if a.set_default || first {
        cfg.current = Some(a.name.clone());
    }
    cfg.save()?;

    let verb = if existed { "updated" } else { "added" };
    println!("{verb} `{}`  ({})", a.name, profile.summary());
    if cfg.current.as_deref() == Some(a.name.as_str()) {
        println!("`{}` is now the default", a.name);
    }
    if !named_driver {
        adopt_storage_driver(&a.name).await;
    }
    Ok(())
}

/// Record the cluster's snapshot-capable driver when the profile named none.
///
/// - Unnamed = follow the default StorageClass, which on a stock `kind create cluster` cannot
///   snapshot → seeding silently degrades, and only a fixture-mounting run finds out
/// - Only on exactly one candidate (several = a choice ztest must not make silently)
/// - Never fatal: the profile is already saved, and `ztest cluster check` is the gate
async fn adopt_storage_driver(name: &str) {
    // SAFETY: pre-spawn, as in `check` (applies the profile via non-thread-safe env set)
    if unsafe { cluster_config::activate(Some(name)) }.is_err() {
        return;
    }
    let Ok(client) = ztest::api::cluster::client().await else {
        println!("  storage driver unset: cluster unreachable — run `ztest cluster check`");
        return;
    };
    let drivers: Vec<String> = match ztest::api::storage_class::discover(&client).await {
        Ok(options) => {
            let mut d: Vec<String> = options.into_iter().map(|o| o.provisioner).collect();
            d.sort();
            d.dedup();
            d
        }
        Err(_) => Vec::new(),
    };
    match storage_choice(&drivers) {
        Ok(only) => match persist_storage_driver(name, only) {
            Ok(()) => println!("  storage driver: {only} (probed)"),
            Err(e) => println!("  storage driver {only} found but not saved: {e}"),
        },
        Err(why) => println!("  storage driver unset: {why}"),
    }
}

/// Sole snapshot-capable driver, or why there is no unambiguous one
fn storage_choice(drivers: &[String]) -> Result<&str, String> {
    match drivers {
        [only] => Ok(only),
        [] => Err("no snapshot-capable storage found — chain fixtures will fail \
                   (docs/ops-local-cluster.md)"
            .to_string()),
        many => Err(format!(
            "{} candidates ({}) — pick one with --storage-driver",
            many.len(),
            many.join(", "),
        )),
    }
}

fn persist_storage_driver(name: &str, driver: &str) -> Result<(), String> {
    let mut cfg = cluster_config::load()?;
    let profile = cfg.clusters.get_mut(name).ok_or("profile vanished between write and probe")?;
    profile.storage_driver = Some(driver.to_string());
    cfg.save()
}

fn set(name: String) -> Result<(), String> {
    let mut cfg = cluster_config::load()?;
    if !cfg.clusters.contains_key(&name) {
        return Err(format!("no profile `{name}`. Known: {}", known(&cfg)));
    }
    cfg.current = Some(name.clone());
    cfg.save()?;
    println!("default is now `{name}`");
    Ok(())
}

fn remove(name: String) -> Result<(), String> {
    let mut cfg = cluster_config::load()?;
    if cfg.clusters.remove(&name).is_none() {
        return Err(format!("no profile `{name}`. Known: {}", known(&cfg)));
    }
    if cfg.current.as_deref() == Some(name.as_str()) {
        cfg.current = None;
    }
    cfg.save()?;
    println!("removed `{name}`");
    Ok(())
}

fn known(cfg: &Config) -> String {
    let names: Vec<&str> = cfg.clusters.keys().map(String::as_str).collect();
    if names.is_empty() { "(none)".to_string() } else { names.join(", ") }
}

/// Probe the cluster, print what it can and cannot do
/// (non-zero only on a missing *required* capability, else unusable as a CI gate)
async fn check(cluster: Option<String>) -> Result<(), String> {
    // SAFETY: pre-spawn, as in `setup` (applies the profile via non-thread-safe env set)
    let bound = unsafe { cluster_config::activate(cluster.as_deref()) }?;
    let client =
        ztest::api::cluster::client().await.map_err(|e| format!("connecting to cluster: {e}"))?;

    let report = ztest::api::capability::probe(&client).await;
    print!("{}", render(&report, bound.as_deref()));

    match report.is_runnable() {
        true => Ok(()),
        false => Err(format!(
            "{} required capability/ies missing — `ztest run` cannot work here",
            report.blocking().count()
        )),
    }
}

/// One line per capability, remedy only where actionable
fn render(report: &ztest::api::capability::Report, bound: Option<&str>) -> String {
    use owo_colors::OwoColorize as _;
    use std::fmt::Write as _;
    use ztest::api::capability::{Finding, Need};

    let theme = ztest_ui::Theme::detect();
    let mut out = String::new();

    let context = cluster_config::active_context().unwrap_or_else(|| "(ambient)".into());
    let class = ztest::backends::image::selected_class().label();
    let _ = writeln!(
        out,
        "cluster  {}  {}  (context: {context})",
        bound.unwrap_or("-").style(theme.styles.count),
        class.style(theme.styles.dim),
    );

    for cap in &report.capabilities {
        // Missing optional = warning (costs a feature, not the run)
        let (mark, style) = match (&cap.finding, cap.need) {
            (Finding::Present(_), _) => (theme.chars.ok, theme.styles.pass),
            (_, Need::Required) => (theme.chars.fail, theme.styles.fail),
            (_, Need::Enables(_)) => (theme.chars.warn, theme.styles.skip),
        };
        let _ = writeln!(
            out,
            "  {} {:<26}  {}",
            mark.style(style),
            cap.name,
            cap.finding.detail().style(theme.styles.dim),
        );
        if !cap.finding.is_present() {
            let lost = match cap.need {
                Need::Required => "ztest run".to_string(),
                Need::Enables(f) => f.to_string(),
            };
            let _ = writeln!(
                out,
                "      {}",
                format!("→ {lost} unavailable; see {}", cap.remedy).style(theme.styles.dim),
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drivers(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_sole_driver_is_adopted() {
        assert_eq!(storage_choice(&drivers(&["topolvm.io"])), Ok("topolvm.io"));
    }

    /// Picking for the user here would bind every seeded run to a driver they never named
    #[test]
    fn several_drivers_are_left_to_the_user() {
        let why = storage_choice(&drivers(&["hostpath.csi.k8s.io", "topolvm.io"]))
            .expect_err("two candidates cannot be resolved");
        assert!(why.contains("--storage-driver"), "{why}");
        assert!(why.contains("topolvm.io"), "the candidates must be named: {why}");
    }

    /// Stock `kind create cluster` — the case the quickstart walks into
    #[test]
    fn no_snapshot_capable_driver_says_fixtures_will_fail() {
        let why = storage_choice(&[]).expect_err("nothing to choose");
        assert!(why.contains("chain fixtures"), "{why}");
    }
}
