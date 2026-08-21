//! `ztest cluster` — named cluster profiles + the ztest-owned resources in them.
//!
//! - Profile = kube-context + cluster class + image distribution under one name
//! - `ztest run --cluster <name>` / persisted default selects all three at once
//! - Store + selection precedence: [`ztest::api::cluster_config`]

use std::process::ExitCode;

use anyhow::{Context as _, Result, anyhow};
use clap::{Args as ClapArgs, Subcommand};

use ztest::api::cluster_config::{self, Config, Profile};
use ztest::api::runtime::{self, ContainerRuntime};
use ztest_ui::Theme;
use ztest_ui::template::{Fields, draw};

mod csi_hostpath;
mod setup;

/// Row shapes for every `ztest cluster` line; glyph + ink resolve through [`Theme`]
mod row {
    /// Mark cells opening a verdict row. Glyph and tone ride one const, so a mark and its
    /// ink cannot disagree
    pub(super) const OK: &str = "{@ok|pass}";
    pub(super) const FAIL: &str = "{@fail|fail}";
    pub(super) const WARN: &str = "{@warn|skip}";
    pub(super) const BLOCKED: &str = "{@dot|dim}";
    pub(super) const BULLET: &str = "{@bullet|dim}";
    /// No `@arrow` glyph cell → caller binds `mark` (`→`)
    pub(super) const BOUND: &str = "{mark|dim}";

    pub(super) const HINT: &str = "  {hint|dim}";
    pub(super) const NOTE: &str = "{note}";
    /// Marker stays plain — the inactive one is a space, and ink around it is pure escape
    pub(super) const PROFILE: &str = "{marker} {name|bold}  ({summary|dim})";
    pub(super) const CURRENT: &str = "{name|bold}  ({summary|dim})";
    pub(super) const SAVED: &str = "{verb} `{name|bold}`  ({summary|dim})";
    pub(super) const DEFAULTED: &str = "`{name|bold}` is now the default";
    pub(super) const DEFAULT_SET: &str = "default is now `{name|bold}`";
    pub(super) const REMOVED: &str = "removed `{name|bold}`";
    pub(super) const PROBED: &str = "  {label}: {value|bold} (probed)";
    pub(super) const UNSET: &str = "  {label} unset: {why|dim}";
    pub(super) const UNSAVED: &str = "  {label} {value|bold} found but not saved: {detail|dim}";
    pub(super) const HEADER: &str =
        "cluster  {name|bold}  {class|dim}  (context: {context}, runtime: {runtime|dim})";
    pub(super) const REMEDY: &str = "      {note|dim}";
    pub(super) const TARGET: &str = "{@bullet|dim} target cluster: {url|bold}";
    pub(super) const READY: &str = "{@ok|pass} ztest resources ready. Run tests with: {cmd|bold}";
    pub(super) const OPTIONAL_DOWN: &str = concat!(
        "{@warn|skip} {name|bold} did not come up. Everything `ztest run` needs is ready; ",
        "metrics and profiling are not.",
    );
    pub(super) const GAP_TOOLS: &str = "{@fail|fail} {gap}; install {tools|bold} to fix it here";
    pub(super) const GAP_FLAG: &str = "{@fail|fail} {gap}; rerun with {flag|bold}";
    pub(super) const PAST_WINDOW: &str = concat!(
        "  {@warn|skip} server 1.{minor|bold} is past {reference|bold}'s {window|bold};",
        " deploying `latest`",
    );
    pub(super) const INSTALLED: &str = concat!(
        "  {@ok|pass} csi-hostpath installed",
        " ({class|bold} is now the default StorageClass)",
    );
    /// Repainted in place while a driver pod settles; `secs` drops on a non-TTY
    pub(super) const SETTLING: &str = "    {note}[  {secs:.0}s]";

    pub(super) fn capability(mark: &str) -> String {
        format!("  {mark} {{name:<26}}  {{detail|dim}}")
    }

    /// Mark + text, `detail` and `note` optional (an absent one takes its separator)
    pub(super) fn marked(mark: &str) -> String {
        format!("{mark} {{text}}[: {{detail|dim}}][ {{note|dim}}]")
    }

    /// [`marked`], indented under the command line that owns it
    pub(super) fn step(mark: &str) -> String {
        format!("  {}", marked(mark))
    }
}

/// Bind + draw one row (no `*` cell and no spinner anywhere here → zero width, zero elapsed)
fn say(src: &str, fields: &Fields<'_>, theme: &Theme) {
    println!("{}", draw(src, fields, theme));
}

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

impl Args {
    /// `--cluster` from whichever subcommand carries one, for `bind_cluster`
    pub(crate) fn cluster_profile(&self) -> Option<&str> {
        match &self.cmd {
            Cmd::Setup(a) => a.cluster_profile(),
            Cmd::Check { cluster } => cluster.as_deref(),
            _ => None,
        }
    }
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

    /// Container engine for builds, side-loads and host profiling. Omitted,
    /// ztest records whichever engine owns the cluster's node — pass this only
    /// when there is no node to observe, or to override what it found.
    #[arg(long, value_name = "ENGINE", value_parser = ["docker", "podman"])]
    runtime: Option<String>,

    /// Also make this the active default.
    #[arg(long)]
    set_default: bool,
}

pub fn execute(args: Args) -> ExitCode {
    // `setup`/`check`/`add` reach a cluster (own runtimes); the rest edit a local file
    let result = match args.cmd {
        Cmd::Setup(a) => return setup::execute(a),
        Cmd::Check { .. } => {
            return super::block_on("cluster check", super::Rt::Current, check());
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

fn list() -> Result<()> {
    let theme = Theme::detect();
    let cfg = cluster_config::load()?;
    if cfg.clusters.is_empty() {
        say(row::NOTE, &Fields::new().text("note", "no cluster profiles. Add one:"), &theme);
        say(row::HINT, &Fields::new().text("hint", "ztest cluster add zkn --kind"), &theme);
        return Ok(());
    }
    for (name, profile) in &cfg.clusters {
        let marker = if cfg.current.as_deref() == Some(name.as_str()) { "*" } else { " " };
        let data = Fields::new()
            .text("marker", marker)
            .text("name", name.as_str())
            .text("summary", profile.summary());
        say(row::PROFILE, &data, &theme);
    }
    Ok(())
}

fn current() -> Result<()> {
    let theme = Theme::detect();
    let cfg = cluster_config::load()?;
    match cfg.current.as_deref() {
        Some(name) => {
            let summary = cfg
                .clusters
                .get(name)
                .map(Profile::summary)
                .unwrap_or_else(|| "<dangling: profile removed>".to_string());
            let data = Fields::new().text("name", name).text("summary", summary);
            say(row::CURRENT, &data, &theme);
        }
        None => {
            let note = "no default cluster set (runs follow the ambient kube-context / env)";
            say(row::NOTE, &Fields::new().text("note", note), &theme);
        }
    }
    Ok(())
}

async fn add(a: AddArgs) -> Result<()> {
    // Bare `--kind` adopts the profile name, `--kind X` overrides.
    // Distribution never typed (a remote profile derives wholly from its kubeconfig).
    let mut profile = match (a.kind, &a.kubeconfig) {
        (Some(k), _) => Profile::local(&k.unwrap_or_else(|| a.name.clone())),
        (None, Some(kc)) => Profile::from_kubeconfig(std::path::Path::new(kc))?,
        (None, None) => return Err(anyhow!("pass --kind or --kubeconfig")),
    };
    let named_driver = a.storage_driver.is_some();
    profile.storage_driver = a.storage_driver;
    profile.runtime = a.runtime.as_deref().and_then(ContainerRuntime::parse);
    profile.validate()?;

    let mut cfg = cluster_config::load()?;
    let first = cfg.clusters.is_empty();
    let existed = cfg.clusters.insert(a.name.clone(), profile.clone()).is_some();
    // First profile defaults without `--set-default` (no ambiguity at one profile)
    if a.set_default || first {
        cfg.current = Some(a.name.clone());
    }
    cfg.save()?;

    let theme = Theme::detect();
    let verb = if existed { "updated" } else { "added" };
    let data = Fields::new()
        .text("verb", verb)
        .text("name", a.name.as_str())
        .text("summary", profile.summary());
    say(row::SAVED, &data, &theme);
    if cfg.current.as_deref() == Some(a.name.as_str()) {
        say(row::DEFAULTED, &Fields::new().text("name", a.name.as_str()), &theme);
    }
    if profile.runtime.is_none() {
        adopt_runtime(&a.name, &profile, &theme);
    }
    if !named_driver {
        adopt_storage_driver(&a.name, &theme).await;
    }
    Ok(())
}

/// `label` names which knob a probe row reports on ([`row::PROBED`] and friends serve
/// runtime and storage driver alike)
fn probed(label: &str, value: &str, saved: Result<()>, theme: &Theme) {
    let data = Fields::new().text("label", label).text("value", value);
    match saved {
        Ok(()) => say(row::PROBED, &data, theme),
        Err(e) => say(row::UNSAVED, &data.text("detail", e.to_string()), theme),
    }
}

fn unset(label: &str, why: &str, theme: &Theme) {
    say(row::UNSET, &Fields::new().text("label", label).text("why", why), theme);
}

/// Record the engine owning this cluster when the profile named none.
///
/// - kind profile → exact (node container = one engine's, never shared)
/// - no node to observe → sole live daemon, else left unset
/// - Never fatal: profile already saved, `ztest cluster check` = the gate
fn adopt_runtime(name: &str, profile: &Profile, theme: &Theme) {
    let observed = profile
        .kind_cluster()
        .and_then(|cluster| runtime::owner_of(&format!("{cluster}-control-plane")))
        .or_else(runtime::sole_usable);
    let Some(rt) = observed else {
        unset("runtime", "no single engine found — name one with --runtime", theme);
        return;
    };
    probed("runtime", rt.as_str(), persist_runtime(name, rt), theme);
}

fn persist_runtime(name: &str, rt: ContainerRuntime) -> Result<()> {
    let mut cfg = cluster_config::load()?;
    let profile = cfg.clusters.get_mut(name).context("profile vanished between write and probe")?;
    profile.runtime = Some(rt);
    Ok(cfg.save()?)
}

/// Record the cluster's snapshot-capable driver when the profile named none.
///
/// - Unnamed = follow the default StorageClass, which on a stock `kind create cluster` cannot
///   snapshot → seeding silently degrades, and only a fixture-mounting run finds out
/// - Only on exactly one candidate (several = a choice ztest must not make silently)
/// - Never fatal: the profile is already saved, and `ztest cluster check` is the gate
async fn adopt_storage_driver(name: &str, theme: &Theme) {
    // The profile just written, not the one bound at dispatch — and reached by naming its
    // context, never by mutating env: this runs inside the runtime, where `set_var` is
    // unsound and a global rebind would leak into the rest of the command
    let context = cluster_config::load()
        .ok()
        .and_then(|cfg| cfg.clusters.get(name).and_then(|p| p.context.clone()));
    let connect = async {
        match &context {
            Some(ctx) => ztest::api::cluster::client_for_context(ctx).await,
            None => ztest::api::cluster::client().await,
        }
    };
    let Ok(client) = connect.await else {
        unset("storage driver", "cluster unreachable — run `ztest cluster check`", theme);
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
        Ok(only) => probed("storage driver", only, persist_storage_driver(name, only), theme),
        Err(why) => unset("storage driver", &why.to_string(), theme),
    }
}

/// Why there is no unambiguous snapshot-capable driver
#[derive(Debug, thiserror::Error)]
enum NoDriver {
    #[error("no snapshot-capable storage")]
    None,
    /// The candidates stay in the message: `--storage-driver` takes one of *these*, and a
    /// bare count sends the reader back to `kubectl get storageclass`
    #[error("pass --storage-driver: {0}")]
    Ambiguous(String),
}

/// Sole snapshot-capable driver, or why there is no unambiguous one
fn storage_choice(drivers: &[String]) -> Result<&str, NoDriver> {
    match drivers {
        [only] => Ok(only),
        [] => Err(NoDriver::None),
        many => Err(NoDriver::Ambiguous(many.join(", "))),
    }
}

fn persist_storage_driver(name: &str, driver: &str) -> Result<()> {
    let mut cfg = cluster_config::load()?;
    let profile = cfg.clusters.get_mut(name).context("profile vanished between write and probe")?;
    profile.storage_driver = Some(driver.to_string());
    Ok(cfg.save()?)
}

fn set(name: String) -> Result<()> {
    let mut cfg = cluster_config::load()?;
    if !cfg.clusters.contains_key(&name) {
        return Err(anyhow!("no profile `{name}`. Known: {}", known(&cfg)));
    }
    cfg.current = Some(name.clone());
    cfg.save()?;
    say(row::DEFAULT_SET, &Fields::new().text("name", name), &Theme::detect());
    Ok(())
}

fn remove(name: String) -> Result<()> {
    let mut cfg = cluster_config::load()?;
    if cfg.clusters.remove(&name).is_none() {
        return Err(anyhow!("no profile `{name}`. Known: {}", known(&cfg)));
    }
    if cfg.current.as_deref() == Some(name.as_str()) {
        cfg.current = None;
    }
    cfg.save()?;
    say(row::REMOVED, &Fields::new().text("name", name), &Theme::detect());
    Ok(())
}

fn known(cfg: &Config) -> String {
    let names: Vec<&str> = cfg.clusters.keys().map(String::as_str).collect();
    if names.is_empty() { "(none)".to_string() } else { names.join(", ") }
}

/// Probe the cluster, print what it can and cannot do
/// (non-zero only on a missing *required* capability, else unusable as a CI gate)
async fn check() -> Result<()> {
    // Bound by `bind_cluster` in dispatch; this only needs its name for the banner
    let bound = cluster_config::active_profile();
    let client = ztest::api::cluster::client().await.context("connecting to cluster")?;

    let report = ztest::api::capability::probe(&client).await;
    print!("{}", render(&report, bound, &Theme::detect()));

    match report.is_runnable() {
        true => Ok(()),
        false => Err(anyhow!("{} capabilities missing", report.blocking().count())),
    }
}

/// One line per capability, remedy only where actionable
fn render(report: &ztest::api::capability::Report, bound: Option<&str>, theme: &Theme) -> String {
    use std::fmt::Write as _;
    use ztest::api::capability::{Finding, Need};

    let mut out = String::new();

    let header = Fields::new()
        .text("name", bound.unwrap_or("-"))
        .text("class", ztest::backends::image::selected_class().label())
        .text("context", cluster_config::active_context().unwrap_or_else(|| "(ambient)".into()))
        .text("runtime", runtime::program());
    let _ = writeln!(out, "{}", draw(row::HEADER, &header, theme));

    for cap in &report.capabilities {
        // Missing optional = warning (costs a feature, not the run)
        let mark = match (&cap.finding, cap.need) {
            (Finding::Present(_), _) => row::OK,
            (_, Need::Required | Need::Provisioned | Need::RequiredForRun) => row::FAIL,
            (_, Need::Enables(_)) => row::WARN,
        };
        let data = Fields::new().text("name", cap.name).text("detail", cap.finding.detail());
        let _ = writeln!(out, "{}", draw(&row::capability(mark), &data, theme));
        if !cap.finding.is_present() {
            let lost = match cap.need {
                Need::Required | Need::Provisioned | Need::RequiredForRun => "ztest run",
                Need::Enables(f) => f,
            };
            let note = format!("{} {lost} unavailable; {}", theme.chars.arrow, cap.remedy);
            let _ =
                writeln!(out, "{}", draw(row::REMEDY, &Fields::new().text("note", note), theme));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use ztest_ui::template::Template;

    use ztest::api::capability::{Capability, Finding, Need, Report};

    fn drivers(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// One row per verdict the mark table distinguishes
    fn report() -> Report {
        Report {
            capabilities: vec![
                Capability {
                    name: "image registry",
                    need: Need::Required,
                    finding: Finding::Present("in-cluster".into()),
                    remedy: "see docs/registry.md",
                },
                Capability {
                    name: "snapshot-capable storage",
                    need: Need::Required,
                    finding: Finding::Absent("no CSI driver".into()),
                    remedy: "see docs/storage.md",
                },
                Capability {
                    name: "metrics stack",
                    need: Need::Enables("profiling"),
                    finding: Finding::Absent("no ztest-obs".into()),
                    remedy: "see docs/obs.md",
                },
            ],
        }
    }

    /// `Template::parse` panics on a malformed source, and most of these rows draw only on
    /// a failing cluster — a typo would otherwise surface as a panic mid-provision
    #[test]
    fn every_row_parses() {
        let marks = [row::OK, row::FAIL, row::WARN, row::BLOCKED, row::BULLET, row::BOUND];
        let composed: Vec<String> =
            marks.iter().flat_map(|m| [row::capability(m), row::marked(m), row::step(m)]).collect();
        let fixed = [
            row::HINT,
            row::NOTE,
            row::PROFILE,
            row::CURRENT,
            row::SAVED,
            row::DEFAULTED,
            row::DEFAULT_SET,
            row::REMOVED,
            row::PROBED,
            row::UNSET,
            row::UNSAVED,
            row::HEADER,
            row::REMEDY,
            row::TARGET,
            row::READY,
            row::OPTIONAL_DOWN,
            row::GAP_TOOLS,
            row::GAP_FLAG,
            row::PAST_WINDOW,
            row::INSTALLED,
            row::SETTLING,
        ];
        for src in fixed.into_iter().chain(composed.iter().map(String::as_str)) {
            Template::parse(src);
        }
    }

    /// Every mark used to be a hardcoded glyph, so a terminal without Unicode got mojibake
    /// where a verdict belonged
    #[test]
    fn a_finding_marks_itself_in_whichever_theme_is_active() {
        let unicode = render(&report(), Some("zkn"), &Theme::for_capabilities(false, true));
        let ascii = render(&report(), Some("zkn"), &Theme::for_capabilities(false, false));
        for (drawn, (ok, fail, warn)) in
            [(&unicode, ("✓", "✗", "!")), (&ascii, ("OK", "FAIL", "WARN"))]
        {
            assert!(drawn.contains(&format!("  {ok} image registry")), "{drawn}");
            assert!(drawn.contains(&format!("  {fail} snapshot-capable storage")), "{drawn}");
            assert!(drawn.contains(&format!("  {warn} metrics stack")), "{drawn}");
        }
    }

    /// The whole `cluster check` frame in ascii. A remedy arrow and a cordon mark were
    /// hardcoded here, and a Unicode golden reads identically either way
    #[test]
    fn the_cluster_report_falls_back_to_ascii() {
        let ascii = Theme::for_capabilities(false, false);
        for context in [Some("zkn"), None] {
            ztest_ui::testing::assert_ascii_clean(
                "cluster::render",
                &render(&report(), context, &ascii),
            );
        }
    }

    /// A missing capability is only actionable with the remedy under it; a present one
    /// would only add noise
    #[test]
    fn only_a_missing_capability_carries_a_remedy() {
        let drawn = render(&report(), None, &Theme::for_capabilities(false, true));
        assert!(drawn.contains("→ ztest run unavailable; see docs/storage.md"), "{drawn}");
        assert!(drawn.contains("→ profiling unavailable; see docs/obs.md"), "{drawn}");
        assert!(!drawn.contains("docs/registry.md"), "{drawn}");
    }

    #[test]
    fn a_sole_driver_is_adopted() {
        assert_eq!(storage_choice(&drivers(&["topolvm.io"])).ok(), Some("topolvm.io"));
    }

    /// Picking for the user here would bind every seeded run to a driver they never named
    #[test]
    fn several_drivers_are_left_to_the_user() {
        let why = storage_choice(&drivers(&["hostpath.csi.k8s.io", "topolvm.io"]))
            .expect_err("two candidates cannot be resolved")
            .to_string();
        assert!(why.contains("--storage-driver"), "{why}");
        assert!(why.contains("topolvm.io"), "the candidates must be named: {why}");
    }

    /// Stock `kind create cluster` — the case the quickstart walks into
    #[test]
    fn no_snapshot_capable_driver_is_named_as_such() {
        let why = storage_choice(&[]).expect_err("nothing to choose").to_string();
        assert_eq!(why, "no snapshot-capable storage");
    }
}
