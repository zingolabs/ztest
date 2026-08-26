//! Optional csi-hostpath install — `ztest cluster setup`, snapshot-less local cluster.
//!
//! - Local only (shared-cluster storage = operator's)
//! - Assent-gated (cluster-scoped writes + steals the default StorageClass)
//! - Upstream manifests @ pinned refs, deploy dir = server minor (`latest` past the pin's window)
//! - `deploy.sh` owns the success edge (only it knows the set it applied); ztest owns the
//!   narration + every fail-fast verdict ([`until_stuck`])

use std::collections::{BTreeSet, HashMap};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, anyhow};
use dialoguer::Confirm;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{Pod, PodStatus};
use kube::Client;
use kube::api::{Api, ListParams, Patch, PatchParams};
use serde_json::json;

use super::{draw, row};
use ztest::api::capability::{self, Capability, Finding, Report};
use ztest::api::pod_status::{POLL_INTERVAL, ReadyWatch, Verdict};
use ztest_ui::Theme;
use ztest_ui::template::Fields;

const SNAPSHOTTER_REF: &str = "v8.6.0";
const HOSTPATH_REF: &str = "v1.18.0";
const STORAGE_CLASS: &str = "csi-hostpath-sc";
const DEFAULT_CLASS_ANNOTATION: &str = "storageclass.kubernetes.io/is-default-class";

/// Gaps this install closes — CRDs, the controller reconciling them, and a driver
/// backing both. Named by symbol, so a capability rename cannot silently detach the
/// offer from the gap
const FIXABLE: [&str; 3] =
    [capability::STORAGE, capability::SNAPSHOT_API, capability::SNAPSHOT_CONTROLLER];

/// FIXABLE pair = one gap to a reader (probe names report the harness, not the cluster)
const GAP: &str = "no volume snapshots";

/// Subprocesses [`install`] drives
const TOOLS: [&str; 2] = ["kubectl", "git"];

/// Every object `deploy.sh` applies carries it (kustomize `commonLabels`)
const DRIVER_LABEL: &str = "app.kubernetes.io/instance=hostpath.csi.k8s.io";

/// Namespace + workload `deploy.sh` pins; neither is ours to configure
const PLUGIN_NAMESPACE: &str = "default";
const PLUGIN_STS: &str = "csi-hostpathplugin";
const SNAPSHOTTER_CONTAINER: &str = "csi-snapshotter";
const TIMEOUT_FLAG: &str = "--timeout=";

/// csi-snapshotter's per-RPC deadline, raised off its 1m default.
///
/// - hostpath `tar czf`s the whole volume *inside* `CreateSnapshot`, under the driver's global
///   mutex, and the sidecar's cancel never reaches that `tar` (RPC ctx unplumbed into the exec)
/// - So the default deadline buys no bound, only a retry storm: every retry blocks on the same
///   mutex and expires again, while the first copy runs on regardless
/// - Sized to the largest seed ztest publishes, not to a copy's true duration (unmodelable, as
///   the pull's is): a 250 GiB chain through single-threaded gzip runs ~3.5h
/// - Overrunning it is not a failure — it degrades to the retry storm this removes, which does
///   converge once the first copy lands. `materialize::snapshot` owns the hang verdict either way
const SNAPSHOT_RPC_DEADLINE_MINS: u64 = 240;

/// Budget from `Running` to Ready. Pull time sits *before* `Running`, so this bounds only
/// probe failures — an unbounded cold pull stays the operator's call to watch or abort
const READY_TIMEOUT: Duration = Duration::from_secs(120);

/// One rendered row to stderr (stdout stays the forwarded script's)
fn say(src: &str, fields: &Fields<'_>, theme: &Theme) {
    eprintln!("{}", draw(src, fields, theme));
}

/// One install step, announced before it runs
fn progress(text: &str, theme: &Theme) {
    say(&row::step(row::BULLET), &Fields::new().text("text", text), theme);
}

/// How the local snapshot gap resolved, ahead of any capability report.
///
/// - `Explained` = gap + its fix already on stderr (caller exits without restating)
/// - `Unrelated` = not this gap (caller's own report)
pub enum Offer {
    Install,
    Explained,
    Unrelated,
}

pub fn offer(report: &Report, non_interactive: bool, install: bool) -> Result<Offer> {
    if !fixable_here(report) {
        return Ok(Offer::Unrelated);
    }
    let theme = Theme::detect();
    // Before assent, not inside install() (assent then a tooling error = the offer was a lie)
    let missing: Vec<&str> = TOOLS.into_iter().filter(|b| which(b).is_err()).collect();
    if !missing.is_empty() {
        let data = Fields::new().text("gap", GAP).text("tools", missing.join(" + "));
        say(row::GAP_TOOLS, &data, &theme);
        return Ok(Offer::Explained);
    }
    if install {
        return Ok(Offer::Install);
    }
    // Unattended = never mutate cluster-scoped state by default
    if non_interactive || !std::io::stdin().is_terminal() {
        let data = Fields::new().text("gap", GAP).text("flag", "--install-storage");
        say(row::GAP_FLAG, &data, &theme);
        return Ok(Offer::Explained);
    }
    // One line, no wrap (dialoguer re-renders on answer → a wrapped prompt prints twice)
    let yes = Confirm::new()
        .with_prompt(format!("{GAP} — install csi-hostpath? (seed copies, no CoW)"))
        .default(false)
        .interact()
        .context("confirmation prompt")?;
    if !yes {
        let data = Fields::new()
            .text("mark", theme.chars.arrow)
            .text("text", "nothing installed; `ztest run` needs snapshots");
        say(&row::step(row::BOUND), &data, &theme);
        return Ok(Offer::Explained);
    }
    Ok(Offer::Install)
}

/// Storage = the blocker, cluster = local.
///
/// - Setup blockers, not run blockers: everything `setup` is about to provision is also
///   absent on a fresh cluster, and folding those in means the offer never fires
/// - Storage itself must be the gap. A restarting snapshot-controller on a cluster that
///   already has TopoLVM is briefly a blocker too, and installing here would steal the
///   default StorageClass out from under it
fn fixable_here(report: &Report) -> bool {
    ztest::backends::image::selected_class() == ztest::api::cluster_config::ClusterClass::Local
        && repairs_the_gap(report)
}

/// Cluster-independent half, so the offer's rule is testable without one.
///
/// `Absent`, never `Unknown`: an unreachable cluster or a forbidden list is not a cluster
/// missing a driver, and installing on that reading steals the default StorageClass
fn repairs_the_gap(report: &Report) -> bool {
    let absent = |c: &&Capability| matches!(c.finding, Finding::Absent(_));
    report.setup_blockers().filter(absent).any(|c| c.name == capability::STORAGE)
        && report.setup_blockers().all(|c| FIXABLE.contains(&c.name))
}

/// external-snapshotter (CRDs + controller) → csi-hostpath → class pair → default class.
///
/// - Every step a subprocess, sequential (setup CLI, nothing else in flight); only
///   [`deploy_driver`] runs anything alongside it
pub async fn install(client: &Client) -> Result<()> {
    for bin in TOOLS {
        which(bin)?;
    }
    let theme = Theme::detect();
    let work = WorkDir::new()?;
    // deploy.sh = kubectl with no --context, no override hook → pin via single-context kubeconfig
    let kubeconfig = minified_kubeconfig(work.path())?;

    progress("external-snapshotter CRDs", &theme);
    kubectl(&kubeconfig, &["apply", "-k", &crd_url()])?;
    // Controller RBAC references these kinds (unestablished CRDs → NotFound race)
    kubectl(
        &kubeconfig,
        &[
            "wait",
            "--for=condition=Established",
            "--timeout=90s",
            "crd/volumesnapshots.snapshot.storage.k8s.io",
            "crd/volumesnapshotcontents.snapshot.storage.k8s.io",
            "crd/volumesnapshotclasses.snapshot.storage.k8s.io",
        ],
    )?;

    progress("snapshot-controller", &theme);
    kubectl(&kubeconfig, &["apply", "-k", &controller_url()])?;
    kubectl(
        &kubeconfig,
        &["-n", "kube-system", "rollout", "status", "deploy/snapshot-controller", "--timeout=180s"],
    )?;

    progress(&format!("csi-hostpath driver ({HOSTPATH_REF})"), &theme);
    let checkout = work.path().join("csi-hostpath");
    run(Command::new("git").args([
        "clone",
        "--quiet",
        "--depth",
        "1",
        "--branch",
        HOSTPATH_REF,
        "https://github.com/kubernetes-csi/csi-driver-host-path.git",
        &checkout.display().to_string(),
    ]))?;

    let minor = server_minor(&kubeconfig)?;
    let root = checkout.join("deploy");
    let window = DeployWindow::read(&root)?;
    let deploy = match window.select(minor) {
        Deploy::Exact(m) => root.join(format!("kubernetes-1.{m}")),
        Deploy::Latest => {
            let data = Fields::new()
                .text("minor", minor.to_string())
                .text("reference", HOSTPATH_REF)
                .text("window", window.to_string());
            say(row::PAST_WINDOW, &data, &theme);
            root.join("kubernetes-latest")
        }
        Deploy::Unsupported => {
            return Err(anyhow!("{HOSTPATH_REF} covers {window}, server is 1.{minor}"));
        }
    };
    deploy_driver(client, &deploy, &kubeconfig, &theme).await?;

    progress("csi-snapshotter RPC deadline", &theme);
    pin_snapshotter_timeout(client, &kubeconfig).await?;

    progress("StorageClass + VolumeSnapshotClass", &theme);
    let examples = checkout.join("examples");
    kubectl(
        &kubeconfig,
        &["apply", "-f", &examples.join("csi-storageclass.yaml").display().to_string()],
    )?;
    kubectl(
        &kubeconfig,
        &["apply", "-f", &examples.join("csi-volumesnapshotclass.yaml").display().to_string()],
    )?;

    make_default(&kubeconfig)?;
    say(row::INSTALLED, &Fields::new().text("class", STORAGE_CLASS), &theme);
    Ok(())
}

/// Run `deploy.sh`, narrating driver-pod readiness beside it.
///
/// - Script decides success (sole knower of the set it applied), re-checking within a tick
///   of the last pod going Ready — no upstream hook to skip its loop anyway
/// - Its failure edge = 5min silent, then a `describe` + logs wall → [`until_stuck`] pre-empts
///   it, naming the container
/// - Script stdout piped, not inherited (its wait counter would fight the painted line)
async fn deploy_driver(
    client: &Client,
    deploy: &Path,
    kubeconfig: &Path,
    theme: &Theme,
) -> Result<()> {
    let script = deploy.join("deploy.sh");
    let mut child = tokio::process::Command::new(&script)
        .env("KUBECONFIG", kubeconfig)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn {}", script.display()))?;
    let stdout = child.stdout.take().expect("stdout piped");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let forwarder = tokio::spawn(forward(stdout, tx));

    let mut line = StatusLine::new(theme.clone());
    let outcome = tokio::select! {
        exited = child.wait() => Outcome::Exited(exited),
        stuck = until_stuck(client, &mut line, &mut rx) => Outcome::Stuck(stuck),
    };
    if matches!(outcome, Outcome::Stuck(_)) {
        terminate(&mut child).await;
    }
    // Sender drops with the forwarder → the drain below terminates (script's dump lands in it)
    let _ = forwarder.await;
    line.finish();
    while let Some(text) = rx.recv().await {
        println!("{text}");
    }

    match outcome {
        Outcome::Stuck(why) => Err(anyhow!("{why}")),
        Outcome::Exited(exited) => {
            let status = exited.context("wait for deploy.sh")?;
            match status.success() {
                true => Ok(()),
                false => Err(anyhow!("csi-hostpath deploy.sh failed ({status})")),
            }
        }
    }
}

enum Outcome {
    Exited(std::io::Result<std::process::ExitStatus>),
    Stuck(String),
}

/// Line the script's wait loop repeats every 10s; ztest's own line says strictly more
const WAIT_COUNTER: &str = "waiting for hostpath deployment to complete";

/// Script stdout → painter, [`WAIT_COUNTER`] dropped. Everything else passes verbatim
async fn forward(
    stdout: tokio::process::ChildStdout,
    tx: tokio::sync::mpsc::UnboundedSender<String>,
) {
    use tokio::io::AsyncBufReadExt as _;

    let mut lines = tokio::io::BufReader::new(stdout).lines();
    while let Ok(Some(text)) = lines.next_line().await {
        if !text.contains(WAIT_COUNTER) && tx.send(text).is_err() {
            return;
        }
    }
}

/// Poll driver pods until one provably will not become Ready. Returns the reason.
///
/// - No success return (`deploy.sh` owns that edge) → no race against a StatefulSet it has
///   applied but we have not yet listed
/// - Empty list = not applied yet, never done
/// - Transient list errors ignored, as in the runner-pod loop
async fn until_stuck(
    client: &Client,
    line: &mut StatusLine,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
) -> String {
    let pods: Api<Pod> = Api::default_namespaced(client.clone());
    let params = ListParams::default().labels(DRIVER_LABEL);
    let mut watches: HashMap<String, ReadyWatch> = HashMap::new();
    let started = Instant::now();
    // Independent of the forwarding arm (a burst of script output must not starve the poll)
    let mut tick = tokio::time::interval(POLL_INTERVAL);
    loop {
        tokio::select! {
            Some(text) = rx.recv() => line.emit(&text),
            _ = tick.tick() => {
                let Ok(list) = pods.list(&params).await else { continue };
                let mut pending = Vec::new();
                for pod in &list.items {
                    let (Some(name), Some(status)) =
                        (pod.metadata.name.as_deref(), pod.status.as_ref())
                    else {
                        continue;
                    };
                    let watch = watches
                        .entry(name.to_string())
                        .or_insert_with(|| ReadyWatch::new(READY_TIMEOUT));
                    match watch.observe(status, Instant::now()) {
                        Verdict::Ready => {}
                        Verdict::Waiting => pending.push(format!("{name} {}", note(status))),
                        Verdict::Unschedulable { reason, elapsed } => {
                            let secs = elapsed.as_secs();
                            return stuck(name, &format!("unplaceable for {secs}s: {reason}"));
                        }
                        Verdict::Faulted(reason) | Verdict::PullFailed(reason) => {
                            return stuck(name, &reason);
                        }
                        Verdict::ReadyTimeout(budget) => {
                            let secs = budget.as_secs();
                            return stuck(name, &format!("{} after {secs}s", note(status)));
                        }
                    }
                }
                if list.items.is_empty() {
                    pending.push("applying manifests".to_string());
                }
                line.set(&pending.join(" · "), started.elapsed());
            }
        }
    }
}

fn stuck(pod: &str, detail: &str) -> String {
    format!("csi-hostpath driver stuck ({pod} {detail}); `kubectl describe pod {pod}` for events")
}

/// `5/8 ready · csi-provisioner: ImagePullBackOff` — the count, then the first laggard's own word
fn note(status: &PodStatus) -> String {
    let all = status.container_statuses.as_deref().unwrap_or_default();
    let Some(total) = std::num::NonZeroUsize::new(all.len()) else {
        return status.phase.clone().unwrap_or_else(|| "Pending".to_string());
    };
    let ready = all.iter().filter(|c| c.ready).count();
    let head = format!("{ready}/{total} ready");
    match all.iter().find(|c| !c.ready) {
        None => head,
        Some(c) => match c.state.as_ref().and_then(|s| s.waiting.as_ref()) {
            Some(w) => match w.reason.as_deref() {
                Some(reason) => format!("{head} · {}: {reason}", c.name),
                None => format!("{head} · {}", c.name),
            },
            None => format!("{head} · {}", c.name),
        },
    }
}

/// SIGTERM, never `start_kill` — `deploy.sh` drops its `mktemp -d` from an EXIT trap
async fn terminate(child: &mut tokio::process::Child) {
    if let Some(pid) = child.id() {
        // SAFETY: pid of a child we own and have not reaped, so it cannot have been reused
        unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    }
    let _ = child.wait().await;
}

/// One stderr line repainted in place.
///
/// - Non-TTY = print on change only, elapsed dropped (CI logs must not carry repaint frames)
/// - stderr, as the bullets above it (one stream → no interleave)
struct StatusLine {
    term: console::Term,
    tty: bool,
    theme: Theme,
    last: String,
    painted: bool,
}

impl StatusLine {
    fn new(theme: Theme) -> Self {
        let term = console::Term::stderr();
        let tty = term.is_term();
        StatusLine { term, tty, theme, last: String::new(), painted: false }
    }

    fn set(&mut self, text: &str, elapsed: Duration) {
        let data = Fields::new().text("note", text);
        if self.tty {
            // Elapsed only here — its group drops below, so a CI log carries no repaint frames
            let data = data.value("secs", elapsed.as_secs() as f64);
            let _ = self.term.clear_line();
            let _ = self.term.write_str(&draw(row::SETTLING, &data, &self.theme));
            self.painted = true;
            return;
        }
        if text != self.last {
            self.last = text.to_string();
            eprintln!("{}", draw(row::SETTLING, &data, &self.theme));
        }
    }

    /// Forwarded script output: clear, print, let the next tick repaint below it
    fn emit(&mut self, text: &str) {
        self.finish();
        println!("{text}");
    }

    /// Idempotent — every path closes the line before printing anything else
    fn finish(&mut self) {
        if self.painted {
            self.painted = false;
            let _ = self.term.clear_line();
        }
    }
}

/// kind's default `standard` (local-path) = no snapshots, no expansion → unnamed PVCs unusable
fn make_default(kubeconfig: &Path) -> Result<()> {
    let listed = output(Command::new("kubectl").env("KUBECONFIG", kubeconfig).args([
        "get",
        "storageclass",
        "-o",
        "name",
    ]))?;
    for sc in listed.lines().map(str::trim).filter(|s| !s.is_empty()) {
        let default = sc == format!("storageclass.storage.k8s.io/{STORAGE_CLASS}");
        let patch = format!(
            "{{\"metadata\":{{\"annotations\":{{\"{DEFAULT_CLASS_ANNOTATION}\":\"{default}\"}}}}}}"
        );
        kubectl(kubeconfig, &["patch", sc, "-p", &patch])?;
    }
    Ok(())
}

fn crd_url() -> String {
    format!(
        "https://github.com/kubernetes-csi/external-snapshotter//client/config/crd?ref={SNAPSHOTTER_REF}"
    )
}

fn controller_url() -> String {
    format!(
        "https://github.com/kubernetes-csi/external-snapshotter//deploy/kubernetes/snapshot-controller?ref={SNAPSHOTTER_REF}"
    )
}

/// Picks the deploy dir ([`DeployWindow`])
fn server_minor(kubeconfig: &Path) -> Result<u32> {
    let raw = output(
        Command::new("kubectl").env("KUBECONFIG", kubeconfig).args(["version", "-o", "json"]),
    )?;
    let json: serde_json::Value = serde_json::from_str(&raw).context("parse `kubectl version`")?;
    let minor = json["serverVersion"]["minor"]
        .as_str()
        .context("`kubectl version` reported no server minor")?;
    // Managed distributions suffix it ("29+")
    minor
        .trim_end_matches(|c: char| !c.is_ascii_digit())
        .parse()
        .with_context(|| format!("server minor `{minor}`"))
}

/// Kubernetes minors a release's `deploy/` names = what it was tested against.
///
/// - One manifest set per release (numbered dirs + `latest` = symlinks onto it) → minor labels
///   the pairing, never the content
/// - Sparse across releases (no release ever shipped 1.29 / 1.32 / 1.33)
#[derive(Debug)]
struct DeployWindow(BTreeSet<u32>);

/// Which `deploy/` dir serves a server minor
#[derive(Debug, PartialEq)]
enum Deploy {
    Exact(u32),
    Latest,
    Unsupported,
}

impl DeployWindow {
    fn read(root: &Path) -> Result<Self> {
        let entries = std::fs::read_dir(root)
            .with_context(|| format!("read {}", root.display()))?
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("read {}", root.display()))?;
        // Name-parsed, not is_dir(): every minor but the oldest is a symlink
        let minors: BTreeSet<u32> = entries
            .iter()
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.strip_prefix("kubernetes-1.")?.parse().ok()
            })
            .collect();
        if minors.is_empty() {
            return Err(anyhow!("{HOSTPATH_REF}: no kubernetes minor under {}", root.display()));
        }
        Ok(Self(minors))
    }

    /// Past the window = routine (kind ships a minor long before the driver cuts a release)
    fn select(&self, minor: u32) -> Deploy {
        if self.0.contains(&minor) {
            Deploy::Exact(minor)
        } else if minor < self.oldest() {
            Deploy::Unsupported
        } else {
            Deploy::Latest
        }
    }

    fn oldest(&self) -> u32 {
        *self.0.iter().next().expect("read() rejects an empty window")
    }

    fn newest(&self) -> u32 {
        *self.0.iter().next_back().expect("read() rejects an empty window")
    }
}

impl std::fmt::Display for DeployWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (oldest, newest) = (self.oldest(), self.newest());
        if oldest == newest { write!(f, "1.{oldest}") } else { write!(f, "1.{oldest}-1.{newest}") }
    }
}

fn minified_kubeconfig(work: &Path) -> Result<PathBuf> {
    let mut cmd = Command::new("kubectl");
    cmd.args(["config", "view", "--raw", "--minify"]);
    if let Some(ctx) = ztest::api::cluster_config::active_context() {
        cmd.args(["--context", &ctx]);
    }
    let path = work.join("kubeconfig");
    std::fs::write(&path, output(&mut cmd)?)
        .with_context(|| format!("write {}", path.display()))?;
    Ok(path)
}

/// Raise the snapshotter's RPC deadline past any copy this cluster will be asked to make.
///
/// - Read-modify-write, so an upstream arg change survives the patch
/// - Strategic merge: `containers` merges by `name`, leaving every sidecar but this one alone
/// - Only safe now that ztest watches snapshot liveness itself — a long deadline with no
///   watcher would trade a retry storm for a silent hang
async fn pin_snapshotter_timeout(client: &Client, kubeconfig: &Path) -> Result<()> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), PLUGIN_NAMESPACE);
    let sts = api
        .get_opt(PLUGIN_STS)
        .await?
        .ok_or_else(|| anyhow!("{PLUGIN_STS} absent after deploy.sh"))?;
    let args = snapshotter_args(&sts)
        .ok_or_else(|| anyhow!("no {SNAPSHOTTER_CONTAINER} container in {PLUGIN_STS}"))?;
    let patch = json!({
        "spec": { "template": { "spec": { "containers": [
            { "name": SNAPSHOTTER_CONTAINER, "args": with_deadline(args) }
        ] } } }
    });
    api.patch(PLUGIN_STS, &PatchParams::default(), &Patch::Strategic(&patch)).await?;
    kubectl(
        kubeconfig,
        &[
            "-n",
            PLUGIN_NAMESPACE,
            "rollout",
            "status",
            &format!("statefulset/{PLUGIN_STS}"),
            "--timeout=180s",
        ],
    )
}

fn snapshotter_args(sts: &StatefulSet) -> Option<Vec<String>> {
    sts.spec
        .as_ref()?
        .template
        .spec
        .as_ref()?
        .containers
        .iter()
        .find(|c| c.name == SNAPSHOTTER_CONTAINER)
        .map(|c| c.args.clone().unwrap_or_default())
}

/// Flag replaced where present, appended where not (re-install must not stack duplicates)
fn with_deadline(args: Vec<String>) -> Vec<String> {
    let mut kept: Vec<String> = args.into_iter().filter(|a| !a.starts_with(TIMEOUT_FLAG)).collect();
    kept.push(format!("{TIMEOUT_FLAG}{SNAPSHOT_RPC_DEADLINE_MINS}m"));
    kept
}

fn kubectl(kubeconfig: &Path, args: &[&str]) -> Result<()> {
    run(Command::new("kubectl").env("KUBECONFIG", kubeconfig).args(args))
}

fn run(cmd: &mut Command) -> Result<()> {
    let status = cmd.status().with_context(|| format!("spawn {:?}", cmd.get_program()))?;
    if !status.success() {
        return Err(anyhow!("{:?} failed ({status})", cmd.get_program()));
    }
    Ok(())
}

fn output(cmd: &mut Command) -> Result<String> {
    let out = cmd.output().with_context(|| format!("spawn {:?}", cmd.get_program()))?;
    if !out.status.success() {
        return Err(anyhow!(
            "{:?} failed ({}): {}",
            cmd.get_program(),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    String::from_utf8(out.stdout).context("non-UTF-8 output")
}

fn which(bin: &str) -> Result<()> {
    ztest::api::on_path(bin)
        .then_some(())
        .with_context(|| format!("`{bin}` not on PATH; needed to install csi-hostpath"))
}

/// Scratch dir, removed on drop (`tempfile` = wallet-feature-gated)
struct WorkDir(PathBuf);

impl WorkDir {
    fn new() -> Result<Self> {
        let path = std::env::temp_dir().join(format!("ztest-csi-hostpath-{}", std::process::id()));
        std::fs::create_dir_all(&path).with_context(|| format!("create {}", path.display()))?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_deadline_replaces_an_existing_flag_rather_than_stacking_one() {
        let got = with_deadline(vec!["-v=5".into(), "--timeout=1m".into()]);
        let want = format!("--timeout={SNAPSHOT_RPC_DEADLINE_MINS}m");
        assert_eq!(got, vec!["-v=5".to_string(), want]);
    }

    #[test]
    fn upstream_args_survive_the_patch() {
        let got = with_deadline(vec!["-v=5".into(), "--csi-address=/csi/csi.sock".into()]);
        assert_eq!(got[0], "-v=5");
        assert_eq!(got[1], "--csi-address=/csi/csi.sock");
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn re_running_the_install_is_idempotent() {
        let once = with_deadline(vec!["-v=5".into()]);
        assert_eq!(with_deadline(once.clone()), once);
    }

    /// 250 GiB (largest published seed) through single-threaded gzip, at a pessimistic
    /// 20 MiB/s — the deadline must clear the biggest copy this cluster can be handed
    #[test]
    fn the_deadline_clears_the_largest_seed_a_default_timeout_cannot() {
        let biggest_copy_mins = (250 * 1024) / 20 / 60;
        assert!(
            SNAPSHOT_RPC_DEADLINE_MINS >= biggest_copy_mins,
            "{SNAPSHOT_RPC_DEADLINE_MINS}m does not cover {biggest_copy_mins}m"
        );
    }

    use k8s_openapi::api::core::v1::{ContainerState, ContainerStateWaiting, ContainerStatus};
    use ztest::api::capability::Need;

    fn window(minors: [u32; 2]) -> DeployWindow {
        DeployWindow(minors.into_iter().collect())
    }

    /// v1.18.0's shape: one real dir, the rest symlinks, plus the `-test` and `distributed` noise
    fn v1_18_0_deploy() -> PathBuf {
        let root = std::env::temp_dir()
            .join(format!("ztest-csi-window-{}", std::process::id()))
            .join("deploy");
        let _ = std::fs::remove_dir_all(&root);
        for entry in [
            "kubernetes-1.34",
            "kubernetes-1.34-test",
            "kubernetes-1.35",
            "kubernetes-1.35-test",
            "kubernetes-distributed",
            "kubernetes-latest",
            "kubernetes-latest-test",
            "util",
        ] {
            std::fs::create_dir_all(root.join(entry)).unwrap();
        }
        root
    }

    #[test]
    fn a_window_reads_every_numbered_minor_and_nothing_else() {
        let root = v1_18_0_deploy();
        let w = DeployWindow::read(&root).unwrap();
        assert_eq!(w.0, BTreeSet::from([34, 35]));
        assert_eq!(w.to_string(), "1.34-1.35");
        std::fs::remove_dir_all(root.parent().unwrap()).unwrap();
    }

    #[test]
    fn a_deploy_dir_holding_no_minor_is_a_named_error() {
        let root = std::env::temp_dir().join(format!("ztest-csi-empty-{}", std::process::id()));
        std::fs::create_dir_all(root.join("util")).unwrap();
        assert!(DeployWindow::read(&root).unwrap_err().to_string().contains("no kubernetes minor"));
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn an_in_window_minor_selects_its_own_dir() {
        assert_eq!(window([34, 35]).select(34), Deploy::Exact(34));
        assert_eq!(window([34, 35]).select(35), Deploy::Exact(35));
    }

    #[test]
    fn a_server_past_the_window_falls_through_to_latest() {
        assert_eq!(window([34, 35]).select(36), Deploy::Latest);
        assert_eq!(window([34, 35]).select(99), Deploy::Latest);
    }

    /// v1.17.1 shipped 1.30/1.31, v1.18.0 jumps to 1.34 — skipped minors resolve, not error
    #[test]
    fn a_minor_upstream_skipped_falls_through_to_latest() {
        assert_eq!(window([30, 34]).select(32), Deploy::Latest);
    }

    #[test]
    fn a_server_below_the_window_is_unsupported() {
        assert_eq!(window([34, 35]).select(33), Deploy::Unsupported);
    }

    // ── The line the operator actually reads ────────────────────────────────

    fn container(name: &str, ready: bool, waiting: Option<&str>) -> ContainerStatus {
        ContainerStatus {
            name: name.into(),
            image: "img".into(),
            image_id: String::new(),
            ready,
            restart_count: 0,
            state: waiting.map(|reason| ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some(reason.into()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn pod_status(containers: Vec<ContainerStatus>) -> PodStatus {
        PodStatus {
            phase: Some("Pending".into()),
            container_statuses: Some(containers),
            ..Default::default()
        }
    }

    #[test]
    fn a_note_counts_ready_then_names_the_first_laggard() {
        let s = pod_status(vec![
            container("hostpath", true, None),
            container("csi-provisioner", false, Some("ImagePullBackOff")),
            container("csi-resizer", false, Some("PodInitializing")),
        ]);
        assert_eq!(note(&s), "1/3 ready · csi-provisioner: ImagePullBackOff");
    }

    #[test]
    fn a_note_drops_the_laggard_clause_once_every_container_is_ready() {
        let s = pod_status(vec![container("hostpath", true, None)]);
        assert_eq!(note(&s), "1/1 ready");
    }

    /// Statuses land a beat after the pod does — the phase is all there is to say
    #[test]
    fn a_note_falls_back_to_the_phase_before_any_container_reports() {
        assert_eq!(note(&pod_status(vec![])), "Pending");
        assert_eq!(note(&PodStatus::default()), "Pending");
    }

    #[test]
    fn a_note_names_a_laggard_that_reports_no_waiting_reason() {
        let s = pod_status(vec![container("hostpath", false, None)]);
        assert_eq!(note(&s), "0/1 ready · hostpath");
    }

    fn report(blocked: &[&'static str]) -> Report {
        with(blocked, |_| Finding::Absent("gone".into()))
    }

    fn with(blocked: &[&'static str], finding: fn(&str) -> Finding) -> Report {
        let cap = |name: &'static str| Capability {
            name,
            need: Need::Required,
            finding: finding(name),
            remedy: "docs/ops-cluster-requirements.md",
        };
        Report { capabilities: blocked.iter().copied().map(cap).collect() }
    }

    #[test]
    fn a_cluster_with_no_snapshot_storage_is_offered_the_install() {
        assert!(repairs_the_gap(&report(&[capability::STORAGE, capability::SNAPSHOT_API])));
    }

    /// The regression: installing here steals the default StorageClass from a driver that
    /// works, because its controller happened to be mid-restart
    #[test]
    fn a_restarting_controller_beside_working_storage_is_never_offered_an_install() {
        assert!(!repairs_the_gap(&report(&[capability::SNAPSHOT_CONTROLLER])));
    }

    #[test]
    fn a_capable_cluster_is_offered_nothing() {
        assert!(!repairs_the_gap(&report(&[])));
    }

    /// An unreachable cluster reports every storage row unreadable; installing on that
    /// reading would steal the default StorageClass from a driver nobody could see
    #[test]
    fn an_unreadable_cluster_is_never_offered_an_install() {
        let unknown = with(&[capability::STORAGE, capability::SNAPSHOT_API], |_| {
            Finding::Unknown("client error (Connect)".into())
        });
        assert!(!repairs_the_gap(&unknown));
    }

    /// A gap this install cannot close means the offer would be a lie
    #[test]
    fn an_unrelated_blocker_withdraws_the_offer() {
        assert!(!repairs_the_gap(&report(&[capability::STORAGE, "host toolchain"])));
    }
}
