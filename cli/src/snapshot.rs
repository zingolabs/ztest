//! `ztest snapshot`: publishing chain snapshots, and the seed cache in `ztest-seeds`.
//!
//! - `push` = thin shim over `rclone` (hash tree → SHA256SUMS → upload files, sums last)
//! - `verify` asserts every declared snapshot resolves. Both are cluster-free
//! - Seed = `seed-<sha8>-<driver>` PVC filled once from the bucket + paired
//!   `VolumeSnapshot`; tests clone it copy-on-write (`materialize.rs` / `seeds.rs`)
//! - Keyed on content *and* driver → `list` reports `DRIVER this|other` and seeds
//!   for a driver this cluster no longer uses are inert, never selected
//! - `list` inspects, `prune` reclaims

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context as _, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams};
use kube::{Client, ResourceExt};

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use ztest::api::seeds::SEEDS_NAMESPACE;
use ztest::api::storage::{KEY_PREFIX, SUMS_FILE, StorageError};
use ztest_ui::Theme;
use ztest_ui::template::{Fields, draw};

const READY_LABEL: &str = "seeds.ztest.io/ready";
const DRIVER_LABEL: &str = "seeds.ztest.io/driver";
const SEED_PREFIX: &str = "seed-";

/// Seed column bounds (driver slug runs a name out to `DNS_LABEL_MAX`)
const SEED_COL_MIN: usize = 24;
const SEED_COL_MAX: usize = 44;

mod tmpl {
    pub(super) const NOTE: &str = "{note|dim}";
    pub(super) const PUSH_RESULT: &str = "pushed snap/{oid} {@dot|dim} {size|bytes.bold}";
    pub(super) const PRUNED: &str = "{@ok|pass} pruned {name}";
    pub(super) const PRUNED_ORPHAN: &str = "{@ok|pass} pruned orphan {name}";
    pub(super) const FAIL_DETAIL: &str = "  {@fail|fail} {detail|dim}";
    pub(super) const VERIFY_TALLY: &str =
        "{count|bold} snapshots, all present · read path {endpoints|bold} sound";

    /// Endpoint row: one per distinct `base_uri`, not per artifact
    pub(super) fn endpoint_row(tone: &str) -> String {
        format!("{{mark:<4|{tone}}} {{base}}  {{detail|dim}}")
    }

    /// - Header + body = one shape, tone apart (columns cannot drift)
    /// - `[{size}][{size_raw}]` = exactly one binds (parsed `Quantity`, else its raw text)
    pub(super) fn list_row(seed: usize, tone: &str) -> String {
        format!(
            "{{seed:<{seed}|{tone}}} {{ready:<5|{tone}}} [{{size:>9|bytes.{tone}}}]\
             [{{size_raw:>9|{tone}}}] {{driver:<6|{tone}}} {{snap|{tone}}}"
        )
    }

    pub(super) fn verify_row(tone: &str) -> String {
        format!("{{mark:<4|{tone}}} {{oid|dim}}  {{name}}")
    }
}

/// Bind and draw (no `*` cell, no spinner here → zero width, zero elapsed)
/// Result lines → stdout (caller redirects it), everything else → [`say_err`]
fn say(src: &str, f: Fields<'_>, theme: &Theme) {
    println!("{}", draw(src, &f, theme));
}

fn say_err(src: &str, f: Fields<'_>, theme: &Theme) {
    eprintln!("{}", draw(src, &f, theme));
}

fn note(text: &str, theme: &Theme) {
    say(tmpl::NOTE, Fields::new().text("note", text), theme);
}

/// `{ok} … {name}` row (pruned / pruned orphan differ in wording alone)
fn ok_line(src: &str, name: &str, theme: &Theme) {
    say(src, Fields::new().text("name", name), theme);
}

/// Column captions, drawn through the body's own template (→ no drift)
fn header_fields() -> Fields<'static> {
    Fields::new()
        .text("seed", "SEED")
        .text("ready", "READY")
        .text("size_raw", "SIZE")
        .text("driver", "DRIVER")
        .text("snap", "SNAPSHOT")
}

/// Non-fatal failure (`prune`/`verify` carry on → report, never return)
fn fail_detail(detail: &str, theme: &Theme) {
    say_err(tmpl::FAIL_DETAIL, Fields::new().text("detail", detail), theme);
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    cmd: SnapshotCmd,
}

#[derive(Debug, Subcommand)]
enum SnapshotCmd {
    /// List the seed PVCs and their snapshot/ready state.
    List,

    /// Delete cached seeds (PVCs + paired VolumeSnapshots) and any
    /// orphaned cluster-scoped seed-binding VolumeSnapshotContents.
    Prune(PruneArgs),

    /// Publish a snapshot directory to the bucket and print its manifest.
    ///
    /// Hashes every file under DIR into a SHA256SUMS (the sha256 of that file is the
    /// snapshot's id), uploads the files to `ztest-seeds:snap/<id>/`, uploads SHA256SUMS
    /// last, then prints the manifest TOML to stdout. Redirect it into
    /// `snapshots/<network>/<name>.toml` and commit it; progress goes to stderr.
    ///
    /// Needs `rclone` on PATH with a `ztest-seeds` remote (setup:
    /// docs/design-snapshots.md#publishing). Safe to rerun: files already uploaded are
    /// skipped, so an interrupted push resumes where it stopped.
    Push(PushArgs),

    /// Check every declared snapshot is published and readable without credentials:
    /// its SHA256SUMS must exist on the public read path and hash to the manifest's
    /// `sha256`. Then check each read endpoint serves seed keys only and refuses writes.
    Verify,
}

#[derive(Debug, Parser)]
struct PushArgs {
    /// Snapshot directory, e.g. a synced zebra state cache. Every file under it is
    /// published; paths may only use `A-Z a-z 0-9 . _ -` and `/`.
    dir: PathBuf,

    /// Snapshot name recorded in the manifest, e.g. `zebra-6.2.3-orchard-testnet`.
    #[arg(long)]
    name: String,
}

#[derive(Debug, Parser)]
struct PruneArgs {
    /// Delete every seed in the cache.
    #[arg(long)]
    all: bool,

    /// Specific seed sha8 prefixes to delete (e.g. `4c86ea3c`). The
    /// `seed-` prefix is optional.
    shas: Vec<String>,
}

pub fn execute(args: Args) -> ExitCode {
    super::block_on("snapshot", super::Rt::Current, async {
        // `push` touches only the bucket, `verify` only the public read path: neither needs
        // a cluster (connecting first would make publishing require one to be up)
        match args.cmd {
            SnapshotCmd::Push(p) => return push(&p).await,
            SnapshotCmd::Verify => return verify().await,
            _ => {}
        }
        let client = ztest::api::cluster::client().await.context("connecting to cluster")?;
        match args.cmd {
            SnapshotCmd::List => list(&client).await,
            SnapshotCmd::Prune(p) => prune(&client, &p).await,
            SnapshotCmd::Push(_) | SnapshotCmd::Verify => unreachable!("handled above"),
        }
    })
}

// ─────────────────────────── push ───────────────────────────

/// rclone alias → `<r2-remote>:<bucket>`; ztest holds no credentials, rclone's config does
const REMOTE: &str = "ztest-seeds";
const SETUP_DOC: &str = "docs/design-snapshots.md#publishing";

/// Multi-hour uploads over a flaky link (rerun = resume: rclone skips what landed)
const UPLOAD_FLAGS: &[&str] = &[
    "--transfers",
    "8",
    "--checkers",
    "16",
    "--retries",
    "20",
    "--retries-sleep",
    "30s",
    "--low-level-retries",
    "100",
    "--timeout",
    "5m",
    "--contimeout",
    "1m",
    "--stats",
    "30s",
    "--stats-one-line",
    "--stats-log-level",
    "NOTICE",
];

/// Hash, upload files, upload SHA256SUMS last, print the manifest (stdout = TOML only)
async fn push(args: &PushArgs) -> Result<()> {
    let theme = Theme::detect();
    if !args.dir.is_dir() {
        bail!("{} is not a directory", args.dir.display());
    }
    if !valid_segment(&args.name) {
        bail!("--name {:?}: use only A-Z a-z 0-9 . _ -", args.name);
    }
    let scratch = Scratch::new()?;
    let hashsum = scratch.0.join("hashsum");
    note_err("hashing", &theme);
    rclone(&["hashsum", "sha256", "--checkers", "16", "--output-file"], &[&hashsum, &args.dir])
        .await?;
    let listed = std::fs::read_to_string(&hashsum).context("reading rclone hashsum output")?;
    let sums = Sums::parse(&listed)?;
    let size = sums.tree_size(&args.dir)?;
    let text = sums.text();
    let oid = ztest::api::storage::oid_of(text.as_bytes());
    let (sums_path, files) = (scratch.0.join(SUMS_FILE), scratch.0.join("files"));
    std::fs::write(&sums_path, &text).context("writing SHA256SUMS")?;
    std::fs::write(&files, sums.relpaths()).context("writing file list")?;

    let dest = format!("{REMOTE}:{KEY_PREFIX}/{oid}");
    note_err(&format!("uploading {} files to {dest}", sums.0.len()), &theme);
    // Exactly the hashed set (a file added since hashing never publishes)
    let copy = [&["copy"][..], UPLOAD_FLAGS, &["--files-from-raw"]].concat();
    rclone(&copy, &[&files, &args.dir, Path::new(&dest)]).await?;
    let copyto = [&["copyto"][..], UPLOAD_FLAGS].concat();
    rclone(&copyto, &[&sums_path, Path::new(&format!("{dest}/{SUMS_FILE}"))]).await?;

    say_err(
        tmpl::PUSH_RESULT,
        Fields::new().text("oid", oid.as_str()).value("size", size as f64),
        &theme,
    );
    print!("{}", manifest_toml(&args.name, &oid, size));
    Ok(())
}

fn note_err(text: &str, theme: &Theme) {
    say_err(tmpl::NOTE, Fields::new().text("note", text), theme);
}

/// The record `artifact!` bakes; `sha256` = oid of the tree's SHA256SUMS
fn manifest_toml(name: &str, oid: &str, size_bytes: u64) -> String {
    format!(
        "# Generated by `ztest snapshot push` — do not hand-edit\n\
         name       = {name:?}\n\
         sha256     = {oid:?}\n\
         size_bytes = {size_bytes}\n\
         base_uri   = {:?}\n\
         key_prefix = {KEY_PREFIX:?}\n",
        ztest::api::storage::BASE_URI,
    )
}

/// `rclone <args> <paths>`: stdout → our stderr (stdout carries the manifest alone)
async fn rclone(args: &[&str], paths: &[&Path]) -> Result<()> {
    let status = tokio::process::Command::new("rclone")
        .args(args)
        .args(paths)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(std::io::stderr()))
        .status()
        .await
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => {
                anyhow!("rclone not found on PATH — install it and set up `{REMOTE}` ({SETUP_DOC})")
            }
            _ => anyhow!("spawning rclone: {e}"),
        })?;
    match status.success() {
        true => Ok(()),
        false => Err(anyhow!(
            "`rclone {}` failed ({status}) — is the `{REMOTE}` remote configured? ({SETUP_DOC})",
            args[0]
        )),
    }
}

/// Temp dir for rclone's hashsum output, SHA256SUMS and the file list; removed on drop
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Result<Self> {
        let dir = std::env::temp_dir().join(format!("ztest-push-{}", std::process::id()));
        std::fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
        Ok(Scratch(dir))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// SHA256SUMS lines `(sha256 hex, relpath)`, bytewise-sorted by relpath
#[derive(Debug, PartialEq, Eq)]
struct Sums(Vec<(String, String)>);

impl Sums {
    /// `rclone hashsum sha256` output → the canonical line set.
    ///
    /// - relpaths = Worker-servable keys only (`[A-Za-z0-9._-]` segments, no `.`/`..`/empty)
    /// - root `SHA256SUMS` rejected (its key is the checksum file's)
    fn parse(hashsum: &str) -> Result<Sums> {
        let mut lines = Vec::new();
        for line in hashsum.lines().filter(|l| !l.is_empty()) {
            let (hash, rel) = line
                .split_once("  ")
                .with_context(|| format!("unparseable hashsum line {line:?}"))?;
            let hash = hash.to_ascii_lowercase();
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                bail!("{rel}: {hash:?} is not a sha256");
            }
            if !rel.split('/').all(valid_segment) || rel == SUMS_FILE {
                bail!("{rel:?}: not publishable (segments of A-Z a-z 0-9 . _ -, not {SUMS_FILE})");
            }
            lines.push((hash, rel.to_string()));
        }
        if lines.is_empty() {
            bail!("empty tree: nothing to publish");
        }
        lines.sort_by(|a, b| a.1.as_bytes().cmp(b.1.as_bytes()));
        if let Some(w) = lines.windows(2).find(|w| w[0].1 == w[1].1) {
            bail!("{} listed twice", w[0].1);
        }
        Ok(Sums(lines))
    }

    /// Exact coreutils `sha256sum` format (`sha256sum -c` in the puller reads it)
    fn text(&self) -> String {
        self.0.iter().map(|(hash, rel)| format!("{hash}  {rel}\n")).collect()
    }

    fn relpaths(&self) -> String {
        self.0.iter().map(|(_, rel)| format!("{rel}\n")).collect()
    }

    fn tree_size(&self, dir: &Path) -> Result<u64> {
        self.0.iter().try_fold(0u64, |sum, (_, rel)| {
            let len =
                std::fs::metadata(dir.join(rel)).with_context(|| format!("stat {rel}"))?.len();
            Ok(sum + len)
        })
    }
}

fn valid_segment(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

// ─────────────────────────── seed cache ───────────────────────────

fn volume_snapshot_ar() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshot".into(),
    })
}

fn volume_snapshot_content_ar() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind {
        group: "snapshot.storage.k8s.io".into(),
        version: "v1".into(),
        kind: "VolumeSnapshotContent".into(),
    })
}

/// Seed PVCs in the namespace, by `seed-<sha8>` name
async fn seed_pvcs(client: &Client) -> Result<Vec<PersistentVolumeClaim>> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let list = api.list(&ListParams::default()).await.context("listing seed PVCs")?;
    Ok(list.items.into_iter().filter(|p| p.name_any().starts_with(SEED_PREFIX)).collect())
}

async fn list(client: &Client) -> Result<()> {
    let theme = Theme::detect();
    let pvcs = seed_pvcs(client).await?;
    if pvcs.is_empty() {
        note(&format!("no seeds in {SEEDS_NAMESPACE}"), &theme);
        return Ok(());
    }
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &volume_snapshot_ar());
    // Seeds published on another driver still list: they are inert here, not broken,
    // and a run switched back to that driver reuses them
    let ours = ztest::api::storage_class::selected(client)
        .await
        .map(|s| ztest::api::naming::slug(&s.provisioner, ztest::api::naming::DNS_LABEL_MAX))
        .unwrap_or_default();
    let names: Vec<String> = pvcs.iter().map(|p| p.name_any()).collect();
    let seed_w =
        ztest::api::column_width(names.iter().map(String::as_str), SEED_COL_MIN, SEED_COL_MAX);
    say(&tmpl::list_row(seed_w, "dim"), header_fields(), &theme);
    for (pvc, name) in pvcs.iter().zip(&names) {
        let ready = pvc.labels().get(READY_LABEL).map(|v| v == "true").unwrap_or(false);
        // Pre-`driver`-label seeds carry no driver → unknown, not "other"
        let driver = match pvc.labels().get(DRIVER_LABEL) {
            None => "?",
            Some(d) if *d == ours => "this",
            Some(_) => "other",
        };
        let size = pvc
            .spec
            .as_ref()
            .and_then(|s| s.resources.as_ref())
            .and_then(|r| r.requests.as_ref())
            .and_then(|m| m.get("storage"))
            .map(|q| q.0.clone())
            .unwrap_or_else(|| "?".into());
        let size_bytes = ztest::qos::units::parse_mem_bytes_opt(&size);
        let snap = match snap_api.get_opt(name).await {
            Ok(Some(s)) => {
                let bound = s.data["status"]["readyToUse"].as_bool().unwrap_or(false);
                if bound { "ready" } else { "pending" }
            }
            Ok(None) => "missing",
            Err(_) => "?",
        };
        let row = Fields::new()
            .text("seed", name.as_str())
            .text("ready", if ready { "yes" } else { "no" })
            .maybe_value("size", size_bytes.map(|b| b as f64))
            .maybe_text("size_raw", size_bytes.is_none().then_some(size.as_str()))
            .text("driver", driver)
            .text("snap", snap);
        say(&tmpl::list_row(seed_w, ""), row, &theme);
    }
    Ok(())
}

async fn prune(client: &Client, args: &PruneArgs) -> Result<()> {
    let theme = Theme::detect();
    if !args.all && args.shas.is_empty() {
        return Err(anyhow!("nothing selected; pass --all or a sha8 prefix"));
    }
    let pvcs = seed_pvcs(client).await?;
    let targets: Vec<String> = pvcs
        .iter()
        .map(|p| p.name_any())
        .filter(|name| {
            args.all
                || args.shas.iter().any(|s| {
                    let want = s.trim_start_matches(SEED_PREFIX);
                    name.trim_start_matches(SEED_PREFIX).starts_with(want)
                })
        })
        .collect();

    if targets.is_empty() {
        note("no matching seeds to prune", &theme);
        return Ok(());
    }

    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let snap_api: Api<DynamicObject> =
        Api::namespaced_with(client.clone(), SEEDS_NAMESPACE, &volume_snapshot_ar());
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let dp = DeleteParams::default();
    for name in &targets {
        // Leftover uploader pod first: a crashed materialization leaves one mounting
        // the PVC, blocking its delete on the mount finalizer
        let uploader = name.replace(SEED_PREFIX, "uploader-");
        match pod_api.delete(&uploader, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => fail_detail(&format!("uploader pod {uploader}: {e}"), &theme),
        }
        // Snapshot next → its content releases before the PVC
        match snap_api.delete(name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(anyhow!("deleting VolumeSnapshot {name}: {e}")),
        }
        match pvc_api.delete(name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(anyhow!("deleting PVC {name}: {e}")),
        }
        ok_line(tmpl::PRUNED, name, &theme);
    }

    // Orphaned cluster-scoped seed-binding contents (`Retain` → a crashed test leaves
    // them). Matched by name prefix, not label: sweep of last resort, must catch a
    // content whose labels never landed. Always safe — `Retain` means the backend
    // snapshot belongs to the seed, not the binding
    let vsc_api: Api<DynamicObject> = Api::all_with(client.clone(), &volume_snapshot_content_ar());
    if let Ok(vscs) = vsc_api.list(&ListParams::default()).await {
        for vsc in vscs.items {
            let n = vsc.name_any();
            if n.starts_with(ztest::api::seeds::BINDING_PREFIX) {
                match vsc_api.delete(&n, &dp).await {
                    Ok(_) => ok_line(tmpl::PRUNED_ORPHAN, &n, &theme),
                    Err(kube::Error::Api(e)) if e.code == 404 => {}
                    Err(e) => fail_detail(&format!("{n}: {e}"), &theme),
                }
            }
        }
    }
    Ok(())
}

// ─────────────────────────── verify ───────────────────────────

/// Every declared snapshot must be readable on the public read path.
///
/// - Committed manifest = claim the tree exists; nothing else enforces that `push` finished
/// - One GET of SHA256SUMS per snapshot, checked against the oid (no transfer, no credentials)
/// - Then each distinct `base_uri`: non-seed key must 404, a write must be refused
async fn verify() -> Result<()> {
    let theme = Theme::detect();
    let mut missing = 0usize;
    for snapshot in ztest::snapshots::ALL {
        let a = &snapshot.artifact;
        let found =
            ztest::api::storage::sums_present(a.base_uri, a.key_prefix, a.oid, VERIFY_TIMEOUT)
                .await;
        let present = match found {
            Ok(present) => present,
            Err(e @ StorageError::Mismatch { .. }) => {
                fail_detail(&e.to_string(), &theme);
                false
            }
            Err(e) => return Err(e).with_context(|| format!("checking {}", a.name)),
        };
        let (mark, tone) = match present {
            true => (theme.chars.ok, "pass"),
            false => (theme.chars.warn, "fail"),
        };
        say(
            &tmpl::verify_row(tone),
            Fields::new().text("mark", mark).text("oid", a.oid).text("name", a.name),
            &theme,
        );
        missing += usize::from(!present);
    }
    let mut endpoints: Vec<&str> =
        ztest::snapshots::ALL.iter().map(|s| s.artifact.base_uri).collect();
    endpoints.sort_unstable();
    endpoints.dedup();

    println!();
    let mut unsound = 0usize;
    for base in &endpoints {
        let sound = endpoint_is_sound(base).await;
        let (mark, tone, detail) = match &sound {
            Ok(()) => (theme.chars.ok, "pass", "seed keys only, writes refused".to_string()),
            Err(why) => (theme.chars.warn, "fail", why.clone()),
        };
        unsound += usize::from(sound.is_err());
        say(
            &tmpl::endpoint_row(tone),
            Fields::new().text("mark", mark).text("base", *base).text("detail", detail.as_str()),
            &theme,
        );
    }

    match (missing, unsound) {
        (0, 0) => {
            let count = ztest::api::thousands(ztest::snapshots::ALL.len() as u64);
            println!();
            say(
                tmpl::VERIFY_TALLY,
                Fields::new()
                    .text("count", count)
                    .text("endpoints", ztest::api::thousands(endpoints.len() as u64)),
                &theme,
            );
            Ok(())
        }
        (0, n) => Err(anyhow!("{n}/{} read endpoints unsound", endpoints.len())),
        (n, _) => Err(anyhow!(
            "{n}/{} declared snapshots absent from the bucket or corrupt",
            ztest::snapshots::ALL.len(),
        )),
    }
}

/// Read path serves seeds and nothing else. `Err` carries what it did instead.
///
/// Write probe targets an unused oid, never a declared one (a writable endpoint would
/// otherwise have a real snapshot's SHA256SUMS overwritten by the probe body)
async fn endpoint_is_sound(base: &str) -> Result<(), String> {
    let only_seeds = ztest::api::storage::serves_only_seeds(base, VERIFY_TIMEOUT)
        .await
        .map_err(|e| e.to_string())?;
    if !only_seeds {
        return Err("a non-seed key did not 404 — endpoint exposes the whole bucket".into());
    }
    let scratch = ztest::api::storage::sums_url(base, KEY_PREFIX, &"0".repeat(64));
    let read_only = ztest::api::storage::refuses_writes(&scratch, VERIFY_TIMEOUT)
        .await
        .map_err(|e| e.to_string())?;
    if !read_only {
        return Err(format!("a PUT to {scratch} was accepted — endpoint is not read-only"));
    }
    Ok(())
}

/// Bounded per object: `verify` walks every declared snapshot, and a wrong base_uri hangs
/// on connect rather than failing
const VERIFY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

#[cfg(test)]
mod tests {
    use super::*;

    fn theme() -> Theme {
        Theme::for_capabilities(false, true)
    }

    fn row(size: &str) -> String {
        let bytes = ztest::qos::units::parse_mem_bytes_opt(size);
        let f = Fields::new()
            .text("seed", "seed-4c86ea3c-hostpath")
            .text("ready", "yes")
            .maybe_value("size", bytes.map(|b| b as f64))
            .maybe_text("size_raw", bytes.is_none().then_some(size))
            .text("driver", "this")
            .text("snap", "ready");
        draw(&tmpl::list_row(24, ""), &f, &theme())
    }

    /// - `48Gi` / `51539607552` = one size, one rendering
    /// - unparseable falls back to its raw text, still holding the header's column
    #[test]
    fn a_size_reads_as_bytes_and_falls_back_to_its_raw_quantity() {
        assert_eq!(row("48Gi"), "seed-4c86ea3c-hostpath   yes    48.0 GiB this   ready");
        assert_eq!(row("51539607552"), "seed-4c86ea3c-hostpath   yes    48.0 GiB this   ready");
        assert_eq!(row("?"), "seed-4c86ea3c-hostpath   yes           ? this   ready");
    }

    #[test]
    fn the_header_lands_on_the_body_columns() {
        let head = draw(&tmpl::list_row(24, "dim"), &header_fields(), &theme());
        let column = |s: &str, word: &str| s.find(word);
        assert_eq!(
            column(&head, "SIZE").map(|c| c + 4),
            column(&row("48Gi"), "GiB").map(|c| c + 3)
        );
        assert_eq!(column(&head, "DRIVER"), column(&row("48Gi"), "this"));
    }

    const H1: &str = "98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4";
    const H2: &str = "68a3064ec1d3caa270e21494c5f15f47431188c4d670c894656ba42e40c1ca8d";

    /// rclone lists in walk order; the oid needs one canonical byte string
    #[test]
    fn hashsum_output_normalises_to_bytewise_sorted_sha256sum_lines() {
        let listed = format!("{H1}  x.txt\n{H2}  a/b/big.sst\n{H1}  B.txt\n");
        let sums = Sums::parse(&listed).expect("parses");
        assert_eq!(sums.text(), format!("{H1}  B.txt\n{H2}  a/b/big.sst\n{H1}  x.txt\n"));
        assert_eq!(sums.relpaths(), "B.txt\na/b/big.sst\nx.txt\n");
    }

    /// Same tree, any listing order → same oid
    #[test]
    fn the_oid_does_not_depend_on_listing_order() {
        let a = Sums::parse(&format!("{H1}  a\n{H2}  b/c\n")).expect("a");
        let b = Sums::parse(&format!("{H2}  b/c\n{H1}  a\n")).expect("b");
        assert_eq!(
            ztest::api::storage::oid_of(a.text().as_bytes()),
            ztest::api::storage::oid_of(b.text().as_bytes())
        );
    }

    /// Anything the Worker would refuse to serve (or the puller could mis-cut) never publishes
    #[test]
    fn unservable_relpaths_are_rejected() {
        for rel in ["a//b", "../x", "a/./b", "sp ace", "SHA256SUMS", "a/b/", "é"] {
            assert!(Sums::parse(&format!("{H1}  {rel}\n")).is_err(), "{rel:?} accepted");
        }
        assert!(Sums::parse(&format!("{H1}  sub/SHA256SUMS\n")).is_ok());
    }

    #[test]
    fn an_empty_tree_or_a_bad_digest_is_rejected() {
        assert!(Sums::parse("").is_err());
        assert!(Sums::parse("abc  a\n").is_err());
        assert!(Sums::parse(&format!("{H1}  a\n{H2}  a\n")).is_err(), "duplicate relpath");
    }

    /// `artifact!` reads exactly these keys; `uncompressed_bytes` would fail compilation
    #[test]
    fn the_manifest_carries_exactly_the_artifact_keys() {
        let toml = manifest_toml("zebra-6.2.3-orchard-testnet", H1, 42);
        let doc: toml::Table = toml::from_str(&toml).expect("valid TOML");
        let mut keys: Vec<&str> = doc.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["base_uri", "key_prefix", "name", "sha256", "size_bytes"]);
        assert_eq!(doc["key_prefix"].as_str(), Some(KEY_PREFIX));
        assert_eq!(doc["size_bytes"].as_integer(), Some(42));
    }
}
