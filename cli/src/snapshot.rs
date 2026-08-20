//! `ztest snapshot`: publishing chain archives, and the seed cache in `ztest-seeds`.
//!
//! - `manifest` derives an archive's four-key record, `push` uploads it under its own
//!   sha256, `verify` asserts every declared snapshot resolves in the bucket. All three
//!   are cluster-free — publishing a fixture must not need a cluster up
//! - Seed = `seed-<sha8>-<driver>` PVC filled once from the bucket + paired
//!   `VolumeSnapshot`; tests clone it copy-on-write (`materialize.rs` / `seeds.rs`)
//! - Keyed on content *and* driver → `list` reports `DRIVER this|other` and seeds
//!   for a driver this cluster no longer uses are inert, never selected
//! - `list` inspects, `prune` reclaims, `warm` pre-populates without a test run

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use kube::api::{Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams};
use kube::{Client, ResourceExt};

use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod};
use ztest::api::progress::StepProgress as _;
use ztest::api::seeds::SEEDS_NAMESPACE;
use ztest_ui::template::{Fields, draw};
use ztest_ui::{Theme, TransferKind};

use crate::progress::LiveStep;

const READY_LABEL: &str = "seeds.ztest.io/ready";
const DRIVER_LABEL: &str = "seeds.ztest.io/driver";
const SEED_PREFIX: &str = "seed-";

/// Seed column bounds (driver slug runs a name out to `DNS_LABEL_MAX`)
const SEED_COL_MIN: usize = 24;
const SEED_COL_MAX: usize = 44;

mod tmpl {
    pub(super) const NOTE: &str = "{note|dim}";
    pub(super) const PUSH_RESULT: &str = "{verb} lfs/{oid} {@dot|dim} {size|bytes.bold}";
    pub(super) const WARMING: &str = "{entry|dim} warming seed {sha|bold} {@dot|dim} {name}";
    pub(super) const READY: &str = "{@ok|pass} {name}";
    pub(super) const PRUNED: &str = "{@ok|pass} pruned {name}";
    pub(super) const PRUNED_ORPHAN: &str = "{@ok|pass} pruned orphan {name}";
    pub(super) const PRUNE_ERROR: &str = "  {@fail|fail} {detail|dim}";
    pub(super) const VERIFY_TALLY: &str = "{count|bold} snapshots, all present";

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

/// `{ok} … {name}` row (pruned / pruned orphan / ready differ in wording alone)
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

/// Non-fatal sweep failure (`prune` carries on → reports, never returns)
fn prune_error(detail: &str, theme: &Theme) {
    say_err(tmpl::PRUNE_ERROR, Fields::new().text("detail", detail), theme);
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

    /// Pre-materialize one or more local archives into seeds without
    /// running a test.
    Warm(WarmArgs),

    /// Derive an archive's manifest and print it to stdout. Local only —
    /// no cluster, no bucket, no network.
    Manifest(ManifestArgs),

    /// Upload an archive to the snapshot bucket under its content address.
    /// Idempotent: identical bytes are already there under the same key.
    Push(PushArgs),

    /// Assert every declared snapshot resolves to an object in the bucket.
    /// A committed manifest is a claim the bytes exist; this is what checks it.
    Verify,
}

#[derive(Debug, Parser)]
struct ManifestArgs {
    /// Archive to describe. Read once, streaming: hashed on the way into the
    /// decompressor, whose output is counted for the extracted size.
    archive: PathBuf,
}

#[derive(Debug, Parser)]
struct PushArgs {
    /// Archive to upload. Its SHA-256 is the object key, so publishing the
    /// same bytes twice is a no-op.
    archive: PathBuf,
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

#[derive(Debug, Parser)]
struct WarmArgs {
    /// Archive file(s) to materialize. Treated as compressed tar
    /// archives (extracted into the seed); content-addressed by hash.
    #[arg(required = true)]
    archives: Vec<PathBuf>,
}

pub fn execute(args: Args) -> ExitCode {
    super::block_on("snapshot", super::Rt::Current, async {
        // `manifest` touches only the file; `push` needs the bucket, not a cluster.
        // Connecting first would make publishing a fixture require a cluster to be up
        match args.cmd {
            SnapshotCmd::Manifest(m) => return manifest(&m),
            SnapshotCmd::Push(p) => return push(&p).await,
            SnapshotCmd::Verify => return verify().await,
            _ => {}
        }
        let client = ztest::api::cluster::client()
            .await
            .map_err(|e| format!("connecting to cluster: {e}"))?;
        match args.cmd {
            SnapshotCmd::List => list(&client).await,
            SnapshotCmd::Prune(p) => prune(&client, &p).await,
            SnapshotCmd::Warm(w) => warm(&client, &w).await,
            SnapshotCmd::Manifest(_) | SnapshotCmd::Push(_) | SnapshotCmd::Verify => {
                unreachable!("handled above")
            }
        }
    })
}

/// Derive `archive`'s manifest and print it.
///
/// stdout carries the TOML alone, so it redirects straight into the tree; where it belongs
/// goes to stderr, because a hand-picked filename disagreeing with the content is exactly
/// what the declaration exists to prevent
fn manifest(args: &ManifestArgs) -> Result<(), String> {
    let digest = ztest::api::storage::digest_of(&args.archive)?;
    let name = args
        .archive
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{} has no filename", args.archive.display()))?;
    print!(
        "# Generated by `ztest snapshot manifest` — do not hand-edit.\n\
         #\n\
         # The archive itself is not in the tree; these values are how it is located,\n\
         # addressed, sized, and verified. Chain facts live at the declaration in\n\
         # src/snapshots.rs.\n\
         name               = {name:?}\n\
         sha256             = {:?}\n\
         size_bytes         = {}\n\
         uncompressed_bytes = {}\n\
         base_uri           = {:?}\n\
         key_prefix         = {:?}\n",
        digest.sha256,
        digest.size_bytes,
        digest.uncompressed_bytes,
        ztest::api::storage::BASE_URI,
        ztest::api::storage::KEY_PREFIX,
    );
    eprintln!(
        "write to snapshots/<network>/zebra-<version>-<upgrade>.toml, then `ztest snapshot push`"
    );
    Ok(())
}

/// Upload `archive` under its own content address. Idempotent by construction — the key
/// *is* the content, so re-pushing identical bytes is a no-op
async fn push(args: &PushArgs) -> Result<(), String> {
    let theme = Theme::detect();
    let label = args
        .archive
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| args.archive.display().to_string());
    // - one row across both phases (hash + upload = one push to a watcher)
    // - stage note marks the crossing
    let step = LiveStep::new(label, TransferKind::Upload);
    let result = push_reporting(args, &step, &theme).await;
    step.finish();
    result
}

async fn push_reporting(args: &PushArgs, step: &LiveStep, theme: &Theme) -> Result<(), String> {
    let digest = ztest::api::storage::digest_of_with(&args.archive, step)?;
    let bucket = ztest::api::storage::Bucket::resolve().map_err(|e| e.to_string())?;
    let result = |verb| {
        say(
            tmpl::PUSH_RESULT,
            Fields::new()
                .text("verb", verb)
                .text("oid", digest.sha256.as_str())
                .value("size", digest.size_bytes as f64),
            theme,
        )
    };
    if bucket.has(&digest.sha256, digest.size_bytes).await.map_err(|e| e.to_string())? {
        step.finish();
        result("already present:");
        return Ok(());
    }
    let file = tokio::fs::File::open(&args.archive)
        .await
        .map_err(|e| format!("opening {}: {e}", args.archive.display()))?;
    let total = digest.size_bytes;
    let mut sent = 0u64;
    step.note("uploading");
    bucket
        .put(&digest.sha256, total, file, &mut |n| {
            sent += n as u64;
            step.bytes(sent, total);
        })
        .await
        .map_err(|e| e.to_string())?;
    step.finish();
    result("pushed");
    Ok(())
}

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
async fn seed_pvcs(client: &Client) -> Result<Vec<PersistentVolumeClaim>, String> {
    let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), SEEDS_NAMESPACE);
    let list =
        api.list(&ListParams::default()).await.map_err(|e| format!("listing seed PVCs: {e}"))?;
    Ok(list.items.into_iter().filter(|p| p.name_any().starts_with(SEED_PREFIX)).collect())
}

async fn list(client: &Client) -> Result<(), String> {
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

async fn prune(client: &Client, args: &PruneArgs) -> Result<(), String> {
    let theme = Theme::detect();
    if !args.all && args.shas.is_empty() {
        return Err("nothing selected — pass `--all` or one or more seed sha8 prefixes.".into());
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
            Err(e) => prune_error(&format!("uploader pod {uploader}: {e}"), &theme),
        }
        // Snapshot next → its content releases before the PVC
        match snap_api.delete(name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(format!("deleting VolumeSnapshot {name}: {e}")),
        }
        match pvc_api.delete(name, &dp).await {
            Ok(_) => {}
            Err(kube::Error::Api(e)) if e.code == 404 => {}
            Err(e) => return Err(format!("deleting PVC {name}: {e}")),
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
                    Err(e) => prune_error(&format!("{n}: {e}"), &theme),
                }
            }
        }
    }
    Ok(())
}

/// Pre-provision seeds for the named archives.
///
/// Archive file never opened: identity from the sidecar manifest, bytes from the
/// bucket → works in a checkout holding only the manifest (same property that lets
/// a build pod declare a seed it cannot read)
async fn warm(client: &Client, args: &WarmArgs) -> Result<(), String> {
    let theme = Theme::detect();
    for archive in &args.archives {
        let (name, digest) = ztest::archive::identity_of(archive)?;
        let entry = ztest::api::inventory::SeedEntry {
            name,
            oid: digest.sha256,
            size: digest.size_bytes,
            uncompressed_bytes: digest.uncompressed_bytes,
            payload: ztest::api::inventory::SeedPayload::Archive,
            base_uri: ztest::api::storage::BASE_URI.to_string(),
            key_prefix: ztest::api::storage::KEY_PREFIX.to_string(),
        };
        say_err(
            tmpl::WARMING,
            Fields::new()
                .text("entry", theme.chars.entry)
                .text("sha", ztest::api::storage::seed_sha8(&entry.oid))
                .text("name", entry.name.as_str()),
            &theme,
        );
        // Name comes back on the handle: only `provision_seed` knows the driver half
        let step = LiveStep::new(entry.name.clone(), TransferKind::Seed);
        let handle = ztest::api::materialize::provision_seed(client, &entry, &step).await;
        step.finish();
        let handle = handle.map_err(|e| format!("materializing {}: {e}", entry.name))?;
        ok_line(tmpl::READY, &handle.seed_pvc, &theme);
    }
    Ok(())
}

/// Every declared snapshot's bytes must be in the bucket.
///
/// The lockfile integrity check: committing a manifest claims an object exists, and
/// nothing else enforces that the `push` happened. `HEAD` per object, no transfer
async fn verify() -> Result<(), String> {
    let theme = Theme::detect();
    let bucket = ztest::api::storage::Bucket::resolve().map_err(|e| e.to_string())?;
    let mut missing = 0usize;
    for snapshot in ztest::snapshots::ALL {
        let a = &snapshot.artifact;
        let present = bucket.has(a.oid, a.size).await.map_err(|e| e.to_string())?;
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
    match missing {
        0 => {
            let count = ztest::api::thousands(ztest::snapshots::ALL.len() as u64);
            println!();
            say(tmpl::VERIFY_TALLY, Fields::new().text("count", count), &theme);
            Ok(())
        }
        n => Err(format!(
            "{n} of {} declared snapshots are absent from the bucket — push before \
             committing the manifest",
            ztest::snapshots::ALL.len(),
        )),
    }
}

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
}
