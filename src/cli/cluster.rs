//! `ztest cluster` — manage named cluster profiles.
//!
//! A profile binds a kube-context, image distribution, and the OpenShift flag
//! under one name; `ztest run --cluster <name>` (or the persisted default)
//! selects all three at once. See [`crate::cluster_config`] for the store and
//! the selection precedence.

use std::process::ExitCode;

use clap::{Args as ClapArgs, Subcommand};

use crate::cluster_config::{self, Config, ImageBackend, Profile};

#[derive(Debug, ClapArgs)]
pub struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
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
/// registry config — OpenShift or generic — come from the file itself: its
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

    /// Also make this the active default.
    #[arg(long)]
    set_default: bool,
}

pub fn execute(args: Args) -> ExitCode {
    let result = match args.cmd {
        Cmd::List => list(),
        Cmd::Current => current(),
        Cmd::Add(a) => add(a),
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
        let marker = if cfg.current.as_deref() == Some(name.as_str()) {
            "*"
        } else {
            " "
        };
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

fn add(a: AddArgs) -> Result<(), String> {
    // Bare `--kind` adopts the profile name as the cluster name; `--kind X`
    // overrides it. Context is derived below (kind's is always `kind-<cluster>`).
    let kind_cluster = a.kind.map(|k| k.unwrap_or_else(|| a.name.clone()));

    // kind's context is always `kind-<cluster>`; a remote's context and registry
    // config (generic, or the OpenShift push-route/pull-service split) both come
    // from the kubeconfig — the file's current-context and its `ztest.io/registry`
    // extension. Nothing about distribution is typed on the command line.
    let mut push = None;
    let mut pull = None;
    let mut backend = ImageBackend::Kind;
    let mut context = kind_cluster.as_ref().map(|k| format!("kind-{k}"));
    if kind_cluster.is_none()
        && let Some(kc) = &a.kubeconfig
    {
        let material = cluster_config::read_material(Some(std::path::Path::new(kc)), None)?;
        if let Some(spec) = material.registry {
            push = Some(spec.push);
            // The integrated-registry extension carries a distinct in-cluster
            // pull address only for OpenShift; a generic registry pushes and
            // pulls one address, so the pull is folded into `push`.
            backend = if spec.openshift {
                pull = Some(spec.pull);
                ImageBackend::OpenShift
            } else {
                ImageBackend::Registry
            };
        }
        // Record the resolved context so the profile is self-describing and
        // `ztest run` can verify it, rather than leaning on the file's
        // current-context staying put.
        context = material.context;
    }

    let profile = Profile {
        context,
        kubeconfig: a.kubeconfig,
        push,
        pull,
        kind_cluster,
        backend,
    };
    profile.validate()?;

    let mut cfg = cluster_config::load()?;
    let first = cfg.clusters.is_empty();
    let existed = cfg
        .clusters
        .insert(a.name.clone(), profile.clone())
        .is_some();
    // The first profile becomes the default even without --set-default: with a
    // single profile there is no ambiguity, and it saves a second command.
    if a.set_default || first {
        cfg.current = Some(a.name.clone());
    }
    cfg.save()?;

    let verb = if existed { "updated" } else { "added" };
    println!("{verb} `{}`  ({})", a.name, profile.summary());
    if cfg.current.as_deref() == Some(a.name.as_str()) {
        println!("`{}` is now the default", a.name);
    }
    Ok(())
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
    if names.is_empty() {
        "(none)".to_string()
    } else {
        names.join(", ")
    }
}
