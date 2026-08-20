//! Laptop-side runner-image bake, for clusters with no on-cluster build target.
//!
//! - Same [`docker/runner.Dockerfile`] recipe as on-cluster, a host engine build
//!   instead of `buildctl`-in-pod; only *where* differs
//! - Same [`remote_compile::assemble_outcome`], same in-image binary paths
//! - Reaches the cluster like any `dev!` image
//!   ([`crate::backends::image::from_env`]): registry push, else kind side-load
//!
//! [`docker/runner.Dockerfile`]: remote_compile::RUNNER_DOCKERFILE

use std::time::Instant;

use crate::pipeline::remote_compile::{
    self, Phase, PhaseSink, RUNNER_DOCKERFILE, RemoteCompileOutcome, SourceLayout, TempDir,
};
use crate::proc::{self, ChildHost};
use crate::runtime;

/// `list_args` = the `cargo nextest list` argv every path passes; `run_id` tags
/// the image
pub async fn bake_locally(
    list_args: &[String],
    run_id: &str,
    host: Option<&dyn ChildHost>,
    on_phase: Option<PhaseSink<'_>>,
) -> Result<RemoteCompileOutcome, crate::error::PipelineError> {
    let mut on_phase = on_phase;
    let mut emit = |ev: Phase<'_>| {
        if let Some(cb) = on_phase.as_deref_mut() {
            cb(ev);
        }
    };

    // Staged, not pointed at the source ancestor: staging applies the git file
    // selection (else multi-GB chain archives get tarred in)
    let src = SourceLayout::resolve()?;
    let stage = TempDir::new("ztest-ctx")?;
    let ctx = stage.path().join("ctx");
    let dockerfile = stage.path().join("Dockerfile");
    std::fs::create_dir_all(&ctx).map_err(|e| format!("create build context dir: {e}"))?;
    emit(Phase::Start("staging the build context"));
    let t = Instant::now();
    remote_compile::extract_source_to(&src, &ctx)?;
    std::fs::write(&dockerfile, RUNNER_DOCKERFILE)
        .map_err(|e| format!("write runner Dockerfile: {e}"))?;
    emit(Phase::Done { label: "build context staged", dur: t.elapsed() });

    let tag = format!("{}:dev-{run_id}", crate::naming::RUNNER_REPO);
    let reference = crate::backends::image::pod_reference(&tag);
    let workspace_rel = src.workspace_rel.to_string_lossy().into_owned();
    // Quoted per arg, not joined: the Dockerfile `eval`s NEXTEST_ARGS, so `-E test(=x)`
    // unquoted arrives as broken tokens & silently selects nothing
    let nextest_args =
        list_args.iter().map(|a| remote_compile::shell_quote(a)).collect::<Vec<_>>().join(" ");
    let common = |target: &str| -> Vec<String> {
        vec![
            "build".into(),
            "--target".into(),
            target.into(),
            "--build-arg".into(),
            format!("NEXTEST_ARGS={nextest_args}"),
            "--build-arg".into(),
            format!("WORKSPACE_REL={workspace_rel}"),
            "-f".into(),
            dockerfile.to_string_lossy().into_owned(),
        ]
    };
    // docker: cache mounts + heredocs need BuildKit; podman: short-name `FROM` parity
    let envs = runtime::active().build_envs();

    emit(Phase::Start("building the runner image"));
    let t = Instant::now();
    let mut argv = common("runner");
    argv.extend(["-t".into(), reference.clone()]);
    argv.push(ctx.to_string_lossy().into_owned());
    engine(host, &argv, &envs, "runner build").await?;
    emit(Phase::Done { label: "runner image built", dur: t.elapsed() });

    emit(Phase::Start("dumping test inventory"));
    let t = Instant::now();
    let inv = stage.path().join("inv");
    let mut argv = common("inventory-export");
    argv.extend(["--output".into(), format!("type=local,dest={}", inv.to_string_lossy())]);
    argv.push(ctx.to_string_lossy().into_owned());
    engine(host, &argv, &envs, "inventory export").await?;
    let list_json = std::fs::read_to_string(inv.join("list.json"))
        .map_err(|e| format!("read exported list.json: {e}"))?;
    let inventory = std::fs::read_to_string(inv.join("inventory.jsonl"))
        .map_err(|e| format!("read exported inventory.jsonl: {e}"))?;
    let outcome =
        remote_compile::assemble_outcome(&list_json, &inventory, &src.ancestor, reference.clone())?;
    emit(Phase::Done { label: "inventory dumped", dur: t.elapsed() });

    emit(Phase::Start("publishing the runner image"));
    let t = Instant::now();
    publish(host, &tag, &reference).await?;
    emit(Phase::Done { label: "runner image published", dur: t.elapsed() });
    emit(Phase::Note(&format!("runner image ready: {reference}")));
    Ok(outcome)
}

/// Registry push, else kind side-load — same choice
/// [`crate::backends::image::from_env`] makes for `dev!`
async fn publish(
    host: Option<&dyn ChildHost>,
    tag: &str,
    reference: &str,
) -> Result<(), crate::error::PipelineError> {
    use crate::backends::image::{docker as docker_backend, kind};

    if crate::backends::image::registry_configured() {
        let argv = docker_backend::docker_push_argv(reference);
        return engine(host, &argv, &[], "runner push").await;
    }
    tokio::task::spawn_blocking(kind::ensure_kind_cluster)
        .await
        .map_err(|e| format!("kind preflight: {e}"))?
        .map_err(|e| e.to_string())?;
    kind::side_load(host, runtime::active(), tag).await?;
    kind::confirm_loaded(tag).await.map_err(|e| e.to_string().into())
}

async fn engine(
    host: Option<&dyn ChildHost>,
    argv: &[String],
    envs: &[(&str, String)],
    step: &str,
) -> Result<(), crate::error::PipelineError> {
    proc::run_checked(host, runtime::program(), argv, envs, step).await
}
