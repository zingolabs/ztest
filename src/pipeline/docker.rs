//! Phase D — `docker build` for every discovered dev image.
//!
//! Sequential build (Docker's own BuildKit handles intra-build
//! parallelism; running multiple `docker build`s in parallel just
//! thrashes the daemon). For each declaration we:
//!
//! 1. Compute the content-addressed tag via
//!    [`image::dev_tag`](crate::backends::image::dev_tag).
//! 2. Check whether the image already exists in the kind node's
//!    containerd — if yes, skip the build entirely (warm cache).
//! 3. Otherwise run `docker build -f <dockerfile> -t <tag> <context>`
//!    with stderr inherited so cargo-style progress reaches the
//!    pinned-banner scroll region.
//!
//! Returns the list of `(decl, tag)` pairs the [`kind_load`] phase
//! needs to push into the cluster. Tags that were already in
//! containerd are marked `already_loaded = true` so the load phase
//! can short-circuit those too.

use crate::backends::image;
use crate::inventory::DevImageEntry;

/// One image's outcome from Phase D.
#[derive(Debug, Clone)]
pub struct BuiltImage {
    pub entry: DevImageEntry,
    pub tag: String,
    /// True when the image was already present in containerd at the
    /// start of this run — both `docker build` and `kind load` are
    /// skipped.
    pub already_loaded: bool,
}

#[derive(Debug, Clone)]
pub enum DockerOutcome {
    Built { images: Vec<BuiltImage> },
    Failed { detail: String },
}

/// Build every image in `decls`, skipping anything already present in
/// the cluster's containerd. Blocking I/O — wrapped in
/// `tokio::task::spawn_blocking` by the caller.
pub fn build_all(decls: &[DevImageEntry]) -> DockerOutcome {
    let mut images = Vec::with_capacity(decls.len());
    for decl in decls {
        let dockerfile = std::path::Path::new(&decl.dockerfile);
        let context = std::path::Path::new(&decl.context);

        let tag = match image::dev_tag(dockerfile, context, &decl.features, &decl.repo) {
            Ok(t) => t,
            Err(err) => {
                return DockerOutcome::Failed {
                    detail: format!("hash context for {}: {err}", decl.dockerfile),
                };
            }
        };

        let already = match image::exists_in_kind(&tag) {
            Ok(b) => b,
            Err(err) => {
                return DockerOutcome::Failed {
                    detail: format!("cluster image query for {tag}: {err}"),
                };
            }
        };
        if already {
            images.push(BuiltImage {
                entry: decl.clone(),
                tag,
                already_loaded: true,
            });
            continue;
        }

        if let Err(err) = image::docker_build(dockerfile, context, &decl.features, &tag) {
            return DockerOutcome::Failed {
                detail: format!("docker build {tag}: {err}"),
            };
        }
        images.push(BuiltImage {
            entry: decl.clone(),
            tag,
            already_loaded: false,
        });
    }
    DockerOutcome::Built { images }
}
