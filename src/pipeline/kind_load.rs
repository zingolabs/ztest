//! Phase E — `kind load docker-image` for every built tag.
//!
//! Pushes each freshly-built image from the Docker daemon's local
//! store into the kind node's containerd. Tags marked
//! `already_loaded = true` by Phase D (cache hits) are skipped.
//!
//! `kind load docker-image` itself is sequential per image; running
//! them in parallel just contends on the tar stream into containerd
//! and is no faster. We do them one at a time and emit one stderr
//! line per image so the scroll region tracks progress.

use crate::backends::image;
use crate::pipeline::docker::BuiltImage;

#[derive(Debug, Clone)]
pub enum KindLoadOutcome {
    Loaded { count: usize, skipped: usize },
    Failed { detail: String },
}

/// Load every non-cached image into the kind cluster. Blocking I/O —
/// wrapped in `tokio::task::spawn_blocking` by the caller.
pub fn load_all(images: &[BuiltImage]) -> KindLoadOutcome {
    let mut loaded = 0usize;
    let mut skipped = 0usize;
    for built in images {
        if built.already_loaded {
            skipped += 1;
            continue;
        }
        if let Err(err) = image::kind_load(&built.tag) {
            return KindLoadOutcome::Failed {
                detail: format!("kind load {}: {err}", built.tag),
            };
        }
        loaded += 1;
    }
    KindLoadOutcome::Loaded {
        count: loaded,
        skipped,
    }
}
