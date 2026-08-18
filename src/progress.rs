//! Long-operation progress reporting.
//!
//! - Leaf: work reports *to* this, never reaches back into whatever renders it
//! - `&str` not `impl Into<String>`: the trait is used as `dyn`

pub trait StepProgress: Send + Sync {
    fn note(&self, note: &str);
    fn bytes(&self, done: u64, total: u64);
    fn finalizing(&self);
}

/// Reports nowhere — the test side and non-TTY runs need no second no-op type
#[derive(Debug, Clone, Copy, Default)]
pub struct Silent;

impl StepProgress for Silent {
    fn note(&self, _note: &str) {}
    fn bytes(&self, _done: u64, _total: u64) {}
    fn finalizing(&self) {}
}
