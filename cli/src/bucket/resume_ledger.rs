//! What a killed `ztest snapshot push` leaves behind so the next one starts where it stopped.
//!
//! - One file per oid: header pins the plan (upload id + part size + total), one line per
//!   part as R2 acknowledges it
//! - Append + `sync_data` per part: the ledger exists for the crash case, so a line still
//!   in the page cache is a line that was never written
//! - Cache, not state: deleting it costs a restarted upload, never correctness
//!
//! Local-only by design. S3 `ListParts` is the authoritative recovery — it would let a
//! *second machine* resume — but `object_store` exposes no `list_parts`, and every push
//! ztest makes is from the machine holding the archive.
//!
//! One ledger per oid, unlocked: two pushes of the same archive on one machine interleave
//! on it. Both upload the same content-addressed bytes, so the object is right either way

use std::collections::BTreeMap;
use std::io::Write;

use object_store::multipart::PartId;

use super::{BucketError, UploadPlan};

/// Field separator. Not in an upload id or an ETag, unlike `-` and `:`
const SEP: char = '\t';

pub(super) struct ResumeLedger {
    path: std::path::PathBuf,
    plan: UploadPlan,
    upload_id: Option<String>,
    landed: BTreeMap<usize, String>,
}

impl ResumeLedger {
    /// Ledger for `oid`, resumed when its header describes this same plan.
    ///
    /// - Unreadable or truncated ledger = no ledger (a fresh upload is always correct,
    ///   and this runs before a transfer that costs hours — never fail it on a cache)
    /// - Header naming another part size or total describes a plan R2 would reject on
    ///   completion (uniform-part rule) → dropped rather than resumed
    pub(super) fn open(oid: &str, plan: &UploadPlan) -> Self {
        Self::open_at(ledger_path(oid), plan)
    }

    /// [`Self::open`] against an explicit path. Tests own a directory this way rather than
    /// by setting `XDG_CACHE_HOME`, which is process-global and would race the suite
    fn open_at(path: std::path::PathBuf, plan: &UploadPlan) -> Self {
        let mut ledger = Self { path, plan: *plan, upload_id: None, landed: BTreeMap::new() };
        let Ok(body) = std::fs::read_to_string(&ledger.path) else {
            return ledger;
        };
        // Newline-terminated records only. A push killed mid-append leaves a partial tail,
        // and half an ETag read as a whole one completes the upload with a part id R2 never
        // issued — `lines()` cannot tell the two apart
        let mut records = body
            .split_inclusive('\n')
            .filter(|line| line.ends_with('\n'))
            .map(|line| line.trim_end_matches('\n'));
        let Some(header) = records.next() else {
            return ledger;
        };
        let recorded: Vec<&str> = header.split(SEP).collect();
        if recorded.get(1..) != Some(&[plan.part.to_string().as_str(), &plan.total.to_string()][..])
        {
            return ledger;
        }
        ledger.upload_id = recorded.first().map(|s| s.to_string());
        for line in records {
            if let Some((idx, content_id)) = line.split_once(SEP)
                && let Ok(idx) = idx.parse()
            {
                ledger.landed.insert(idx, content_id.to_string());
            }
        }
        ledger
    }

    pub(super) fn upload_id(&self) -> Option<&str> {
        self.upload_id.as_deref()
    }

    pub(super) fn holds(&self, idx: usize) -> bool {
        self.landed.contains_key(&idx)
    }

    /// Record the plan. Truncates — a new upload id voids every part under the old one
    pub(super) fn begin(&mut self, upload_id: &str) -> Result<(), BucketError> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| self.io("create", e))?;
        }
        let (part, total) = (self.plan.part, self.plan.total);
        std::fs::write(&self.path, format!("{upload_id}{SEP}{part}{SEP}{total}\n"))
            .map_err(|e| self.io("write", e))?;
        self.upload_id = Some(upload_id.to_string());
        self.landed.clear();
        Ok(())
    }

    /// Append one acknowledged part, durable before the caller counts it — a ledger behind
    /// the bucket re-uploads a part (cheap), one ahead completes with an id R2 never
    /// issued (a corrupt object)
    pub(super) fn landed(&mut self, idx: usize, id: &PartId) -> Result<(), BucketError> {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|e| self.io("append to", e))?;
        writeln!(file, "{idx}{SEP}{}", id.content_id).map_err(|e| self.io("append to", e))?;
        file.sync_data().map_err(|e| self.io("sync", e))?;
        self.landed.insert(idx, id.content_id.clone());
        Ok(())
    }

    /// Every part in index order, as `complete_multipart` requires. A gap is a corrupt
    /// object, so it fails here rather than completing
    pub(super) fn parts(&self) -> Result<Vec<PartId>, BucketError> {
        (0..self.plan.count)
            .map(|idx| {
                self.landed
                    .get(&idx)
                    .map(|content_id| PartId { content_id: content_id.clone() })
                    .ok_or_else(|| {
                        BucketError::Bucket(format!(
                            "part {idx} of {} never landed",
                            self.plan.count
                        ))
                    })
            })
            .collect()
    }

    /// Drop the ledger: the upload completed, or its id expired and the next pass starts over
    pub(super) fn forget(&self) -> Result<(), BucketError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(self.io("remove", e)),
        }
    }

    fn io(&self, op: &'static str, source: std::io::Error) -> BucketError {
        BucketError::Io { op, path: self.path.display().to_string(), source }
    }
}

fn ledger_path(oid: &str) -> std::path::PathBuf {
    ztest::api::paths::cache_dir().join("uploads").join(format!("{oid}.ledger"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ledger path this test alone owns. No `XDG_CACHE_HOME`: env is process-global and
    /// the suite runs in parallel
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ztest-ledger-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir.join("push.ledger")
    }

    fn plan(total: u64, part: usize) -> UploadPlan {
        UploadPlan { total, part, count: total.div_ceil(part as u64) as usize }
    }

    fn part(content_id: &str) -> PartId {
        PartId { content_id: content_id.to_string() }
    }

    /// The whole point: parts one pass recorded are skipped by the next, under the same
    /// upload id R2 issued
    #[test]
    fn a_reopened_ledger_resumes_the_same_upload_and_skips_what_landed() {
        let (path, plan) = (scratch("resumes"), plan(4096, 1024));
        let mut first = ResumeLedger::open_at(path.clone(), &plan);
        assert_eq!(first.upload_id(), None, "nothing to resume yet");
        first.begin("upload-1").expect("begin");
        first.landed(0, &part("etag-0")).expect("landed");
        first.landed(2, &part("etag-2")).expect("landed");

        let second = ResumeLedger::open_at(path, &plan);
        assert_eq!(second.upload_id(), Some("upload-1"));
        assert!(second.holds(0) && second.holds(2), "landed parts lost");
        assert!(!second.holds(1) && !second.holds(3), "unsent parts claimed");
    }

    /// R2 rejects a completion whose parts are not uniform, so a ledger written under a
    /// different plan must not be resumed against this one
    #[test]
    fn a_ledger_written_under_a_different_plan_is_not_resumed() {
        let path = scratch("replan");
        let mut ledger = ResumeLedger::open_at(path.clone(), &plan(4096, 1024));
        ledger.begin("upload-1").expect("begin");
        ledger.landed(0, &part("etag-0")).expect("landed");

        let resized = ResumeLedger::open_at(path.clone(), &plan(4096, 2048));
        assert_eq!(resized.upload_id(), None, "resumed across part sizes");
        assert!(!resized.holds(0));

        let regrown = ResumeLedger::open_at(path, &plan(8192, 1024));
        assert_eq!(regrown.upload_id(), None, "resumed across totals");
    }

    /// `complete_multipart` takes the `i`th part at index `i`
    #[test]
    fn completion_needs_every_part_in_index_order() {
        let mut ledger = ResumeLedger::open_at(scratch("order"), &plan(3072, 1024));
        ledger.begin("upload-1").expect("begin");
        ledger.landed(2, &part("etag-2")).expect("landed");
        ledger.landed(0, &part("etag-0")).expect("landed");
        assert!(ledger.parts().is_err(), "gap at index 1 completed");

        ledger.landed(1, &part("etag-1")).expect("landed");
        let ids: Vec<String> =
            ledger.parts().expect("complete").into_iter().map(|p| p.content_id).collect();
        assert_eq!(ids, ["etag-0", "etag-1", "etag-2"], "out of index order");
    }

    /// A ledger killed mid-line — the crash it exists for — must read as "that part never
    /// landed", not resume onto half an ETag and complete with a part id R2 never issued
    #[test]
    fn a_truncated_ledger_is_ignored_rather_than_half_read() {
        let (path, plan) = (scratch("torn"), plan(4096, 1024));
        let mut ledger = ResumeLedger::open_at(path.clone(), &plan);
        ledger.begin("upload-1").expect("begin");
        ledger.landed(0, &part("etag-0")).expect("landed");
        let body = std::fs::read_to_string(&path).expect("read");
        std::fs::write(&path, &body[..body.len() - 4]).expect("truncate");

        let reopened = ResumeLedger::open_at(path, &plan);
        assert_eq!(reopened.upload_id(), Some("upload-1"), "header still stands");
        assert!(!reopened.holds(0), "half-written line resumed");
    }

    /// Ledger path is derived from the oid alone → two archives never share one
    #[test]
    fn the_ledger_is_named_by_the_oid_it_belongs_to() {
        let (a, b) = (ledger_path(&"a".repeat(64)), ledger_path(&"b".repeat(64)));
        assert_ne!(a, b);
        assert!(a.ends_with(format!("{}.ledger", "a".repeat(64))), "{a:?}");
    }
}
