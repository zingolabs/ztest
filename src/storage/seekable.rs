//! zstd seekable-format seek table: the index that makes a seed resumable.
//!
//! - Frame layout is core zstd, not contrib: RFC 8878 makes concatenated frames mandatory
//!   decoder behaviour, so a segmented blob is still a plain `.tar.zst`
//! - Only the *table* comes from `contrib/seekable_format`, and it rides a **skippable**
//!   frame → every decoder that never heard of it ignores it
//! - Written by [`super::pack`], read by the parent (never the pod: the puller is shell,
//!   and gets byte ranges already resolved)
//! - Entries carry sizes, not offsets; an offset is the running sum, which is what keeps
//!   the table 8 bytes a frame
use std::fmt;

/// Skippable-frame magic zstd reserves for auxiliary data (`0x184D2A50..=0x184D2A5F`)
const SKIPPABLE_MAGIC: u32 = 0x184D_2A5E;

/// Trailer magic, last 4 bytes of the blob — what makes the table findable from the end
const FOOTER_MAGIC: u32 = 0x8F92_EAB1;

/// `Number_Of_Frames` + `Seek_Table_Descriptor` + `Seek_Table_Magic_Number`
const FOOTER_LEN: usize = 4 + 1 + 4;

/// `Compressed_Size` + `Decompressed_Size`. Per-frame checksums stay off — zstd's own
/// content checksum already fails a corrupt frame at decompression
const ENTRY_LEN: usize = 8;

/// Tail worth reading to find a table. 16k frames at [`super::pack::SEGMENT_BYTES`] = 8 TiB,
/// so one fetch always covers it
pub const TAIL_PROBE_BYTES: u64 = 128 * 1024;

/// One data frame: a whole number of `tar` members, independently fetchable and extractable
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Segment {
    pub offset: u64,
    pub compressed: u64,
    pub uncompressed: u64,
}

impl Segment {
    /// `curl --range` is inclusive on both ends
    pub fn last_byte(&self) -> u64 {
        self.offset + self.compressed - 1
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SeekTableError {
    NoFooter,
    Truncated {
        need: usize,
        have: usize,
    },
    /// A frame wider than the format's `u32` — [`super::pack`] cannot have written it
    FrameTooLarge {
        index: usize,
    },
    /// Frame sizes do not add up to the blob: table is for different bytes
    SizeMismatch {
        table: u64,
        blob: u64,
    },
}

impl fmt::Display for SeekTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Every pre-segmentation archive lands here — the caller's cue to stream it whole
            SeekTableError::NoFooter => write!(f, "no seek table (unsegmented archive)"),
            SeekTableError::Truncated { need, have } => {
                write!(f, "seek table wants {need} bytes, tail holds {have}")
            }
            SeekTableError::FrameTooLarge { index } => write!(f, "frame {index} exceeds 4 GiB"),
            SeekTableError::SizeMismatch { table, blob } => {
                write!(f, "seek table spans {table} bytes, blob is {blob}")
            }
        }
    }
}

impl std::error::Error for SeekTableError {}

/// Serialize `segments` as the trailing skippable frame.
///
/// Sizes only: the reader rebuilds offsets by summation, so a table can never disagree with
/// itself about where a frame starts
pub fn encode(segments: &[Segment]) -> Vec<u8> {
    let body = segments.len() * ENTRY_LEN + FOOTER_LEN;
    let mut out = Vec::with_capacity(body + 8);
    out.extend_from_slice(&SKIPPABLE_MAGIC.to_le_bytes());
    out.extend_from_slice(&(body as u32).to_le_bytes());
    for s in segments {
        out.extend_from_slice(&(s.compressed as u32).to_le_bytes());
        out.extend_from_slice(&(s.uncompressed as u32).to_le_bytes());
    }
    out.extend_from_slice(&(segments.len() as u32).to_le_bytes());
    out.push(0);
    out.extend_from_slice(&FOOTER_MAGIC.to_le_bytes());
    out
}

/// Parse the table out of `tail`, the last bytes of a blob of `blob_len`.
///
/// - `tail` may start anywhere; the footer is located from the end
/// - Offsets come out absolute, so a caller ranges on the blob without re-deriving anything
/// - Total is checked against `blob_len`: a table that does not span its own blob is for
///   other bytes, and resuming on it would fetch garbage at every offset
pub fn parse(tail: &[u8], blob_len: u64) -> Result<Vec<Segment>, SeekTableError> {
    if tail.len() < FOOTER_LEN {
        return Err(SeekTableError::NoFooter);
    }
    let footer = &tail[tail.len() - FOOTER_LEN..];
    if u32::from_le_bytes(footer[5..9].try_into().expect("4 bytes")) != FOOTER_MAGIC {
        return Err(SeekTableError::NoFooter);
    }
    let count = u32::from_le_bytes(footer[0..4].try_into().expect("4 bytes")) as usize;
    // Descriptor bit 7 = per-frame checksums, which widens every entry. `pack` never sets it
    let entry_len = match footer[4] & 0x80 != 0 {
        true => ENTRY_LEN + 4,
        false => ENTRY_LEN,
    };

    let need = count * entry_len + FOOTER_LEN + 8;
    if tail.len() < need {
        return Err(SeekTableError::Truncated { need, have: tail.len() });
    }
    let entries = &tail[tail.len() - FOOTER_LEN - count * entry_len..tail.len() - FOOTER_LEN];

    let mut segments = Vec::with_capacity(count);
    let mut offset = 0u64;
    for (index, e) in entries.chunks_exact(entry_len).enumerate() {
        let compressed = u32::from_le_bytes(e[0..4].try_into().expect("4 bytes")) as u64;
        let uncompressed = u32::from_le_bytes(e[4..8].try_into().expect("4 bytes")) as u64;
        if compressed == 0 {
            return Err(SeekTableError::FrameTooLarge { index });
        }
        segments.push(Segment { offset, compressed, uncompressed });
        offset += compressed;
    }
    // The table's own skippable frame is the remainder; anything else means these are not
    // this blob's frames
    let table_len = (need - 8) as u64 + 8;
    if offset + table_len != blob_len {
        return Err(SeekTableError::SizeMismatch { table: offset + table_len, blob: blob_len });
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segs(sizes: &[(u64, u64)]) -> Vec<Segment> {
        let mut offset = 0;
        sizes
            .iter()
            .map(|&(compressed, uncompressed)| {
                let s = Segment { offset, compressed, uncompressed };
                offset += compressed;
                s
            })
            .collect()
    }

    fn blob_len(segments: &[Segment], table: &[u8]) -> u64 {
        segments.iter().map(|s| s.compressed).sum::<u64>() + table.len() as u64
    }

    #[test]
    fn a_table_round_trips_through_its_own_encoding() {
        let want = segs(&[(1000, 4096), (2500, 8192), (99, 512)]);
        let table = encode(&want);
        assert_eq!(parse(&table, blob_len(&want, &table)), Ok(want));
    }

    /// Offsets are never stored — the reader sums sizes, so it cannot inherit a stale one
    #[test]
    fn offsets_are_the_running_sum_of_the_frames_before() {
        let want = segs(&[(10, 1), (20, 2), (30, 3)]);
        let table = encode(&want);
        let got = parse(&table, blob_len(&want, &table)).expect("parses");
        assert_eq!(got.iter().map(|s| s.offset).collect::<Vec<_>>(), vec![0, 10, 30]);
        assert_eq!(got[1].last_byte(), 29, "range end is inclusive");
    }

    /// Every archive published before segmentation has no footer, and must read as
    /// "stream it whole" rather than as a corrupt table
    #[test]
    fn an_unsegmented_archive_reports_no_table_rather_than_an_error() {
        assert_eq!(parse(b"not a seek table at all", 23), Err(SeekTableError::NoFooter));
        assert_eq!(parse(b"", 0), Err(SeekTableError::NoFooter));
    }

    /// A table whose frames do not span its blob is a table for *other bytes*. Trusting it
    /// would range at offsets that decode to garbage, one segment at a time
    #[test]
    fn a_table_that_does_not_span_its_blob_is_refused() {
        let want = segs(&[(1000, 4096), (2500, 8192)]);
        let table = encode(&want);
        let right = blob_len(&want, &table);
        assert!(parse(&table, right).is_ok());
        assert_eq!(
            parse(&table, right + 1),
            Err(SeekTableError::SizeMismatch { table: right, blob: right + 1 })
        );
    }

    /// Only the tail is fetched, so a table wider than the probe must say so, not truncate
    #[test]
    fn a_table_wider_than_the_fetched_tail_is_reported_not_guessed() {
        let want = segs(&[(10, 1); 64]);
        let table = encode(&want);
        let cut = &table[table.len() - FOOTER_LEN - 8..];
        assert!(matches!(parse(cut, 999), Err(SeekTableError::Truncated { .. })));
    }

    /// Real zstd must skip the frame, or a segmented blob stops being a plain `.tar.zst`
    #[test]
    fn zstd_decodes_a_stream_whose_last_frame_is_a_seek_table() {
        let mut blob = zstd::encode_all(&b"hello seekable world"[..], 3).expect("encode");
        let segments = segs(&[(blob.len() as u64, 20)]);
        blob.extend_from_slice(&encode(&segments));
        assert_eq!(
            zstd::decode_all(&blob[..]).expect("skippable frame ignored"),
            b"hello seekable world"
        );
    }
}
