//! Re-emit a chain archive as independently-extractable segments, so a lost puller resumes
//! instead of re-fetching a quarter-terabyte.
//!
//! - Segment = complete `.tar.zst` over whole members; concatenated they are still one
//!   valid `.tar.zst` (RFC 8878 frame concatenation), and any one extracts from its range
//! - Complete tars, not one stream cut into frames: a fragment has no end-of-archive block,
//!   so `tar -x` on it fails at EOF. Consumers pay `--ignore-zeros` for this
//! - Members are copied byte-for-byte, never re-created — modes, owners, sparse maps and
//!   long-name headers survive because nothing here interprets them
//! - Cuts land only after a member that nothing follows *depends on* ([`PREFIX_TYPES`])
use std::io::{Read, Write};
use std::path::Path;

use sha2::Digest as _;

use super::seekable::{self, Segment};
use super::{Compression, StorageError, compression_from_name};
use crate::progress::StepProgress;

/// `tar`'s fixed block
const BLOCK: usize = 512;

/// End-of-archive = two zero blocks. Each segment gets its own, so each is a whole archive
const TERMINATOR: [u8; BLOCK * 2] = [0; BLOCK * 2];

/// Headers describing the member that follows: cutting between them and their subject
/// strands the description and the file is extracted under a mangled name
const PREFIX_TYPES: [u8; 3] = [b'L', b'K', b'x'];

/// Uncompressed bytes per segment. Resume granularity against bookkeeping: 512 MiB puts a
/// 258 GiB chain at ~516 frames (a 4 KiB seek table) and re-does at most this much
pub const SEGMENT_BYTES: u64 = 512 * 1024 * 1024;

/// Level for the re-compression. Chain trees are RocksDB SSTs — already compressed, ~5% left
/// on the table — so the default buys the speed instead
pub const DEFAULT_LEVEL: i32 = 3;

#[derive(Debug)]
pub struct Packed {
    pub sha256: String,
    pub size_bytes: u64,
    pub uncompressed_bytes: u64,
    pub segments: Vec<Segment>,
}

/// One `tar` header, in the only two facts segmentation needs
struct Header {
    size: u64,
    typeflag: u8,
}

impl Header {
    /// `None` = the all-zero block that ends an archive
    fn parse(block: &[u8; BLOCK]) -> Result<Option<Self>, StorageError> {
        if block.iter().all(|&b| b == 0) {
            return Ok(None);
        }
        // Stored checksum is computed with its own field blanked to spaces; verifying it is
        // what catches a desynced walk before it starts slicing members at the wrong offset
        let stored = parse_octal(&block[148..156]).ok_or_else(|| Self::corrupt("checksum"))?;
        let sum: u32 = block
            .iter()
            .enumerate()
            .map(|(i, &b)| if (148..156).contains(&i) { b' ' as u32 } else { b as u32 })
            .sum();
        if stored != sum as u64 {
            return Err(Self::corrupt("header checksum mismatch"));
        }
        let size = parse_size(&block[124..136]).ok_or_else(|| Self::corrupt("size"))?;
        Ok(Some(Header { size, typeflag: block[156] }))
    }

    fn corrupt(what: &str) -> StorageError {
        StorageError::Bucket(format!("archive is not a tar stream: bad {what}"))
    }

    /// Data is padded out to a whole number of blocks
    fn data_len(&self) -> u64 {
        self.size.div_ceil(BLOCK as u64) * BLOCK as u64
    }

    /// Something after this member needs it → the two cannot land in different segments
    fn is_prefix(&self) -> bool {
        PREFIX_TYPES.contains(&self.typeflag)
    }
}

fn parse_octal(field: &[u8]) -> Option<u64> {
    let s = field.split(|&b| b == 0 || b == b' ').find(|p| !p.is_empty())?;
    u64::from_str_radix(std::str::from_utf8(s).ok()?, 8).ok()
}

/// GNU switches to base-256 (high bit set) once a size outruns 11 octal digits — a 8 GiB+
/// member, which a chain tree does not have but an unchecked parse would silently mis-read
fn parse_size(field: &[u8]) -> Option<u64> {
    if field[0] & 0x80 == 0 {
        return parse_octal(field);
    }
    let mut n = u64::from(field[0] & 0x7f);
    for &b in &field[1..] {
        n = n.checked_mul(256)?.checked_add(u64::from(b))?;
    }
    Some(n)
}

/// Blob bytes as they are written: the object's own digest and its length, in one pass
struct Sink<W> {
    inner: W,
    hasher: sha2::Sha256,
    written: u64,
}

impl<W: Write> Write for Sink<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write_all(buf)?;
        self.hasher.update(buf);
        self.written += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Reports what it has pulled off the source archive (the output's size is not known until
/// the last frame closes, so the input is the only honest denominator)
struct Metered<'a, R> {
    inner: R,
    read: u64,
    total: u64,
    progress: &'a dyn StepProgress,
}

impl<R: Read> Read for Metered<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        self.progress.bytes(self.read, self.total);
        Ok(n)
    }
}

/// Repack `input` into `out`, returning the object's identity and its frame table.
///
/// Streams throughout: peak memory is one copy buffer, peak disk is the output alone
pub fn pack(
    input: &Path,
    out: &Path,
    level: i32,
    progress: &dyn StepProgress,
) -> Result<Packed, StorageError> {
    pack_with(input, out, level, SEGMENT_BYTES, progress)
}

/// [`pack`] at an explicit segment target, so a test can drive the same walk over a fixture
/// small enough to run in-process
pub fn pack_with(
    input: &Path,
    out: &Path,
    level: i32,
    target: u64,
    progress: &dyn StepProgress,
) -> Result<Packed, StorageError> {
    let path = || input.display().to_string();
    let compression = compression_from_name(&input.to_string_lossy())
        .ok_or_else(|| StorageError::UnknownCompression { name: path() })?;
    let file = std::fs::File::open(input).map_err(|source| StorageError::Io {
        op: "open",
        path: path(),
        source,
    })?;
    let total = file.metadata().map(|m| m.len()).unwrap_or(0);
    let metered = Metered { inner: std::io::BufReader::new(file), read: 0, total, progress };

    progress.note("packing");
    let mut source: Box<dyn Read> = match compression {
        Compression::Zstd => {
            Box::new(zstd::Decoder::new(metered).map_err(|source| StorageError::Io {
                op: "open zstd",
                path: path(),
                source,
            })?)
        }
        Compression::None => Box::new(metered),
        other => return Err(StorageError::Undigestable { path: path(), compression: other }),
    };

    let dest = std::fs::File::create(out).map_err(|source| StorageError::Io {
        op: "create",
        path: out.display().to_string(),
        source,
    })?;
    let mut sink =
        Sink { inner: std::io::BufWriter::new(dest), hasher: sha2::Sha256::new(), written: 0 };

    let io = |source| StorageError::Io { op: "pack", path: path(), source };
    let mut segments = Vec::new();
    let mut uncompressed_bytes = 0;
    let mut header = [0u8; BLOCK];
    let mut pending = read_header(&mut source, &mut header).map_err(io)?;

    while pending {
        let offset = sink.written;
        let mut uncompressed = 0u64;
        let mut encoder = zstd::Encoder::new(&mut sink, level).map_err(io)?;
        // Per-frame XXH64: a segment fetched into a resumed pull is verified by the decoder
        // itself, with no whole-blob digest to re-stream
        encoder.include_checksum(true).map_err(io)?;
        pending = fill(&mut source, &mut encoder, &mut header, &mut uncompressed, target)?;
        encoder.finish().map_err(io)?;

        // Both seek-table fields are `u32`. A segment closes at `target`, but one member is
        // indivisible, so a single 4 GiB+ file is the way either could overflow
        let compressed = sink.written - offset;
        let over = [("compressed", compressed), ("extracted", uncompressed)]
            .into_iter()
            .find(|&(_, n)| n > u64::from(u32::MAX));
        if let Some((which, n)) = over {
            return Err(StorageError::Bucket(format!(
                "segment {} is {n} bytes {which}, past the seek table's 4 GiB field",
                segments.len()
            )));
        }
        segments.push(Segment { offset, compressed, uncompressed });
        uncompressed_bytes += uncompressed;
    }

    sink.write_all(&seekable::encode(&segments)).map_err(io)?;
    sink.flush().map_err(io)?;
    Ok(Packed {
        sha256: hex::encode(sink.hasher.finalize()),
        size_bytes: sink.written,
        uncompressed_bytes,
        segments,
    })
}

/// Next header into `buf`. `false` = the archive ended (zero block, or a clean EOF)
fn read_header(source: &mut impl Read, buf: &mut [u8; BLOCK]) -> std::io::Result<bool> {
    let mut got = 0;
    while got < BLOCK {
        match source.read(&mut buf[got..])? {
            0 if got == 0 => return Ok(false),
            0 => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "tar stream ends mid-header",
                ));
            }
            n => got += n,
        }
    }
    Ok(!buf.iter().all(|&b| b == 0))
}

/// Copy members into one segment until it is big enough to close, terminating it either way.
///
/// `header` carries the already-read block in and the *next* one out — the lookahead is what
/// lets the caller finish without opening an empty trailing frame
fn fill(
    source: &mut impl Read,
    out: &mut impl Write,
    header: &mut [u8; BLOCK],
    uncompressed: &mut u64,
    target: u64,
) -> Result<bool, StorageError> {
    let io = |source| StorageError::Io { op: "pack", path: "<stream>".into(), source };
    let mut buf = vec![0u8; 1 << 20];
    while let Some(h) = Header::parse(header)? {
        if h.typeflag == b'g' {
            return Err(StorageError::Bucket(
                "archive carries a pax global header, which every segment would have to \
                 repeat — re-create it with GNU tar's default format"
                    .into(),
            ));
        }
        out.write_all(header).map_err(io)?;
        *uncompressed += BLOCK as u64;

        let mut left = h.data_len();
        while left > 0 {
            let want = left.min(buf.len() as u64) as usize;
            source.read_exact(&mut buf[..want]).map_err(io)?;
            out.write_all(&buf[..want]).map_err(io)?;
            left -= want as u64;
        }
        *uncompressed += h.data_len();

        let more = read_header(source, header).map_err(io)?;
        if !more {
            break;
        }
        // Cut only where nothing behind us is still owed its subject
        if !h.is_prefix() && *uncompressed >= target {
            out.write_all(&TERMINATOR).map_err(io)?;
            *uncompressed += TERMINATOR.len() as u64;
            return Ok(true);
        }
    }
    out.write_all(&TERMINATOR).map_err(io)?;
    *uncompressed += TERMINATOR.len() as u64;
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn have(tool: &str) -> bool {
        Command::new(tool).arg("--version").output().is_ok()
    }

    struct Fixture {
        dir: std::path::PathBuf,
        archive: std::path::PathBuf,
        names: Vec<String>,
    }

    /// `count` members of `each` bytes, tarred and zstd'd exactly as the produce script does
    fn fixture(tag: &str, count: usize, each: usize) -> Option<Fixture> {
        if !have("tar") || !have("zstd") {
            eprintln!("skipping: tar/zstd not on PATH");
            return None;
        }
        let dir = std::env::temp_dir().join(format!("ztest-pack-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let src = dir.join("src");
        std::fs::create_dir_all(&src).expect("src");
        let names: Vec<String> = (0..count).map(|i| format!("member{i:03}.dat")).collect();
        for (i, name) in names.iter().enumerate() {
            let byte = (i % 251) as u8;
            std::fs::write(src.join(name), vec![byte; each]).expect("member");
        }
        let archive = dir.join("fixture.tar.zst");
        let ok = Command::new("tar")
            .args(["--zstd", "-cf"])
            .arg(&archive)
            .args(["-C", &src.display().to_string(), "."])
            .status()
            .expect("tar runs")
            .success();
        assert!(ok, "fixture archive");
        Some(Fixture { dir, archive, names })
    }

    /// Segments are cut on `SEGMENT_BYTES`, so a test-sized archive needs the threshold
    /// brought to it. Same walk, same cut rule, one member per segment
    fn pack_every_member(f: &Fixture) -> (std::path::PathBuf, Packed) {
        let out = f.dir.join("packed.tar.zst");
        // 1 byte forces the cut at the first legal boundary after every member
        let packed =
            pack_with(&f.archive, &out, DEFAULT_LEVEL, 1, &crate::progress::Silent).expect("packs");
        (out, packed)
    }

    #[test]
    fn a_packed_blob_is_still_one_valid_tar_zst() {
        let Some(f) = fixture("valid", 6, 4096) else { return };
        let (out, packed) = pack_every_member(&f);
        assert!(packed.segments.len() > 1, "nothing was segmented");

        let dest = f.dir.join("whole");
        std::fs::create_dir_all(&dest).expect("dest");
        // `-i` is not optional: without it tar stops at the first segment's end-of-archive
        let ok = Command::new("tar")
            .args(["--zstd", "-ixf"])
            .arg(&out)
            .args(["-C", &dest.display().to_string()])
            .status()
            .expect("tar runs")
            .success();
        assert!(ok, "packed blob did not extract");
        for name in &f.names {
            assert!(dest.join(name).exists(), "{name} missing from the whole-blob extract");
        }
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// The property the whole feature rests on: any single frame, fetched by its recorded
    /// range and nothing else, extracts on its own
    #[test]
    fn any_one_segment_extracts_alone_from_its_recorded_range() {
        let Some(f) = fixture("ranges", 6, 4096) else { return };
        let (out, packed) = pack_every_member(&f);
        let blob = std::fs::read(&out).expect("blob");
        assert_eq!(blob.len() as u64, packed.size_bytes);

        let mut seen = Vec::new();
        for (i, seg) in packed.segments.iter().enumerate() {
            let slice = &blob[seg.offset as usize..=seg.last_byte() as usize];
            let raw = zstd::decode_all(slice).expect("segment is a whole zstd frame");
            assert_eq!(raw.len() as u64, seg.uncompressed, "segment {i} size disagrees");

            let dest = f.dir.join(format!("seg{i}"));
            std::fs::create_dir_all(&dest).expect("dest");
            let piece = f.dir.join(format!("seg{i}.tar"));
            std::fs::write(&piece, &raw).expect("write");
            // No `-i`: a segment is a complete archive in its own right
            let ok = Command::new("tar")
                .arg("-xf")
                .arg(&piece)
                .args(["-C", &dest.display().to_string()])
                .status()
                .expect("tar runs")
                .success();
            assert!(ok, "segment {i} did not extract on its own");
            let mut got: Vec<String> = std::fs::read_dir(&dest)
                .expect("read")
                .filter_map(|e| Some(e.ok()?.file_name().to_string_lossy().into_owned()))
                .filter(|n| n.ends_with(".dat"))
                .collect();
            got.sort();
            seen.extend(got);
        }
        let mut want = f.names.clone();
        want.sort();
        seen.sort();
        assert_eq!(seen, want, "the segments together are not the archive");
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// Offsets recorded at pack time must be the offsets the parent later ranges on
    #[test]
    fn the_written_seek_table_reads_back_as_the_segments_that_were_packed() {
        let Some(f) = fixture("table", 5, 4096) else { return };
        let (out, packed) = pack_every_member(&f);
        let blob = std::fs::read(&out).expect("blob");
        let tail = &blob[blob.len().saturating_sub(seekable::TAIL_PROBE_BYTES as usize)..];
        assert_eq!(seekable::parse(tail, packed.size_bytes), Ok(packed.segments.clone()));
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    /// A truncated member must fail the walk, not produce a short archive that extracts
    #[test]
    fn a_stream_ending_mid_member_is_refused() {
        let Some(f) = fixture("short", 3, 4096) else { return };
        let raw = zstd::decode_all(std::fs::File::open(&f.archive).expect("open")).expect("dec");
        let cut = f.dir.join("cut.tar");
        std::fs::write(&cut, &raw[..raw.len() / 2]).expect("write");
        let out = f.dir.join("nope.tar.zst");
        assert!(pack(&cut, &out, DEFAULT_LEVEL, &crate::progress::Silent).is_err());
        let _ = std::fs::remove_dir_all(&f.dir);
    }

    #[test]
    fn a_base_256_size_field_is_read_rather_than_mistaken_for_octal() {
        let mut field = [0u8; 12];
        field[0] = 0x80;
        field[11] = 0x2a;
        assert_eq!(parse_size(&field), Some(42));
        assert_eq!(parse_size(b"00000010000\0"), Some(4096));
    }
}
