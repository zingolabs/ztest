//! Portable recordings: a run directory → one `.zip`, archivable as a CI artifact and
//! replayable elsewhere (`ztest store export`).
//!
//! Entries uncompressed (`STORE`) — log and blobs are already zstd, `meta.json` is tiny;
//! matches nextest's `CompressionMethod::STORE` for pre-compressed payloads

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use zip::write::SimpleFileOptions;

/// Every file under `run_dir` → the zip at `out`, relative layout preserved
pub fn export(run_dir: &Path, out: &Path) -> io::Result<()> {
    let file = File::create(out)?;
    let mut zip = zip::ZipWriter::new(file);
    let opts = SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);

    for entry in walkdir::WalkDir::new(run_dir).into_iter().filter_map(Result::ok) {
        let rel = match entry.path().strip_prefix(run_dir) {
            Ok(r) if !r.as_os_str().is_empty() => r,
            _ => continue,
        };
        let name = rel.to_string_lossy();
        if entry.file_type().is_dir() {
            zip.add_directory(name.as_ref(), opts).map_err(zip_err)?;
        } else if entry.file_type().is_file() {
            zip.start_file(name.as_ref(), opts).map_err(zip_err)?;
            zip.write_all(&fs::read(entry.path())?)?;
        }
    }
    zip.finish().map_err(zip_err)?;
    Ok(())
}

fn zip_err(e: zip::result::ZipError) -> io::Error {
    match e {
        zip::result::ZipError::Io(e) => e,
        other => io::Error::other(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_bundles_the_run_directory() {
        let base = std::env::temp_dir().join(format!(
            "ztest-portable-test-{}-{}",
            std::process::id(),
            blake3::hash(format!("{:?}", std::thread::current().id()).as_bytes()).to_hex()
        ));
        let run_dir = base.join("run");
        fs::create_dir_all(run_dir.join("out")).unwrap();
        fs::write(run_dir.join("meta.json"), b"{}").unwrap();
        fs::write(run_dir.join("run.log.zst"), b"log-bytes").unwrap();
        fs::write(run_dir.join("out").join("abc-combined"), b"blob").unwrap();

        let out = base.join("export.zip");
        export(&run_dir, &out).unwrap();

        // Archive opens, carrying all three under their relative paths
        let mut archive = zip::ZipArchive::new(File::open(&out).unwrap()).unwrap();
        let names: Vec<String> =
            (0..archive.len()).map(|i| archive.by_index(i).unwrap().name().to_string()).collect();
        assert!(names.iter().any(|n| n == "meta.json"), "{names:?}");
        assert!(names.iter().any(|n| n == "run.log.zst"), "{names:?}");
        assert!(names.iter().any(|n| n.replace('\\', "/") == "out/abc-combined"), "{names:?}");
    }
}
