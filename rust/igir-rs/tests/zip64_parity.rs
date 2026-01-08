use std::fs::read;
use std::path::PathBuf;

/// This test will run only if the golden zip exists at tests/golden/tz_zip64_golden.zip
#[test]
fn compare_zip64_with_golden_if_present() {
    let golden = PathBuf::from("tests/golden/tz_zip64_golden.zip");
    if !golden.exists() {
        eprintln!("zip64 golden not present; skipping test");
        return;
    }

    // create temp output
    let out = PathBuf::from("tests/golden/tz_zip64_golden.repro.zip");
    if out.exists() {
        let _ = std::fs::remove_file(&out);
    }

    // prepare the same source files
    let sdir = PathBuf::from("tests/golden/zip64_files");
    let f1 = sdir.join("large1.bin");
    let f2 = sdir.join("large2.bin");
    assert!(
        f1.exists() && f2.exists(),
        "sparse source files missing; run generator"
    );

    let srcs: Vec<(&std::path::Path, &str)> = vec![(&f1, "large1.bin"), (&f2, "large2.bin")];
    igir::torrentzip_zip64::write_torrentzip_zip64(
        &srcs,
        &out,
        igir::types::ZipFormat::Torrentzip,
        None,
    )
        .expect("write failed");

    let got = read(&out).expect("read out");
    let want = read(&golden).expect("read golden");

    assert_eq!(got.len(), want.len(), "zip size differs from golden");
    assert_eq!(
        got, want,
        "zip bytes differ from golden — run generator and inspect diffs"
    );
}
