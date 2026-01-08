use std::fs::{OpenOptions, create_dir_all};
use std::io::Result as IoResult;
use std::path::PathBuf;

fn make_sparse_file(path: &PathBuf, len: u64) -> IoResult<()> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let f = OpenOptions::new().create(true).write(true).open(path)?;
    f.set_len(len)?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    // Create two sparse files slightly larger than u32::MAX to force Zip64
    let out_dir = PathBuf::from("tests/golden/zip64_files");
    let f1 = out_dir.join("large1.bin");
    let f2 = out_dir.join("large2.bin");

    // sizes > 0xFFFF_FFFF
    let size = 0x1_0000_0000u64 + 16;
    println!("creating sparse files in {:?}", out_dir);
    make_sparse_file(&f1, size)?;
    make_sparse_file(&f2, size + 32)?;

    // prepare srcs for writer
    let srcs: Vec<(&std::path::Path, &str)> = vec![(&f1, "large1.bin"), (&f2, "large2.bin")];
    let dest = PathBuf::from("tests/golden/tz_zip64_golden.zip");
    if let Some(p) = dest.parent() {
        create_dir_all(p)?;
    }

    println!("writing zip to {:?}", dest);
    // call existing writer from library crate
    igir::torrentzip_zip64::write_torrentzip_zip64(
        &srcs,
        &dest,
        igir::types::ZipFormat::Torrentzip,
        None,
    )?;

    println!("wrote golden: {:?}", dest);
    Ok(())
}
