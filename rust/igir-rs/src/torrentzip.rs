use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::Context;
use crc32fast::Hasher as Crc32;
use zip::write::FileOptions;

use crate::actions::ActionProgressHandle;
use crate::types::ZipFormat;

// CP437 table: index -> Unicode char. We'll use it to encode Unicode filenames to CP437
// by reverse-mapping characters to their byte value. Table taken from the CP437 specification.
const CP437_TABLE: [char; 256] = [
    '\u{0000}', '\u{0001}', '\u{0002}', '\u{0003}', '\u{0004}', '\u{0005}', '\u{0006}', '\u{0007}',
    '\u{0008}', '\u{0009}', '\u{000A}', '\u{000B}', '\u{000C}', '\u{000D}', '\u{000E}', '\u{000F}',
    '\u{0010}', '\u{0011}', '\u{0012}', '\u{0013}', '\u{0014}', '\u{0015}', '\u{0016}', '\u{0017}',
    '\u{0018}', '\u{0019}', '\u{001A}', '\u{001B}', '\u{001C}', '\u{001D}', '\u{001E}', '\u{001F}',
    ' ', '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', '0', '1', '2',
    '3', '4', '5', '6', '7', '8', '9', ':', ';', '<', '=', '>', '?', '@', 'A', 'B', 'C', 'D', 'E',
    'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
    'Y', 'Z', '[', '\\', ']', '^', '_', '`', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
    'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '{', '|', '}', '~',
    '\u{007F}', '\u{00C7}', '\u{00FC}', '\u{00E9}', '\u{00E2}', '\u{00E4}', '\u{00E0}', '\u{00E5}',
    '\u{00E7}', '\u{00EA}', '\u{00EB}', '\u{00E8}', '\u{00EF}', '\u{00EE}', '\u{00EC}', '\u{00C4}',
    '\u{00C5}', '\u{00C9}', '\u{00E6}', '\u{00C6}', '\u{00F4}', '\u{00F6}', '\u{00F2}', '\u{00FB}',
    '\u{00F9}', '\u{00FF}', '\u{00D6}', '\u{00DC}', '\u{00A2}', '\u{00A3}', '\u{00A5}', '\u{20A7}',
    '\u{0192}', '\u{00E1}', '\u{00ED}', '\u{00F3}', '\u{00FA}', '\u{00F1}', '\u{00D1}', '\u{00AA}',
    '\u{00BA}', '\u{00BF}', '\u{2310}', '\u{00AC}', '\u{00BD}', '\u{00BC}', '\u{00A1}', '\u{00AB}',
    '\u{00BB}', '\u{2591}', '\u{2592}', '\u{2593}', '\u{2502}', '\u{2524}', '\u{2561}', '\u{2562}',
    '\u{2556}', '\u{2555}', '\u{2563}', '\u{2551}', '\u{2557}', '\u{255D}', '\u{255C}', '\u{255B}',
    '\u{2510}', '\u{2514}', '\u{2534}', '\u{252C}', '\u{251C}', '\u{2500}', '\u{253C}', '\u{255E}',
    '\u{255F}', '\u{255A}', '\u{2554}', '\u{2569}', '\u{2566}', '\u{2560}', '\u{2550}', '\u{256C}',
    '\u{2567}', '\u{2568}', '\u{2564}', '\u{2565}', '\u{2559}', '\u{2558}', '\u{2552}', '\u{2553}',
    '\u{256B}', '\u{256A}', '\u{2518}', '\u{250C}', '\u{2588}', '\u{2584}', '\u{258C}', '\u{2590}',
    '\u{2580}', '\u{03B1}', '\u{00DF}', '\u{0393}', '\u{03C0}', '\u{03A3}', '\u{03C3}', '\u{00B5}',
    '\u{03C4}', '\u{03A6}', '\u{0398}', '\u{03A9}', '\u{03B4}', '\u{221E}', '\u{03C6}', '\u{03B5}',
    '\u{2229}', '\u{2261}', '\u{00B1}', '\u{2265}', '\u{2264}', '\u{2320}', '\u{2321}', '\u{00F7}',
    '\u{2248}', '\u{00B0}', '\u{2219}', '\u{00B7}', '\u{221A}', '\u{207F}', '\u{00B2}', '\u{25A0}',
    '\u{00A0}',
];

fn encode_cp437(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len());
    for ch in s.chars() {
        // fast path for ASCII
        if ch as u32 <= 0x7F {
            out.push(ch as u8);
            continue;
        }
        // search in table
        let mut found = false;
        for (i, &c) in CP437_TABLE.iter().enumerate() {
            if c == ch {
                out.push(i as u8);
                found = true;
                break;
            }
        }
        if !found {
            return None;
        }
    }
    Some(out)
}

/// Create a TorrentZip/RVZSTD archive for a single file. This is a minimal, pragmatic
/// implementation: use the zip crate to write the archive, then compute the CRC32 of the
/// central directory and patch the EOCD comment to match TZWriter behavior.
pub fn write_torrentzip(
    src: &Path,
    dest: &Path,
    filename_in_zip: &str,
    format: ZipFormat,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<()> {
    // If this is a Torrentzip (stored) and the filename is CP437-encodable,
    // write a manual single-file Stored ZIP so we can control the filename bytes
    // (both local header and central directory) exactly to CP437.
    // NOTE: previously we used a manual single-file "stored" writer for
    // Torrentzip when the filename was CP437-encodable to control raw
    // filename bytes exactly. That produced uncompressed (stored) ZIPs.
    // For parity with the Node implementation we want compressed archives
    // by default; skip the manual stored path so the zip crate produces
    // compressed entries instead.
    if false {
        if let Some(raw_name) = encode_cp437(filename_in_zip) {
            // read source bytes
            let mut input = File::open(src).with_context(|| format!("opening {:?}", src))?;
            let mut file_bytes = Vec::new();
            input.read_to_end(&mut file_bytes)?;

            // compute CRC32 of file data
            let mut fh_hasher = Crc32::new();
            fh_hasher.update(&file_bytes);
            let file_crc = fh_hasher.finalize();

            // We'll build local file header, central directory entry, and EOCD
            let mut out = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(dest)
                .with_context(|| format!("creating {:?}", dest))?;

            // Local file header
            // signature
            out.write_all(&0x04034b50u32.to_le_bytes())?;
            // version needed to extract (2 bytes)
            out.write_all(&20u16.to_le_bytes())?;
            // general purpose bit flag (0 = CP437)
            out.write_all(&0u16.to_le_bytes())?;
            // compression method: 0 = stored
            out.write_all(&0u16.to_le_bytes())?;
            // last mod time / date (set zero)
            out.write_all(&0u16.to_le_bytes())?;
            out.write_all(&0u16.to_le_bytes())?;
            // crc32
            out.write_all(&file_crc.to_le_bytes())?;
            // compressed size
            out.write_all(&(file_bytes.len() as u32).to_le_bytes())?;
            // uncompressed size
            out.write_all(&(file_bytes.len() as u32).to_le_bytes())?;
            // filename length
            out.write_all(&(raw_name.len() as u16).to_le_bytes())?;
            // extra field length
            out.write_all(&0u16.to_le_bytes())?;
            // filename bytes
            out.write_all(&raw_name)?;
            // file data
            out.write_all(&file_bytes)?;

            let local_header_size = 30 + raw_name.len() + file_bytes.len();

            // central directory header
            let mut central_dir = Vec::new();
            // signature
            central_dir.extend_from_slice(&0x02014b50u32.to_le_bytes());
            // version made by
            central_dir.extend_from_slice(&20u16.to_le_bytes());
            // version needed
            central_dir.extend_from_slice(&20u16.to_le_bytes());
            // gp flag
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // compression method
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // mod time/date
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // crc32
            central_dir.extend_from_slice(&file_crc.to_le_bytes());
            // comp size
            central_dir.extend_from_slice(&(file_bytes.len() as u32).to_le_bytes());
            // uncomp size
            central_dir.extend_from_slice(&(file_bytes.len() as u32).to_le_bytes());
            // filename length
            central_dir.extend_from_slice(&(raw_name.len() as u16).to_le_bytes());
            // extra length
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // file comment length
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // disk number start
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // internal file attrs
            central_dir.extend_from_slice(&0u16.to_le_bytes());
            // external file attrs
            central_dir.extend_from_slice(&0u32.to_le_bytes());
            // relative offset of local header (0)
            central_dir.extend_from_slice(&0u32.to_le_bytes());
            // filename
            central_dir.extend_from_slice(&raw_name);

            // compute central dir CRC for EOCD comment
            let mut cdh_hasher = Crc32::new();
            cdh_hasher.update(&central_dir);
            let cdfh_crc = cdh_hasher.finalize();
            let cdfh_crc_hex = format!("{:08X}", cdfh_crc);

            // write central dir
            out.write_all(&central_dir)?;

            // EOCD
            out.write_all(&0x06054b50u32.to_le_bytes())?;
            // disk numbers
            out.write_all(&0u16.to_le_bytes())?;
            out.write_all(&0u16.to_le_bytes())?;
            // total entries this disk
            out.write_all(&1u16.to_le_bytes())?;
            // total entries
            out.write_all(&1u16.to_le_bytes())?;
            // size of central dir
            out.write_all(&(central_dir.len() as u32).to_le_bytes())?;
            // offset of central dir
            out.write_all(&(local_header_size as u32).to_le_bytes())?;
            // comment
            let comment = format!("TORRENTZIPPED-{}", cdfh_crc_hex);
            out.write_all(&(comment.len() as u16).to_le_bytes())?;
            out.write_all(comment.as_bytes())?;

            out.flush()?;
            return Ok(());
        }
    }

    // Fallback: use zip crate to write archive and then patch EOCD comment as before
    let mut out = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest)
        .with_context(|| format!("creating {:?}", dest))?;

    let mut zip = zip::ZipWriter::new(&mut out);
    let options: FileOptions<'_, zip::write::ExtendedFileOptions> = match format {
        // Use Deflated for Torrentzip to produce compressed archives (parity with Node)
        ZipFormat::Torrentzip => {
            FileOptions::default().compression_method(zip::CompressionMethod::Deflated)
        }
        ZipFormat::Rvzstd => {
            FileOptions::default().compression_method(zip::CompressionMethod::Zstd)
        }
        ZipFormat::Deflate => {
            FileOptions::default().compression_method(zip::CompressionMethod::Deflated)
        }
    };

    let mut input = File::open(src).with_context(|| format!("opening {:?}", src))?;
    zip.start_file(filename_in_zip, options)?;
    let total = input.metadata().map(|m| m.len()).ok();
    let mut buf = vec![0u8; 1 << 20];
    let mut written = 0u64;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        zip.write_all(&buf[..n])?;
        written = written.saturating_add(n as u64);
        if let Some(handle) = progress {
            handle.report_bytes(written, total);
        }
    }
    zip.finish()?;

    // Now compute CRC32 of the central directory and patch EOCD comment.
    // Read file back to compute central directory CRC: seek to start and read contents.
    out.seek(SeekFrom::Start(0))?;
    let mut data = Vec::new();
    out.read_to_end(&mut data)?;

    // Find EOCD signature 0x06054b50 (little endian bytes "PK\x05\x06")
    let eocd_sig = b"PK\x05\x06";
    let pos = data
        .windows(4)
        .rposition(|w| w == eocd_sig)
        .context("EOCD not found")?;

    // EOCD structure: offset 16..20 is size of central directory, 12..16 is offset
    if data.len() < pos + 22 {
        anyhow::bail!("EOCD truncated");
    }
    let cd_size = u32::from_le_bytes([
        data[pos + 12],
        data[pos + 13],
        data[pos + 14],
        data[pos + 15],
    ]) as usize;
    let cd_offset = u32::from_le_bytes([
        data[pos + 16],
        data[pos + 17],
        data[pos + 18],
        data[pos + 19],
    ]) as usize;

    let central_dir = &data[cd_offset..cd_offset + cd_size];
    let mut hasher = Crc32::new();
    hasher.update(central_dir);
    let cdfh_crc = hasher.finalize();
    let cdfh_crc_hex = format!("{:08X}", cdfh_crc);

    let comment = match format {
        ZipFormat::Torrentzip => format!("TORRENTZIPPED-{}", cdfh_crc_hex),
        ZipFormat::Rvzstd => format!("RVZSTD-{}", cdfh_crc_hex),
        ZipFormat::Deflate => format!("TORRENTZIPPED-{}", cdfh_crc_hex),
    };

    // Patch comment length and bytes in EOCD
    // EOCD structure: comment length at pos+20 (2 bytes), comment starts at pos+22
    let comment_len = comment.len() as u16;
    // update in-memory data
    let mut patched = data;
    patched[pos + 20] = (comment_len & 0xff) as u8;
    patched[pos + 21] = ((comment_len >> 8) & 0xff) as u8;
    // ensure buffer length; truncate or extend as necessary
    let comment_start = pos + 22;
    if patched.len() < comment_start {
        patched.resize(comment_start, 0);
    }
    if patched.len() >= comment_start + comment.len() {
        for (i, b) in comment.as_bytes().iter().enumerate() {
            patched[comment_start + i] = *b;
        }
        patched.truncate(comment_start + comment.len());
    } else {
        // append comment
        patched.extend_from_slice(comment.as_bytes());
    }

    // rewrite file from start
    out.seek(SeekFrom::Start(0))?;
    out.set_len(patched.len() as u64)?;
    out.write_all(&patched)?;
    out.flush()?;

    Ok(())
}
