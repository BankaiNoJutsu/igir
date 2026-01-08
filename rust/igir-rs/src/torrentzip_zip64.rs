use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::Context;
use crc32fast::Hasher as Crc32;

use crate::actions::ActionProgressHandle;
use crate::types::ZipFormat;

/// Minimal scaffolding for a Zip64-capable TorrentZip writer.
/// This file will be expanded iteratively. The initial version provides
/// types and simple helpers to write local headers and central directory
/// entries using 64-bit sizes/offsets where necessary.

#[derive(Debug)]
pub(crate) struct Entry {
    name: Vec<u8>,
    crc32: u32,
    compressed_size: u64,
    uncompressed_size: u64,
    local_header_offset: u64,
}

// Copy of CP437 encoder from `torrentzip.rs` to ensure filename bytes match TorrentZip expectations.
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
        if ch as u32 <= 0x7F {
            out.push(ch as u8);
            continue;
        }
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

const COPY_BUF_SIZE: usize = 1 << 20;

fn compute_crc32_and_size(path: &Path) -> anyhow::Result<(u32, u64)> {
    let mut input = File::open(path).with_context(|| format!("opening {:?}", path))?;
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut hasher = Crc32::new();
    let mut total = 0u64;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total = total.saturating_add(n as u64);
    }
    Ok((hasher.finalize(), total))
}

fn stream_file_into(
    src: &Path,
    out: &mut File,
    progress: Option<&ActionProgressHandle>,
    aggregate_total: Option<u64>,
    aggregate_written: &mut u64,
) -> anyhow::Result<()> {
    let mut input = File::open(src).with_context(|| format!("opening {:?}", src))?;
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])?;
        *aggregate_written = aggregate_written.saturating_add(n as u64);
        if let Some(handle) = progress {
            handle.report_bytes(*aggregate_written, aggregate_total);
        }
    }
    Ok(())
}

pub fn write_torrentzip_zip64(
    srcs: &[(&Path, &str)],
    dest: &Path,
    format: ZipFormat,
    progress: Option<&ActionProgressHandle>,
) -> anyhow::Result<()> {
    // If single entry, delegate to existing torrentzip writer for parity.
    if srcs.len() == 1 {
        let (src, name) = srcs[0];
        return crate::torrentzip::write_torrentzip(src, dest, name, format, progress);
    }

    // Multi-file stored writer (initial implementation without Zip64 extras).
    let mut out = File::create(dest).with_context(|| format!("creating {:?}", dest))?;

    let mut entries: Vec<Entry> = Vec::new();
    let aggregate_total = if progress.is_some() {
        let mut sum = 0u64;
        let mut known = true;
        for (src, _) in srcs.iter() {
            match std::fs::metadata(*src) {
                Ok(meta) => sum = sum.saturating_add(meta.len()),
                Err(_) => {
                    known = false;
                    break;
                }
            }
        }
        if known { Some(sum) } else { None }
    } else {
        None
    };
    let mut aggregate_written = 0u64;

    for (src, name) in srcs {
        let raw_name = encode_cp437(name)
            .with_context(|| format!("filename not CP437 encodable: {}", name))?;
        let (file_crc, file_len) = compute_crc32_and_size(src)?;
        let need_zip64_for_entry = file_len > u32::MAX as u64;

        // build local header into buffer (avoid rewrites)
        let mut lh: Vec<u8> = Vec::new();
        lh.extend_from_slice(&0x04034b50u32.to_le_bytes());
        lh.extend_from_slice(&20u16.to_le_bytes()); // version needed
        lh.extend_from_slice(&0u16.to_le_bytes()); // gp flag
        lh.extend_from_slice(&0u16.to_le_bytes()); // method (stored)
        lh.extend_from_slice(&0u16.to_le_bytes()); // mod time
        lh.extend_from_slice(&0u16.to_le_bytes()); // mod date
        lh.extend_from_slice(&file_crc.to_le_bytes());

        if need_zip64_for_entry {
            // placeholders in header
            lh.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // comp size
            lh.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // uncomp size
            lh.extend_from_slice(&(raw_name.len() as u16).to_le_bytes());
            // extra: Zip64 local extra field [id(2)][size(2)][uncomp(8)][comp(8)]
            let extra_len = 4u16 + 16u16; // header + two u64
            lh.extend_from_slice(&extra_len.to_le_bytes());
            lh.extend_from_slice(&raw_name);
            lh.extend_from_slice(&0x0001u16.to_le_bytes());
            lh.extend_from_slice(&16u16.to_le_bytes());
            lh.extend_from_slice(&file_len.to_le_bytes());
            lh.extend_from_slice(&file_len.to_le_bytes());
        } else {
            lh.extend_from_slice(&((file_len as u32)).to_le_bytes()); // comp size
            lh.extend_from_slice(&((file_len as u32)).to_le_bytes()); // uncomp size
            lh.extend_from_slice(&(raw_name.len() as u16).to_le_bytes());
            lh.extend_from_slice(&0u16.to_le_bytes()); // extra len
            lh.extend_from_slice(&raw_name);
        }

        // record local header offset (before writing)
        let local_header_offset = out.seek(SeekFrom::Current(0))?;
        out.write_all(&lh)?;
        stream_file_into(src, &mut out, progress, aggregate_total, &mut aggregate_written)?;

        entries.push(Entry {
            name: raw_name,
            crc32: file_crc,
            compressed_size: file_len,
            uncompressed_size: file_len,
            local_header_offset,
        });
    }

    // build central directory
    let _cd_offset = out.seek(SeekFrom::Current(0))? as u64;
    let mut central_dir: Vec<u8> = Vec::new();
    // determine if we need Zip64 overall
    let _need_zip64 = entries.len() > 0xFFFF
        || entries.iter().any(|e| {
            e.uncompressed_size > 0xFFFF_FFFF
                || e.compressed_size > 0xFFFF_FFFF
                || e.local_header_offset > 0xFFFF_FFFF
        });

    for e in &entries {
        central_dir.extend_from_slice(&0x02014b50u32.to_le_bytes());
        central_dir.extend_from_slice(&20u16.to_le_bytes()); // ver made
        central_dir.extend_from_slice(&20u16.to_le_bytes()); // ver needed
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // gp flag
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // method
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // mtime
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // mdate
        central_dir.extend_from_slice(&e.crc32.to_le_bytes());

        if e.uncompressed_size > 0xFFFF_FFFF || e.compressed_size > 0xFFFF_FFFF {
            central_dir.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
            central_dir.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        } else {
            central_dir.extend_from_slice(&(e.compressed_size as u32).to_le_bytes());
            central_dir.extend_from_slice(&(e.uncompressed_size as u32).to_le_bytes());
        }

        central_dir.extend_from_slice(&(e.name.len() as u16).to_le_bytes());

        // prepare extra field: possibly Zip64 extra
        let mut extra_field: Vec<u8> = Vec::new();
        if e.uncompressed_size > 0xFFFF_FFFF
            || e.compressed_size > 0xFFFF_FFFF
            || e.local_header_offset > 0xFFFF_FFFF
        {
            // Zip64 extra: id 0x0001, size depends on presence of fields (we include uncompr, compr, offset)
            extra_field.extend_from_slice(&0x0001u16.to_le_bytes());
            extra_field.extend_from_slice(&24u16.to_le_bytes()); // 3 * 8 bytes
            extra_field.extend_from_slice(&(e.uncompressed_size as u64).to_le_bytes());
            extra_field.extend_from_slice(&(e.compressed_size as u64).to_le_bytes());
            extra_field.extend_from_slice(&(e.local_header_offset as u64).to_le_bytes());
        }

        central_dir.extend_from_slice(&(extra_field.len() as u16).to_le_bytes()); // extra len
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // comment len
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // disk start
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // int attrs
        central_dir.extend_from_slice(&0u32.to_le_bytes()); // ext attrs

        if e.local_header_offset > 0xFFFF_FFFF {
            central_dir.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        } else {
            central_dir.extend_from_slice(&(e.local_header_offset as u32).to_le_bytes());
        }

        central_dir.extend_from_slice(&e.name);
        if !extra_field.is_empty() {
            central_dir.extend_from_slice(&extra_field);
        }
    }

    // delegate to helper that can be used by unit tests
    write_central_and_eocd_to(&mut out, &entries)?;
    out.flush()?;

    Ok(())
}

pub(crate) fn write_central_and_eocd_to<W: Write + Seek>(
    out: &mut W,
    entries: &[Entry],
) -> anyhow::Result<()> {
    // build central directory
    let cd_offset = out.seek(SeekFrom::Current(0))? as u64;
    let mut central_dir: Vec<u8> = Vec::new();
    // determine if we need Zip64 overall
    let need_zip64 = entries.len() > 0xFFFF
        || entries.iter().any(|e| {
            e.uncompressed_size > 0xFFFF_FFFF
                || e.compressed_size > 0xFFFF_FFFF
                || e.local_header_offset > 0xFFFF_FFFF
        });

    for e in entries {
        central_dir.extend_from_slice(&0x02014b50u32.to_le_bytes());
        central_dir.extend_from_slice(&20u16.to_le_bytes()); // ver made
        central_dir.extend_from_slice(&20u16.to_le_bytes()); // ver needed
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // gp flag
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // method
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // mtime
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // mdate
        central_dir.extend_from_slice(&e.crc32.to_le_bytes());

        if e.uncompressed_size > 0xFFFF_FFFF || e.compressed_size > 0xFFFF_FFFF {
            central_dir.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
            central_dir.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        } else {
            central_dir.extend_from_slice(&(e.compressed_size as u32).to_le_bytes());
            central_dir.extend_from_slice(&(e.uncompressed_size as u32).to_le_bytes());
        }

        central_dir.extend_from_slice(&(e.name.len() as u16).to_le_bytes());

        // prepare extra field: possibly Zip64 extra
        let mut extra_field: Vec<u8> = Vec::new();
        if e.uncompressed_size > 0xFFFF_FFFF
            || e.compressed_size > 0xFFFF_FFFF
            || e.local_header_offset > 0xFFFF_FFFF
        {
            // Zip64 extra: id 0x0001, size depends on presence of fields (we include uncompr, compr, offset)
            extra_field.extend_from_slice(&0x0001u16.to_le_bytes());
            extra_field.extend_from_slice(&24u16.to_le_bytes()); // 3 * 8 bytes
            extra_field.extend_from_slice(&(e.uncompressed_size as u64).to_le_bytes());
            extra_field.extend_from_slice(&(e.compressed_size as u64).to_le_bytes());
            extra_field.extend_from_slice(&(e.local_header_offset as u64).to_le_bytes());
        }

        central_dir.extend_from_slice(&(extra_field.len() as u16).to_le_bytes()); // extra len
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // comment len
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // disk start
        central_dir.extend_from_slice(&0u16.to_le_bytes()); // int attrs
        central_dir.extend_from_slice(&0u32.to_le_bytes()); // ext attrs

        if e.local_header_offset > 0xFFFF_FFFF {
            central_dir.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        } else {
            central_dir.extend_from_slice(&(e.local_header_offset as u32).to_le_bytes());
        }

        central_dir.extend_from_slice(&e.name);
        if !extra_field.is_empty() {
            central_dir.extend_from_slice(&extra_field);
        }
    }

    // compute central dir CRC
    let mut cdh_hasher = Crc32::new();
    cdh_hasher.update(&central_dir);
    let cdfh_crc = cdh_hasher.finalize();
    let cdfh_crc_hex = format!("{:08X}", cdfh_crc);

    // write central dir
    out.write_all(&central_dir)?;

    let cd_size_u64 = central_dir.len() as u64;
    let cd_offset_u64 = cd_offset as u64;

    // If Zip64 is required, write Zip64 EOCD and locator
    if need_zip64 {
        let zip64_eocd_offset = out.seek(SeekFrom::Current(0))? as u64;
        // Zip64 EOCD
        out.write_all(&0x06064b50u32.to_le_bytes())?; // signature
        out.write_all(&44u64.to_le_bytes())?; // size of remaining record
        out.write_all(&45u16.to_le_bytes())?; // version made by
        out.write_all(&45u16.to_le_bytes())?; // version needed
        out.write_all(&0u32.to_le_bytes())?; // disk number
        out.write_all(&0u32.to_le_bytes())?; // disk where cd starts
        out.write_all(&(entries.len() as u64).to_le_bytes())?; // total entries this disk
        out.write_all(&(entries.len() as u64).to_le_bytes())?; // total entries
        out.write_all(&cd_size_u64.to_le_bytes())?; // size of central dir
        out.write_all(&cd_offset_u64.to_le_bytes())?; // offset of cd

        // Zip64 EOCD locator
        out.write_all(&0x07064b50u32.to_le_bytes())?; // locator sig
        out.write_all(&0u32.to_le_bytes())?; // number of disk with start of zip64 eocd
        out.write_all(&zip64_eocd_offset.to_le_bytes())?; // relative offset of zip64 eocd
        out.write_all(&1u32.to_le_bytes())?; // total number of disks
    }

    // regular EOCD (with 0xFFFF/0xFFFFFFFF placeholders if Zip64)
    out.write_all(&0x06054b50u32.to_le_bytes())?;
    out.write_all(&0u16.to_le_bytes())?; // disk num
    out.write_all(&0u16.to_le_bytes())?; // start disk
    if entries.len() > 0xFFFF {
        out.write_all(&0xFFFFu16.to_le_bytes())?;
        out.write_all(&0xFFFFu16.to_le_bytes())?;
    } else {
        out.write_all(&((entries.len() as u16).to_le_bytes()))?;
        out.write_all(&((entries.len() as u16).to_le_bytes()))?;
    }

    if cd_size_u64 > 0xFFFF_FFFF {
        out.write_all(&0xFFFF_FFFFu32.to_le_bytes())?;
    } else {
        out.write_all(&(cd_size_u64 as u32).to_le_bytes())?;
    }

    if cd_offset_u64 > 0xFFFF_FFFF {
        out.write_all(&0xFFFF_FFFFu32.to_le_bytes())?;
    } else {
        out.write_all(&(cd_offset_u64 as u32).to_le_bytes())?;
    }

    let comment = format!("TORRENTZIPPED-{}", cdfh_crc_hex);
    out.write_all(&(comment.len() as u16).to_le_bytes())?;
    out.write_all(comment.as_bytes())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn zip64_eocd_and_locator_emitted_for_large_entries() {
        // construct fake entries that require Zip64
        let entries = vec![Entry {
            name: b"large.bin".to_vec(),
            crc32: 0xDEADBEEFu32,
            compressed_size: 0x1_0000_0000u64, // > 0xFFFFFFFF
            uncompressed_size: 0x1_0000_0000u64,
            local_header_offset: 0x1_0000_0000u64,
        }];

        let mut buf = Cursor::new(Vec::new());
        // write central dir + EOCD to buffer
        write_central_and_eocd_to(&mut buf, &entries).expect("write failed");
        let bytes = buf.into_inner();

        // assert Zip64 EOCD signature (0x06064b50) exists
        assert!(bytes.windows(4).any(|w| w == 0x06064b50u32.to_le_bytes()));
        // assert Zip64 EOCD locator signature (0x07064b50) exists
        assert!(bytes.windows(4).any(|w| w == 0x07064b50u32.to_le_bytes()));
        // assert regular EOCD signature exists
        assert!(bytes.windows(4).any(|w| w == 0x06054b50u32.to_le_bytes()));
        // assert comment with TORRENTZIPPED- present (search raw bytes to avoid UTF-8 errors)
        assert!(
            bytes
                .windows(b"TORRENTZIPPED-".len())
                .any(|w| w == b"TORRENTZIPPED-")
        );
    }

    #[test]
    fn zip64_eocd_comment_crc_matches_central_dir() {
        // construct two fake entries to form a central directory
        let entries = vec![
            Entry {
                name: b"large1.bin".to_vec(),
                crc32: 0xAAAAAAAAu32,
                compressed_size: 0x1_0000_0000u64, // force Zip64
                uncompressed_size: 0x1_0000_0000u64,
                local_header_offset: 0x1_0000_0000u64,
            },
            Entry {
                name: b"large2.bin".to_vec(),
                crc32: 0xBBBBBBBBu32,
                compressed_size: 0x1_0000_0001u64,
                uncompressed_size: 0x1_0000_0001u64,
                local_header_offset: 0x1_0000_0010u64,
            },
        ];

        let mut buf = std::io::Cursor::new(Vec::new());
        write_central_and_eocd_to(&mut buf, &entries).expect("write failed");
        let bytes = buf.into_inner();

        // find Zip64 EOCD signature
        let sig_zip64 = 0x06064b50u32.to_le_bytes();
        let pos_zip64 = bytes
            .windows(4)
            .position(|w| w == sig_zip64)
            .expect("zip64 eocd not found");

        // compute offset positions per write_central_and_eocd_to layout
        let mut p = pos_zip64 + 4; // after signature
        // size of remaining (8)
        p += 8;
        // version made (2), version needed (2), disk number (4), disk where cd starts (4)
        p += 2 + 2 + 4 + 4;
        // total entries this disk (8), total entries (8)
        p += 8 + 8;
        // now p points to cd size (8)
        let cd_size_bytes = &bytes[p..p + 8];
        let cd_size = u64::from_le_bytes(cd_size_bytes.try_into().unwrap());
        p += 8;
        let cd_offset_bytes = &bytes[p..p + 8];
        let cd_offset = u64::from_le_bytes(cd_offset_bytes.try_into().unwrap());

        // extract central dir bytes
        let cd_offset_usize = cd_offset as usize;
        let cd_size_usize = cd_size as usize;
        let central_dir = &bytes[cd_offset_usize..cd_offset_usize + cd_size_usize];

        // compute CRC of central dir
        let mut hasher = Crc32::new();
        hasher.update(central_dir);
        let crc = hasher.finalize();
        let crc_hex = format!("{:08X}", crc);

        // find regular EOCD (last occurrence)
        let sig_eocd = 0x06054b50u32.to_le_bytes();
        let pos_eocd = bytes
            .windows(4)
            .rposition(|w| w == sig_eocd)
            .expect("eocd not found");
        // comment length is at eocd + 20 (2 bytes)
        let comment_len_off = pos_eocd + 20;
        let comment_len = u16::from_le_bytes(
            bytes[comment_len_off..comment_len_off + 2]
                .try_into()
                .unwrap(),
        ) as usize;
        let comment_start = pos_eocd + 22;
        let comment = &bytes[comment_start..comment_start + comment_len];

        assert!(
            comment
                .windows(crc_hex.as_bytes().len())
                .any(|w| w == crc_hex.as_bytes()),
            "comment did not contain CRC {}",
            crc_hex
        );
    }
}
