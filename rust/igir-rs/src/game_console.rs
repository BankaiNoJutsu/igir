use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::path::Path;

// Mapping from file extension (lowercase, without dot) to RomM token
pub static EXT_MAP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();
    // Derived from gameConsole.ts: common extensions -> RomM tokens
    let pairs = [
        ("sfc", "snes"),
        ("smc", "snes"),
        ("snes", "snes"),
        ("fig", "snes"),
        ("nes", "nes"),
        ("fc", "nes"),
        ("nez", "nes"),
        ("gba", "gba"),
        ("gb", "gb"),
        ("sgb", "gb"),
        ("gbc", "gbc"),
        ("d64", "n64"),
        ("n64", "n64"),
        ("v64", "n64"),
        ("z64", "n64"),
        ("3ds", "3ds"),
        ("3dsx", "3ds"),
        ("cci", "3ds"),
        ("cia", "3ds"),
        ("nds", "nds"),
        ("dsi", "nintendo-dsi"),
        ("gcm", "ngc"),
        ("gcz", "ngc"),
        ("iso", "cdrom"),
        ("bin", "cdrom"),
        ("cue", "cdrom"),
        ("pbp", "ps"),
        ("psx", "ps"),
        ("psexe", "ps"),
        ("psp", "psp"),
        ("psvita", "psvita"),
        ("ps3", "ps3"),
        ("nsp", "switch"),
        ("xci", "switch"),
        ("nro", "switch"),
        ("nso", "switch"),
        ("md", "genesis-slash-megadrive"),
        ("gen", "genesis-slash-megadrive"),
        ("smd", "genesis-slash-megadrive"),
        ("mdx", "genesis-slash-megadrive"),
        ("32x", "sega32"),
        ("sms", "sms"),
        ("gg", "gamegear"),
        ("sgx", "supergrafx"),
        ("pce", "turbografx16--1"),
        ("sg", "sg1000"),
        ("sc", "sg1000"),
        ("min", "pokemon-mini"),
        ("tic", "tic80"),
        ("vb", "virtualboy"),
        ("vboy", "virtualboy"),
        ("gba", "gba"),
        ("mgw", "g-and-w"),
        ("int", "intellivision"),
        ("a26", "atari2600"),
        ("a52", "atari5200"),
        ("a78", "atari7800"),
        ("j64", "jaguar"),
        ("lnx", "lynx"),
        ("lyx", "lynx"),
        ("crt", "c64"),
        ("d88", "pc-8800-series"),
        ("d98", "pc-9800-series"),
        ("rpk", "ti-994a"),
    ];

    for (k, v) in pairs {
        m.insert(k, v);
    }
    m
});

static DISC_BASED_ROMM_TOKENS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    let tokens = [
        "3do",
        "amiga-cd32",
        "cdrom",
        "commodore-cdtv",
        "dc",
        "neo-geo-cd",
        "ngc",
        "pc-fx",
        "philips-cd-i",
        "ps",
        "ps2",
        "ps3",
        "psp",
        "saturn",
        "segacd",
        "turbografx-16-slash-pc-engine-cd",
        "wii",
        "wiiu",
        "xbox",
        "xbox360",
    ];

    let mut set = HashSet::new();
    for token in tokens {
        set.insert(token);
    }
    set
});

// Mapping from DAT name regex -> RomM token. Use a small but useful set of regexes.
pub static DAT_PATTERNS: Lazy<Vec<(Regex, &'static str)>> = Lazy::new(|| {
    let mut v = Vec::new();
    macro_rules! r {
        ($pat:expr, $token:expr) => {
            v.push((Regex::new($pat).unwrap(), $token));
        };
    }
    // Derived from the TS CONSOLES datRegexes: many specific patterns
    r!(r"(?i)Archimedes|Archie|Archimedes", "acorn-archimedes");
    r!(r"(?i)Atom", "atom");
    r!(r"(?i)CPC|Amstrad", "acpc");
    r!(r"(?i)PCW", "amstrad-pcw");
    r!(r"(?i)Apple.*II|Apple II", "appleii");
    r!(r"(?i)Amiga", "amiga");
    r!(r"(?i)Atari.*ST|Atari ST", "atari-st");
    r!(r"(?i)2600|A2600", "atari2600");
    r!(r"(?i)5200|A5200", "atari5200");
    r!(r"(?i)7800|A7800", "atari7800");
    r!(r"(?i)Jaguar", "jaguar");
    r!(r"(?i)Lynx", "lynx");
    r!(r"(?i)Vectrex", "vectrex");
    r!(r"(?i)PC[ -]?88|PC88", "pc-8800-series");
    r!(r"(?i)PC-98|PC98", "pc-9800-series");
    r!(r"(?i)FDS|Famicom Disk|Disk System", "fds");
    r!(r"(?i)Game (and|&) Watch|Game.?Watch", "g-and-w");
    r!(r"(?i)GameCube|GCM|GC|NGC", "ngc");
    r!(r"(?i)Game ?Boy Advance|GBA", "gba");
    r!(r"(?i)Game ?Boy Color|GBC", "gbc");
    r!(r"(?i)Game ?Boy|GB\b", "gb");
    r!(r"(?i)Nintendo 64|N64", "n64");
    r!(r"(?i)Nintendo 64DD|64DD", "64dd");
    r!(r"(?i)3DS|Nintendo 3DS", "3ds");
    r!(r"(?i)NDS|Nintendo DS", "nds");
    r!(
        r"(?i)SNES|Super Nintendo|Super Nintendo Entertainment System|Famicom",
        "snes"
    );
    r!(r"(?i)NES|Famicom|Nintendo Entertainment System", "nes");
    r!(r"(?i)Switch|Nintendo Switch", "switch");
    r!(r"(?i)Virtual Boy", "virtualboy");
    r!(r"(?i)WiiU|Wii U", "wiiu");
    r!(r"(?i)Wii", "wii");
    r!(r"(?i)3DO", "3do");
    r!(r"(?i)CDI|CD-i", "philips-cd-i");
    r!(r"(?i)Mega Drive|Genesis", "genesis-slash-megadrive");
    r!(r"(?i)Saturn", "saturn");
    r!(r"(?i)SG[ -]?1000|SG-1000", "sg1000");
    r!(r"(?i)Neo ?Geo Pocket|NGP", "neo-geo-pocket");
    r!(r"(?i)Neo ?Geo Pocket Color|NGPC", "neo-geo-pocket-color");
    r!(r"(?i)PlayStation|PSX|PS1", "ps");
    r!(r"(?i)PlayStation 2|PS2", "ps2");
    r!(r"(?i)PlayStation 3|PS3", "ps3");
    r!(r"(?i)PlayStation Portable|PSP", "psp");
    r!(r"(?i)PlayStation Vita|PSVita", "psvita");
    r!(r"(?i)PC Engine|TurboGrafx|TG16", "turbografx16--1");
    r!(r"(?i)Amstrad|CPC", "acpc");
    r!(r"(?i)MSX", "msx");
    r!(r"(?i)Intellivision", "intellivision");
    r!(r"(?i)Atari 800|8-bit Family", "atari8bit");
    r!(r"(?i)Master System|Mastersystem", "sms");
    r!(r"(?i)Game Gear", "gamegear");
    r!(r"(?i)Dreamcast", "dc");
    r!(r"(?i)Segacd|Mega CD|Sega CD", "segacd");
    r!(r"(?i)Neo ?Geo|Neogeo", "neogeomvs");
    r!(r"(?i)ColecoVision", "colecovision");
    r!(r"(?i)Intellivision", "intellivision");
    r!(r"(?i)Vectrex", "vectrex");
    r!(r"(?i)TI ?99|TI-99", "ti-994a");
    r!(r"(?i)Xbox 360|Xbox360", "xbox360");
    r!(r"(?i)Xbox", "xbox");
    r!(r"(?i)Palm OS|Palm", "palm-os");
    r!(r"(?i)Symbian", "symbian");
    r!(r"(?i)Amiga CD32|CD32", "amiga-cd32");
    r!(r"(?i)Amiga CDTV|CDTV", "commodore-cdtv");
    r!(r"(?i)C64|Commodore", "c64");
    r!(r"(?i)Sharp MZ|MZ", "sharp-mz-2200");
    r!(r"(?i)X68000", "sharp-x68000");
    r!(r"(?i)ZX Spectrum|ZX[ -]?Spectrum", "zxs");
    r!(r"(?i)Neo ?Geo Pocket Color|NGPC", "neo-geo-pocket-color");
    r!(r"(?i)3DS|Nintendo 3DS", "3ds");
    v
});

/// Try to determine a RomM token from DAT name (more authoritative) or file extension.
pub fn romm_from_dat_name(dat_name: &str) -> Option<String> {
    for (re, token) in DAT_PATTERNS.iter() {
        if re.is_match(dat_name) {
            return Some(token.to_string());
        }
    }
    None
}

pub fn romm_from_dat(dat: &DatRom) -> Option<String> {
    if let Some(token) = romm_from_dat_name(&dat.name) {
        return Some(token);
    }

    if let Some(desc) = dat.description.as_deref() {
        if let Some(token) = romm_from_dat_name(desc) {
            return Some(token);
        }
    }

    if let Some(fname) = dat.source_dat.file_name().and_then(|f| f.to_str()) {
        if let Some(token) = romm_from_dat_name(fname) {
            return Some(token);
        }
    }

    None
}

/// Map a free-form platform name (from online sources) to a RomM token.
/// This is a thin wrapper around `romm_from_dat_name` which already contains
/// a broad set of regex patterns for common platform names.
pub fn romm_from_platform_name(platform_name: &str) -> Option<String> {
    romm_from_dat_name(platform_name)
}

pub fn romm_from_extension(path: &Path) -> Option<String> {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let key = ext.to_ascii_lowercase();
        if let Some(tok) = EXT_MAP.get(key.as_str()) {
            return Some(tok.to_string());
        }
    }
    None
}

fn looks_like_disc_extension(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let ext = ext.to_ascii_lowercase();
        const DISC_EXTS: &[&str] = &[
            "iso", "cue", "img", "ccd", "mds", "mdf", "nrg", "uif", "cso", "wbfs", "wia", "rvz",
            "gcm", "gcz", "chd",
        ];

        // Treat .bin as disc-based only when paired with well-known optical formats. Without
        // additional context, many cartridge systems also use .bin, so only classify as disc
        // when the filename suggests a cue/bin style bundle.
        if ext == "bin" {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.to_ascii_lowercase().contains("disc")
                || name.to_ascii_lowercase().contains("track")
            {
                return true;
            }
            return false;
        }

        return DISC_EXTS.iter().any(|candidate| ext == **candidate);
    }
    false
}

pub fn is_disc_based_romm(token: &str) -> bool {
    DISC_BASED_ROMM_TOKENS.contains(token)
}

pub fn record_is_cartridge_based(record: &FileRecord, dats: Option<&[DatRom]>) -> bool {
    if let Some(token) = romm_for_record(record, dats) {
        return !is_disc_based_romm(&token);
    }

    if let Some(info) = &record.scan_info {
        if info.is_iso || info.is_cue || info.is_chd || info.is_pbp {
            return false;
        }
    }

    if looks_like_disc_extension(&record.source) || looks_like_disc_extension(&record.relative) {
        return false;
    }

    true
}

use crate::dat::DatRom;
use crate::types::FileRecord;

/// Given a FileRecord and optional DatRom list, prefer DAT-derived mapping; fall back to extension.
pub fn romm_for_record(record: &FileRecord, dats: Option<&[DatRom]>) -> Option<String> {
    if let Some(platform) = &record.derived_platform {
        return Some(platform.clone());
    }

    // If we have DATs, try to find a matching dat ROM (checksum preferred)
    if let Some(dats) = dats {
        // try exact checksum matches first
        for dat in dats.iter() {
            if let Some(sha1) = &dat.sha1 {
                if record.checksums.sha1.as_deref() == Some(sha1.as_str()) {
                    if let Some(token) = romm_from_dat(dat) {
                        return Some(token);
                    }
                }
            }
            if let Some(md5) = &dat.md5 {
                if record.checksums.md5.as_deref() == Some(md5.as_str()) {
                    if let Some(token) = romm_from_dat(dat) {
                        return Some(token);
                    }
                }
            }
            if let Some(crc) = &dat.crc32 {
                if record
                    .checksums
                    .crc32
                    .as_deref()
                    .is_some_and(|c| c.eq_ignore_ascii_case(crc))
                {
                    if let Some(token) = romm_from_dat(dat) {
                        return Some(token);
                    }
                }
            }
        }
        // fallback: name-based match
        for dat in dats.iter() {
            if let Some(rel) = record.relative.file_name().and_then(|s| s.to_str()) {
                if dat.name.to_lowercase().contains(&rel.to_lowercase()) {
                    if let Some(token) = romm_from_dat(dat) {
                        return Some(token);
                    }
                }
            }
        }
    }

    // Fallback: use extension mapping
    // If we have scan info, consult it for ambiguous disk-image cases (cue/bin/iso)
    if let Some(info) = &record.scan_info {
        // If this is a PBP or PS-X EXE, it's likely PlayStation
        if info.is_pbp || info.is_psx_exe {
            return Some("ps".to_string());
        }
        // If this is a CUE sheet, attempt to parse referenced filename and use its extension
        if info.is_cue {
            if let Ok(txt) = std::fs::read_to_string(&record.source) {
                // crude parse: look for FILE "name.bin" or FILE name.bin
                let re = regex::Regex::new("(?i)FILE\\s+\"?([^\"\\s]+)\"?").unwrap();
                if let Some(cap) = re.captures(&txt) {
                    if let Some(fname) = cap.get(1) {
                        let p = std::path::Path::new(fname.as_str());
                        if let Some(ext) = p.extension().and_then(|e| e.to_str()) {
                            let key = ext.to_ascii_lowercase();
                            if let Some(tok) = EXT_MAP.get(key.as_str()) {
                                return Some((*tok).to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    romm_from_extension(&record.relative)
}

/// Provide common output token values (e.g. {es}, {batocera}, {mister}, etc.)
/// We prefer DAT-derived mapping (using romm_for_record) and fall back to extension mapping.
pub fn output_token_for(
    token: &str,
    record: &FileRecord,
    dats: Option<&[DatRom]>,
) -> Option<String> {
    // derive a base platform token (romm) first
    let romm = romm_for_record(record, dats).or_else(|| romm_from_extension(&record.relative))?;

    // Many of the output tokens in the original Node implementation map to a small
    // set of console identifiers. For a pragmatic, backwards-compatible approach
    // return the romm token for most tokens. This mirrors the Node behaviour for
    // the common cases where the value is the platform short-name.
    match token.to_ascii_lowercase().as_str() {
        "romm" | "platform" => Some(romm),
        // Emulator-specific folders that generally align with the platform token
        "es" | "batocera" | "retrodeck" | "onion" | "mister" | "pocket" | "adam" | "minui"
        | "twmenu" | "funkeyos" | "jelos" | "miyoocfw" => Some(romm),
        // fall back to romm as a safe default for unknown tokens as well
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dat::DatRom;
    use crate::types::ChecksumSet;
    use crate::types::FileRecord;
    use std::path::PathBuf;

    fn record_named(name: &str) -> FileRecord {
        FileRecord {
            source: PathBuf::from(name),
            relative: PathBuf::from(name),
            size: 0,
            checksums: ChecksumSet {
                crc32: None,
                md5: None,
                sha1: None,
                sha256: None,
            },
            letter_dir: None,
            derived_platform: None,
            derived_genres: Vec::new(),
            derived_region: None,
            derived_languages: Vec::new(),
            scan_info: None,
        }
    }

    #[test]
    fn romm_from_dat_falls_back_to_dat_filename() {
        let dat = DatRom {
            name: "Addams Family, The (World).gg".to_string(),
            description: None,
            source_dat: PathBuf::from("/tmp/Sega - Game Gear (20251118-005324).dat"),
            size: None,
            crc32: None,
            md5: None,
            sha1: None,
            sha256: None,
            match_reasons: None,
        };

        let derived = romm_from_dat(&dat);
        assert_eq!(derived.as_deref(), Some("gamegear"));
    }

    #[test]
    fn record_is_cartridge_based_defaults_to_true() {
        let record = record_named("Super Mario World.sfc");
        assert!(record_is_cartridge_based(&record, None));
    }

    #[test]
    fn record_is_cartridge_based_detects_disc_extension() {
        let record = record_named("Parasite Eve.iso");
        assert!(!record_is_cartridge_based(&record, None));
    }

    #[test]
    fn record_is_cartridge_based_uses_dat_platforms() {
        let mut record = record_named("Parasite Eve.bin");
        record.checksums.sha1 = Some("deadbeef".to_string());

        let dat = DatRom {
            name: "Sony PlayStation".to_string(),
            description: None,
            source_dat: PathBuf::from("ps.dat"),
            size: None,
            crc32: None,
            md5: None,
            sha1: Some("deadbeef".to_string()),
            sha256: None,
            match_reasons: None,
        };

        assert!(!record_is_cartridge_based(&record, Some(&[dat])));
    }

    #[test]
    fn record_is_cartridge_based_prefers_derived_platform_over_extension() {
        let mut record = record_named("Sonic the Hedgehog.bin");
        record.derived_platform = Some("genesis-slash-megadrive".to_string());

        assert!(record_is_cartridge_based(&record, None));
    }
}
