use once_cell::sync::Lazy;
use std::collections::HashMap;

static IGDB_PLATFORM_MAP: Lazy<HashMap<String, &'static str>> = Lazy::new(|| {
    let mut map = HashMap::new();

    fn insert(map: &mut HashMap<String, &'static str>, token: &'static str, names: &[&str]) {
        for name in names {
            map.insert(normalize_identifier(name), token);
        }
    }

    insert(
        &mut map,
        "acorn-archimedes",
        &["Acorn Archimedes", "Archimedes"],
    );
    insert(&mut map, "atom", &["Acorn Atom", "Atom"]);
    insert(&mut map, "acpc", &["Amstrad CPC"]);
    insert(&mut map, "amstrad-pcw", &["Amstrad PCW"]);
    insert(&mut map, "appleii", &["Apple II"]);
    insert(&mut map, "amiga", &["Amiga", "Commodore Amiga"]);
    insert(&mut map, "amiga-cd32", &["Amiga CD32", "CD32"]);
    insert(&mut map, "commodore-cdtv", &["Commodore CDTV", "CDTV"]);
    insert(&mut map, "atari2600", &["Atari 2600", "VCS"]);
    insert(&mut map, "atari5200", &["Atari 5200"]);
    insert(&mut map, "atari7800", &["Atari 7800"]);
    insert(&mut map, "atari8bit", &["Atari 8-bit", "Atari 800"]);
    insert(&mut map, "atari-st", &["Atari ST"]);
    insert(&mut map, "lynx", &["Atari Lynx", "Lynx"]);
    insert(&mut map, "vectrex", &["Vectrex"]);
    insert(&mut map, "c64", &["Commodore 64", "C64"]);
    insert(&mut map, "pc-8800-series", &["PC-8800 Series", "PC-88"]);
    insert(&mut map, "pc-9800-series", &["PC-9800 Series", "PC-98"]);
    insert(
        &mut map,
        "fds",
        &["Famicom Disk System", "Disk System", "Nintendo Disk System"],
    );
    insert(&mut map, "g-and-w", &["Game & Watch", "Game and Watch"]);
    insert(&mut map, "64dd", &["Nintendo 64DD", "64DD"]);
    insert(
        &mut map,
        "nes",
        &["Nintendo Entertainment System", "NES", "Famicom"],
    );
    insert(
        &mut map,
        "snes",
        &[
            "Super Nintendo",
            "Super Nintendo Entertainment System",
            "SNES",
            "Super Famicom",
        ],
    );
    insert(&mut map, "gb", &["Game Boy", "GB"]);
    insert(&mut map, "gbc", &["Game Boy Color", "GBC"]);
    insert(&mut map, "gba", &["Game Boy Advance", "GBA"]);
    insert(&mut map, "n64", &["Nintendo 64", "N64"]);
    insert(&mut map, "ngc", &["Nintendo GameCube", "GameCube"]);
    insert(&mut map, "nds", &["Nintendo DS", "NDS"]);
    insert(&mut map, "3ds", &["Nintendo 3DS", "3DS"]);
    insert(&mut map, "switch", &["Nintendo Switch", "Switch"]);
    insert(&mut map, "wii", &["Nintendo Wii", "Wii"]);
    insert(&mut map, "wiiu", &["Nintendo Wii U", "Wii U", "WiiU"]);
    insert(&mut map, "virtualboy", &["Virtual Boy"]);
    insert(
        &mut map,
        "gamegear",
        &[
            "Game Gear",
            "Sega Game Gear",
            "GameGear",
            "GG",
            "Handheld Electronic LCD",
            "handheld-electronic-lcd",
        ],
    );
    insert(
        &mut map,
        "sms",
        &["Sega Master System", "Master System", "Mark III"],
    );
    insert(&mut map, "sega32", &["Sega 32X", "32X"]);
    insert(
        &mut map,
        "genesis-slash-megadrive",
        &["Mega Drive", "Sega Mega Drive", "Genesis", "Sega Genesis"],
    );
    insert(&mut map, "segacd", &["Sega CD", "Mega CD"]);
    insert(&mut map, "saturn", &["Sega Saturn", "Saturn"]);
    insert(&mut map, "sg1000", &["SG-1000", "Sega SG-1000"]);
    insert(&mut map, "dc", &["Dreamcast", "Sega Dreamcast"]);
    insert(&mut map, "turbografx16--1", &["TurboGrafx-16", "PC Engine"]);
    insert(&mut map, "philips-cd-i", &["Philips CD-i", "CD-i"]);
    insert(&mut map, "3do", &["3DO", "Panasonic 3DO"]);
    insert(&mut map, "neo-geo-pocket", &["Neo Geo Pocket", "NGP"]);
    insert(
        &mut map,
        "neo-geo-pocket-color",
        &["Neo Geo Pocket Color", "NGPC"],
    );
    insert(
        &mut map,
        "neogeomvs",
        &["Neo Geo", "Neo-Geo", "Neo Geo AES", "Neo Geo MVS"],
    );
    insert(&mut map, "colecovision", &["ColecoVision"]);
    insert(&mut map, "intellivision", &["Intellivision"]);
    insert(&mut map, "jaguar", &["Atari Jaguar", "Jaguar"]);
    insert(&mut map, "msx", &["MSX"]);
    insert(&mut map, "ti-994a", &["TI-99/4A", "TI 99/4A"]);
    insert(&mut map, "sharp-mz-2200", &["Sharp MZ", "Sharp MZ-2200"]);
    insert(&mut map, "sharp-x68000", &["Sharp X68000", "X68000"]);
    insert(&mut map, "zxs", &["ZX Spectrum", "Sinclair ZX Spectrum"]);
    insert(&mut map, "palm-os", &["Palm OS"]);
    insert(&mut map, "symbian", &["Symbian"]);
    insert(
        &mut map,
        "ps",
        &["PlayStation", "PlayStation 1", "PS", "PS1", "PSX"],
    );
    insert(&mut map, "ps2", &["PlayStation 2", "PS2"]);
    insert(&mut map, "ps3", &["PlayStation 3", "PS3"]);
    insert(&mut map, "psp", &["PlayStation Portable", "PSP"]);
    insert(&mut map, "psvita", &["PlayStation Vita", "PSV", "PS Vita"]);
    insert(&mut map, "xbox", &["Xbox", "Microsoft Xbox"]);
    insert(&mut map, "xbox360", &["Xbox 360", "Microsoft Xbox 360"]);

    map
});

static PLATFORM_DISPLAY_NAMES: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert("acorn-archimedes", "Acorn Archimedes");
    map.insert("acpc", "Amstrad CPC");
    map.insert("amiga", "Amiga");
    map.insert("amiga-cd32", "Amiga CD32");
    map.insert("commodore-cdtv", "Commodore CDTV");
    map.insert("atari2600", "Atari 2600");
    map.insert("atari5200", "Atari 5200");
    map.insert("atari7800", "Atari 7800");
    map.insert("atari-st", "Atari ST");
    map.insert("lynx", "Atari Lynx");
    map.insert("vectrex", "Vectrex");
    map.insert("c64", "Commodore 64");
    map.insert("pc-8800-series", "NEC PC-8801");
    map.insert("pc-9800-series", "NEC PC-9801");
    map.insert("fds", "Famicom Disk System");
    map.insert("g-and-w", "Game & Watch");
    map.insert("64dd", "Nintendo 64DD");
    map.insert("nes", "Nintendo Entertainment System");
    map.insert("snes", "Super Nintendo");
    map.insert("gb", "Game Boy");
    map.insert("gbc", "Game Boy Color");
    map.insert("gba", "Game Boy Advance");
    map.insert("n64", "Nintendo 64");
    map.insert("ngc", "Nintendo GameCube");
    map.insert("nds", "Nintendo DS");
    map.insert("3ds", "Nintendo 3DS");
    map.insert("switch", "Nintendo Switch");
    map.insert("wii", "Nintendo Wii");
    map.insert("wiiu", "Nintendo Wii U");
    map.insert("virtualboy", "Virtual Boy");
    map.insert("gamegear", "Game Gear");
    map.insert("sms", "Sega Master System");
    map.insert("sega32", "Sega 32X");
    map.insert("genesis-slash-megadrive", "Sega Mega Drive");
    map.insert("segacd", "Sega CD");
    map.insert("saturn", "Sega Saturn");
    map.insert("sg1000", "SG-1000");
    map.insert("dc", "Dreamcast");
    map.insert("turbografx16--1", "TurboGrafx-16");
    map.insert("philips-cd-i", "Philips CD-i");
    map.insert("3do", "3DO");
    map.insert("neo-geo-pocket", "Neo Geo Pocket");
    map.insert("neo-geo-pocket-color", "Neo Geo Pocket Color");
    map.insert("neogeomvs", "Neo Geo");
    map.insert("colecovision", "ColecoVision");
    map.insert("intellivision", "Intellivision");
    map.insert("jaguar", "Atari Jaguar");
    map.insert("msx", "MSX");
    map.insert("ti-994a", "TI-99/4A");
    map.insert("sharp-mz-2200", "Sharp MZ");
    map.insert("sharp-x68000", "Sharp X68000");
    map.insert("zxs", "ZX Spectrum");
    map.insert("ps", "PlayStation");
    map.insert("ps2", "PlayStation 2");
    map.insert("ps3", "PlayStation 3");
    map.insert("psp", "PlayStation Portable");
    map.insert("psvita", "PlayStation Vita");
    map.insert("xbox", "Xbox");
    map.insert("xbox360", "Xbox 360");
    map
});

static PLATFORM_SLUGS: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert("gba", "gba");
    map.insert("gbc", "gbc");
    map.insert("gb", "gb");
    map.insert("nes", "nes");
    map.insert("snes", "snes");
    map.insert("n64", "n64");
    map.insert("ngc", "gamecube");
    map.insert("nds", "nds");
    map.insert("3ds", "3ds");
    map.insert("switch", "nintendo-switch");
    map.insert("wii", "wii");
    map.insert("wiiu", "wii-u");
    map.insert("virtualboy", "virtual-boy");
    map.insert("gamegear", "game-gear");
    map.insert("sms", "master-system");
    map.insert("sega32", "sega-32x");
    map.insert("genesis-slash-megadrive", "sega-mega-drive");
    map.insert("segacd", "sega-cd");
    map.insert("saturn", "saturn");
    map.insert("sg1000", "sg-1000");
    map.insert("dc", "dreamcast");
    map.insert("turbografx16--1", "pc-engine");
    map.insert("ps", "playstation");
    map.insert("ps2", "playstation-2");
    map.insert("ps3", "playstation-3");
    map.insert("psp", "psp");
    map.insert("psvita", "ps-vita");
    map.insert("xbox", "xbox");
    map.insert("xbox360", "xbox-360");
    map
});

fn normalize_identifier(input: &str) -> String {
    if input.trim().is_empty() {
        return String::new();
    }

    let lowered = input.trim().to_ascii_lowercase();
    let mut buf = String::with_capacity(lowered.len());
    for ch in lowered.chars() {
        match ch {
            '-' | '_' | '/' | '\\' | '.' | ',' => buf.push(' '),
            '&' => buf.push(' '),
            _ => buf.push(ch),
        }
    }

    buf.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Attempt to map an IGDB platform identifier (name, slug, abbreviation) to a RomM token.
/// Only tokens already defined in `game_console.rs` are returned.
pub fn lookup(identifier: &str) -> Option<&'static str> {
    if identifier.trim().is_empty() {
        return None;
    }
    let key = normalize_identifier(identifier);
    if key.is_empty() {
        return None;
    }
    IGDB_PLATFORM_MAP.get(&key).copied()
}

/// Return a canonical human-readable platform name for the provided RomM token.
pub fn display_name(token: &str) -> Option<&'static str> {
    PLATFORM_DISPLAY_NAMES.get(token).copied()
}

/// Return the canonical IGDB slug for a RomM token when known.
pub fn slug(token: &str) -> Option<&'static str> {
    PLATFORM_SLUGS.get(token).copied()
}

#[cfg(test)]
mod tests {
    use super::{lookup, slug};

    #[test]
    fn matches_game_gear_variants() {
        assert_eq!(lookup("Game Gear"), Some("gamegear"));
        assert_eq!(lookup("game-gear"), Some("gamegear"));
        assert_eq!(lookup("Handheld Electronic LCD"), Some("gamegear"));
    }

    #[test]
    fn returns_none_for_unknown_platforms() {
        assert!(lookup("V.Smile").is_none());
    }

    #[test]
    fn returns_slug_when_available() {
        assert_eq!(slug("gba"), Some("gba"));
        assert_eq!(slug("ngc"), Some("gamecube"));
    }
}
