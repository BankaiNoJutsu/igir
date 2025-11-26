use std::path::PathBuf;

use anyhow::Context;
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::blocking::Client;
use serde::Serialize;

use crate::config::Config;
use crate::records::collect_files;
use crate::types::FileRecord;

#[derive(Debug, Clone, Serialize)]
pub struct DatRom {
    pub name: String,
    pub description: Option<String>,
    pub source_dat: PathBuf,
    pub size: Option<u64>,
    pub crc32: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OnlineMatch {
    pub name: String,
    pub source_dat: PathBuf,
    pub hasheous: Option<serde_json::Value>,
    pub igdb: Option<serde_json::Value>,
}

pub fn load_dat_roms(config: &Config) -> anyhow::Result<Vec<DatRom>> {
    let mut roms = Vec::new();

    for dat_path in &config.dat {
        let mut reader = Reader::from_file(dat_path)
            .with_context(|| format!("unable to open DAT file: {}", dat_path.to_string_lossy()))?;
        reader.trim_text(true);
        let mut buf = Vec::new();

        let mut current_description: Option<String> = None;
        let mut in_description = false;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e))
                    if e.name().as_ref() == b"game" || e.name().as_ref() == b"machine" =>
                {
                    current_description = e
                        .attributes()
                        .filter_map(Result::ok)
                        .find(|a| a.key.as_ref() == b"name")
                        .and_then(|a| String::from_utf8(a.value.into_owned()).ok());
                }
                Ok(Event::Start(ref e)) if e.name().as_ref() == b"description" => {
                    in_description = true;
                }
                Ok(Event::Text(e)) if in_description => {
                    current_description = Some(e.unescape().unwrap_or_default().to_string());
                    in_description = false;
                }
                Ok(Event::Empty(ref e)) if e.name().as_ref() == b"rom" => {
                    let mut rom = DatRom {
                        name: String::new(),
                        description: current_description.clone(),
                        source_dat: dat_path.clone(),
                        size: None,
                        crc32: None,
                        md5: None,
                        sha1: None,
                        sha256: None,
                    };

                    for attr in e.attributes().flatten() {
                        let key = attr.key.as_ref();
                        let value = String::from_utf8_lossy(&attr.value).to_string();
                        match key {
                            b"name" => rom.name = value,
                            b"size" => rom.size = value.parse().ok(),
                            b"crc" => rom.crc32 = Some(value.to_ascii_uppercase()),
                            b"md5" => rom.md5 = Some(value.to_ascii_lowercase()),
                            b"sha1" => rom.sha1 = Some(value.to_ascii_lowercase()),
                            b"sha256" => rom.sha256 = Some(value.to_ascii_lowercase()),
                            _ => {}
                        }
                    }

                    roms.push(rom);
                }
                Ok(Event::Eof) => break,
                _ => {}
            }
            buf.clear();
        }
    }

    Ok(roms)
}

fn rom_matches(record: &FileRecord, dat: &DatRom) -> bool {
    if let Some(sha1) = &dat.sha1 {
        if record.checksums.sha1.as_deref() == Some(sha1.as_str()) {
            return true;
        }
    }
    if let Some(md5) = &dat.md5 {
        if record.checksums.md5.as_deref() == Some(md5.as_str()) {
            return true;
        }
    }
    if let Some(crc) = &dat.crc32 {
        if record
            .checksums
            .crc32
            .as_deref()
            .is_some_and(|c| c.eq_ignore_ascii_case(crc))
        {
            return true;
        }
    }

    if let Some(size) = dat.size {
        if record.size == size {
            if let Some(name) = record.relative.file_name().and_then(|n| n.to_str()) {
                return name == dat.name;
            }
        }
    }

    false
}

pub fn dat_unmatched(records: &[FileRecord], dat_roms: &[DatRom]) -> (Vec<DatRom>, usize) {
    let mut matched = 0usize;
    let mut unmatched = Vec::new();

    for dat in dat_roms {
        if records.iter().any(|record| rom_matches(record, dat)) {
            matched += 1;
        } else {
            unmatched.push(dat.clone());
        }
    }

    (unmatched, matched)
}

fn query_hasheous(hash: &str) -> anyhow::Result<Option<serde_json::Value>> {
    let url = format!("https://hasheous.com/api/v1/hash/{hash}");
    let response = reqwest::blocking::get(&url)?;
    if response.status().is_success() {
        return Ok(Some(response.json()?));
    }

    Ok(None)
}

fn query_igdb(
    name: &str,
    config: &Config,
    client: &Client,
) -> anyhow::Result<Option<serde_json::Value>> {
    let Some(client_id) = &config.igdb_client_id else {
        return Ok(None);
    };
    let Some(token) = &config.igdb_token else {
        return Ok(None);
    };

    let body = format!(
        "search \"{}\"; fields name,summary,first_release_date,platforms; limit 1;",
        name
    );
    let response = client
        .post("https://api.igdb.com/v4/games")
        .header("Client-ID", client_id)
        .header("Authorization", format!("Bearer {token}"))
        .body(body)
        .send()?;

    if response.status().is_success() {
        return Ok(Some(response.json()?));
    }

    Ok(None)
}

pub fn online_lookup(unmatched: &[DatRom], config: &Config) -> anyhow::Result<Vec<OnlineMatch>> {
    if !config.enable_hasheous && config.igdb_client_id.is_none() {
        return Ok(Vec::new());
    }

    let client = Client::new();
    let mut results = Vec::new();

    for rom in unmatched {
        let mut hasheous_result = None;
        if config.enable_hasheous {
            if let Some(hash) = rom
                .sha1
                .as_ref()
                .or(rom.md5.as_ref())
                .or(rom.sha256.as_ref())
            {
                hasheous_result = query_hasheous(hash).ok().flatten();
            }
        }

        let mut igdb_result = None;
        if config.igdb_client_id.is_some() {
            let name = rom
                .description
                .as_ref()
                .filter(|s| !s.is_empty())
                .unwrap_or(&rom.name);
            igdb_result = query_igdb(name, config, &client).ok().flatten();
        }

        if hasheous_result.is_some() || igdb_result.is_some() {
            results.push(OnlineMatch {
                name: rom.name.clone(),
                source_dat: rom.source_dat.clone(),
                hasheous: hasheous_result,
                igdb: igdb_result,
            });
        }
    }

    Ok(results)
}

pub fn scan_inputs_and_dats(
    config: &Config,
) -> anyhow::Result<(Vec<FileRecord>, Vec<DatRom>, Vec<OnlineMatch>)> {
    let records = collect_files(config)?;
    let dat_roms = load_dat_roms(config)?;
    let (unmatched, _) = dat_unmatched(&records, &dat_roms);
    let online = online_lookup(&unmatched, config)?;
    Ok((records, dat_roms, online))
}
