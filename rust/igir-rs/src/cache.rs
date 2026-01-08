use anyhow::Context;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;
use std::path::{Path, PathBuf};

use crate::types::ChecksumSet;

pub struct Cache {
    conn: Connection,
}

#[derive(Debug, Clone)]
pub struct IgdbCacheEntry {
    pub json: Value,
    pub slug: Option<String>,
    pub name: Option<String>,
    pub genres: Vec<String>,
    pub platforms: Vec<String>,
}

impl Cache {
    pub fn open(
        cache_db: Option<&PathBuf>,
        _config_output: Option<&PathBuf>,
    ) -> anyhow::Result<Self> {
        // Determine path for DB: explicit `--cache-db` path wins, else fallback to the current
        // working directory so runs are isolated per invocation location.
        let db_path = if let Some(explicit) = cache_db {
            // Use the explicit path provided via --cache-db
            explicit.clone()
        } else {
            let mut p = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
            p.push("igir_cache.sqlite");
            p
        };

        let conn = Connection::open(db_path).with_context(|| "opening sqlite cache")?;
        let cache = Cache { conn };
        cache.init_schema()?;
        Ok(cache)
    }

    fn init_schema(&self) -> anyhow::Result<()> {
        self.conn.execute_batch(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS checksums (
                key TEXT PRIMARY KEY,
                source TEXT,
                size INTEGER,
                crc32 TEXT,
                md5 TEXT,
                sha1 TEXT,
                sha256 TEXT,
                updated_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS hasheous (
                key TEXT PRIMARY KEY,
                source TEXT,
                json TEXT,
                updated_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS igdb (
                key TEXT PRIMARY KEY,
                json TEXT,
                slug TEXT,
                name TEXT,
                genres_json TEXT,
                platforms_json TEXT,
                updated_at INTEGER
            );
            COMMIT;",
        )?;
        self.ensure_igdb_columns()?;
        Ok(())
    }

    fn ensure_igdb_columns(&self) -> anyhow::Result<()> {
        const MIGRATIONS: &[(&str, &str)] = &[
            ("slug", "ALTER TABLE igdb ADD COLUMN slug TEXT"),
            ("name", "ALTER TABLE igdb ADD COLUMN name TEXT"),
            (
                "genres_json",
                "ALTER TABLE igdb ADD COLUMN genres_json TEXT",
            ),
            (
                "platforms_json",
                "ALTER TABLE igdb ADD COLUMN platforms_json TEXT",
            ),
        ];

        for (_, ddl) in MIGRATIONS {
            self.add_column_if_missing(ddl)?;
        }
        Ok(())
    }

    fn add_column_if_missing(&self, ddl: &str) -> anyhow::Result<()> {
        match self.conn.execute(ddl, []) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(err, msg)) => {
                let duplicate = msg
                    .as_deref()
                    .map(|m| m.contains("duplicate column"))
                    .unwrap_or(false);
                if duplicate {
                    Ok(())
                } else {
                    Err(rusqlite::Error::SqliteFailure(err, msg).into())
                }
            }
            Err(err) => Err(err.into()),
        }
    }
    pub fn get_checksums_by_key(&self, key: &str) -> anyhow::Result<Option<ChecksumSet>> {
        let mut stmt = self
            .conn
            .prepare("SELECT crc32, md5, sha1, sha256 FROM checksums WHERE key = ?1")?;
        let row = stmt
            .query_row(params![key], |r| {
                Ok(ChecksumSet {
                    crc32: r.get::<_, Option<String>>(0)?,
                    md5: r.get::<_, Option<String>>(1)?,
                    sha1: r.get::<_, Option<String>>(2)?,
                    sha256: r.get::<_, Option<String>>(3)?,
                })
            })
            .optional()?;
        Ok(row)
    }

    pub fn set_checksums_by_key(
        &self,
        key: &str,
        source: &Path,
        size: Option<u64>,
        set: &ChecksumSet,
    ) -> anyhow::Result<()> {
        let s = source.to_string_lossy();
        let ts = chrono::Utc::now().timestamp();
        self.conn.execute(
            "REPLACE INTO checksums (key, source, size, crc32, md5, sha1, sha256, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![key, s.as_ref(), size.map(|v| v as i64), set.crc32.as_deref(), set.md5.as_deref(), set.sha1.as_deref(), set.sha256.as_deref(), ts],
        )?;
        Ok(())
    }

    pub fn get_hasheous_raw_by_key(&self, key: &str) -> anyhow::Result<Option<Value>> {
        let mut stmt = self
            .conn
            .prepare("SELECT json FROM hasheous WHERE key = ?1")?;
        let row = stmt
            .query_row(params![key], |r| r.get::<_, Option<String>>(0))
            .optional()?;
        if let Some(Some(j)) = row {
            let v = serde_json::from_str::<Value>(&j).ok();
            Ok(v)
        } else {
            Ok(None)
        }
    }

    pub fn set_hasheous_raw_by_key(
        &self,
        key: &str,
        source: &Path,
        json: &Value,
    ) -> anyhow::Result<()> {
        let s = source.to_string_lossy();
        let ts = chrono::Utc::now().timestamp();
        let js = serde_json::to_string(json)?;
        self.conn.execute(
            "REPLACE INTO hasheous (key, source, json, updated_at) VALUES (?1, ?2, ?3, ?4)",
            params![key, s.as_ref(), js, ts],
        )?;
        Ok(())
    }

    pub fn get_igdb_raw_by_key(&self, key: &str) -> anyhow::Result<Option<Value>> {
        let mut stmt = self.conn.prepare("SELECT json FROM igdb WHERE key = ?1")?;
        let row = stmt
            .query_row(params![key], |r| r.get::<_, Option<String>>(0))
            .optional()?;
        if let Some(Some(j)) = row {
            let v = serde_json::from_str::<Value>(&j).ok();
            Ok(v)
        } else {
            Ok(None)
        }
    }

    pub fn get_igdb_entry_by_key(&self, key: &str) -> anyhow::Result<Option<IgdbCacheEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT json, slug, name, genres_json, platforms_json FROM igdb WHERE key = ?1",
        )?;
        let row = stmt
            .query_row(params![key], |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                ))
            })
            .optional()?;
        if let Some((Some(json_str), slug, name, genres_json, platforms_json)) = row {
            let json = serde_json::from_str::<Value>(&json_str).ok();
            if let Some(json) = json {
                let genres = parse_string_list(genres_json);
                let platforms = parse_string_list(platforms_json);
                return Ok(Some(IgdbCacheEntry {
                    json,
                    slug,
                    name,
                    genres,
                    platforms,
                }));
            }
        }
        Ok(None)
    }

    pub fn set_igdb_raw_by_key(&self, key: &str, json: &Value) -> anyhow::Result<()> {
        let ts = chrono::Utc::now().timestamp();
        let js = serde_json::to_string(json)?;
        let summary = IgdbSummary::from_json(json);
        self.conn.execute(
            "REPLACE INTO igdb (key, json, slug, name, genres_json, platforms_json, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                key,
                js,
                summary.slug.as_deref(),
                summary.name.as_deref(),
                summary.genres_json.as_deref(),
                summary.platforms_json.as_deref(),
                ts
            ],
        )?;
        Ok(())
    }

    pub fn delete_igdb_key(&self, key: &str) -> anyhow::Result<()> {
        self.conn
            .execute("DELETE FROM igdb WHERE key = ?1", params![key])?;
        Ok(())
    }
}

fn parse_string_list(data: Option<String>) -> Vec<String> {
    if let Some(raw) = data {
        if let Ok(list) = serde_json::from_str::<Vec<String>>(&raw) {
            return list;
        }
    }
    Vec::new()
}

struct IgdbSummary {
    slug: Option<String>,
    name: Option<String>,
    genres_json: Option<String>,
    platforms_json: Option<String>,
}

impl IgdbSummary {
    fn from_json(json: &Value) -> Self {
        let mut slug = None;
        let mut name = None;
        let mut genres: Vec<String> = Vec::new();
        let mut platforms: Vec<String> = Vec::new();

        if let Some(entries) = json.as_array() {
            if let Some(first) = entries.first() {
                slug = first
                    .get("slug")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                name = first
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                if let Some(genre_arr) = first.get("genres").and_then(|v| v.as_array()) {
                    for genre in genre_arr {
                        if let Some(genre_name) = genre.get("name").and_then(|n| n.as_str()) {
                            let trimmed = genre_name.trim();
                            if trimmed.is_empty() {
                                continue;
                            }
                            if !genres
                                .iter()
                                .any(|existing| existing.eq_ignore_ascii_case(trimmed))
                            {
                                genres.push(trimmed.to_string());
                            }
                        }
                    }
                }

                if let Some(platform_arr) = first.get("platforms").and_then(|v| v.as_array()) {
                    for platform in platform_arr {
                        let mut pushed = false;
                        if let Some(platform_name) = platform.get("name").and_then(|n| n.as_str()) {
                            pushed |= push_unique(&mut platforms, platform_name.trim());
                        }
                        if let Some(platform_slug) = platform.get("slug").and_then(|s| s.as_str()) {
                            pushed |= push_unique(&mut platforms, platform_slug.trim());
                        }
                        if let Some(platform_abbr) =
                            platform.get("abbreviation").and_then(|s| s.as_str())
                        {
                            pushed |= push_unique(&mut platforms, platform_abbr.trim());
                        }
                        if !pushed {
                            continue;
                        }
                    }
                }
            }
        }

        let genres_json = if genres.is_empty() {
            None
        } else {
            serde_json::to_string(&genres).ok()
        };
        let platforms_json = if platforms.is_empty() {
            None
        } else {
            serde_json::to_string(&platforms).ok()
        };

        Self {
            slug,
            name,
            genres_json,
            platforms_json,
        }
    }
}

fn push_unique(list: &mut Vec<String>, candidate: &str) -> bool {
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        return false;
    }
    if list
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(trimmed))
    {
        return false;
    }
    list.push(trimmed.to_string());
    true
}
