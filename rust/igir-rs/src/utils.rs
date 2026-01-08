use globset::{Glob, GlobSet, GlobSetBuilder};
use std::path::PathBuf;

pub fn build_globset(patterns: &[PathBuf]) -> anyhow::Result<Option<GlobSet>> {
    if patterns.is_empty() {
        return Ok(None);
    }

    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = Glob::new(pattern.to_string_lossy().as_ref())?;
        builder.add(glob);
    }

    Ok(Some(builder.build()?))
}
