use std::path::Path;

/// Stubbed patch application API.
///
/// Current implementation is a placeholder that returns Ok(None) which indicates
/// "not applied / not implemented yet". Future work: implement IPS/BPS/UPS
/// applying logic here or call into a dedicated crate.
pub fn apply_patch_to_bytes(_patch_path: &Path, _source: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    // TODO: implement patch formats (IPS, BPS, UPS, IPS32, etc.) or integrate a crate.
    Ok(None)
}
