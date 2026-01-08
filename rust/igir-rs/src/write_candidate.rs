use crate::types::FileRecord;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct WriteCandidate {
    pub name: String,
    /// the files proposed to satisfy this candidate (one or more input files)
    pub files: Vec<FileRecord>,
    /// mapping of dat part name -> chosen FileRecord
    pub files_map: HashMap<String, FileRecord>,
}

impl WriteCandidate {
    pub fn new(name: impl Into<String>, files: Vec<FileRecord>) -> Self {
        Self {
            name: name.into(),
            files,
            files_map: HashMap::new(),
        }
    }
}
