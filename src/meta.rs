use crate::id::ResourceId;

use anyhow::Error;
use canonical_path::CanonicalPathBuf;
use chrono::{DateTime, Utc};
use std::ffi::{OsStr, OsString};
use strum::{Display, EnumCount, EnumString};
use walkdir::DirEntry;
#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct ResourceMeta {
    pub id: ResourceId,
    pub modified: DateTime<Utc>,
    pub name: Option<OsString>,
    pub extension: Option<OsString>,
    pub kind: Option<ResourceKind>,
}

impl ResourceMeta {
    pub fn scan(
        path: CanonicalPathBuf,
        entry: DirEntry,
    ) -> Result<(CanonicalPathBuf, Self), Error> {
        if entry.file_type().is_dir() {
            return Err(Error::msg("DirEntry is directory"));
        }

        let metadata = entry.metadata()?;
        let size = metadata.len();
        if size == 0 {
            return Err(Error::msg("Empty resource"));
        }

        let id = ResourceId::compute(size, &path);
        let name = convert_str(path.file_name());
        let extension = convert_str(path.extension());
        let modified = metadata.modified()?.into();

        let kind = None;

        let meta = ResourceMeta {
            id,
            modified,
            name,
            extension,
            kind,
        };

        Ok((path.clone(), meta))
    }
}

#[derive(Eq, PartialEq, Hash, Clone, Debug, EnumString, Display, EnumCount)]
#[strum(ascii_case_insensitive)]
pub enum ResourceKind {
    Image,
    Video {
        height: i64,
        width: i64,
        duration: i64,
    },
    Document {
        pages: i64,
    },
    Link {
        title: String,
        description: String,
        url: String,
    },

    PlainText,
    Archive,
}

fn convert_str(option: Option<&OsStr>) -> Option<OsString> {
    if let Some(value) = option {
        return Some(value.to_os_string());
    }
    None
}
