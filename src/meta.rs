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
        path: &CanonicalPathBuf,
        entry: &DirEntry,
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

        let kind = Some(ResourceKind::from(path.clone()));

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
        height: Option<i64>,
        width: Option<i64>,
        duration: Option<i64>,
    },
    Document {
        pages: Option<i64>,
    },
    Link {
        title: Option<String>,
        description: Option<String>,
        url: Option<String>,
    },

    PlainText,
    Archive,
}

// Currently all unrecognized/unsupported format will be parsed to PlainText
impl From<CanonicalPathBuf> for ResourceKind {
    fn from(path: CanonicalPathBuf) -> Self {
        let ext = path
            .extension()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default();

        if ext == "link" {
            return ResourceKind::Link {
                title: None,
                description: None,
                url: None,
            };
        }

        let accepted_doc = ["pdf", "doc", "docx", "odt", "ods", "md"];
        let accepted_img = ["jpg", "jpeg", "png", "svg", "gif"];
        let accepted_ar = ["zip", "7z", "rar", "tar.gz", "tar.xz"];
        let accepted_video = [
            "mp4", "avi", "mkv", "mov", "wmv", "flv", "webm", "ts", "mpg",
        ];
        if accepted_ar.contains(&ext) {
            return ResourceKind::Archive;
        };
        if accepted_img.contains(&ext) {
            return ResourceKind::Image;
        }
        if accepted_doc.contains(&ext) {
            return ResourceKind::Document { pages: None };
        }
        if accepted_video.contains(&ext) {
            return ResourceKind::Video {
                height: None,
                width: None,
                duration: None,
            };
        }

        ResourceKind::PlainText
    }
}

fn convert_str(option: Option<&OsStr>) -> Option<OsString> {
    if let Some(value) = option {
        return Some(value.to_os_string());
    }
    None
}
