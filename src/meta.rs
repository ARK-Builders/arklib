use crate::{id::ResourceId, pdf};

use anyhow::Error;
use canonical_path::CanonicalPathBuf;
use chrono::{DateTime, Utc};
use infer::MatcherType;
use mime_guess::mime;
use pdfium_render::prelude::Pdfium;
use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use std::fs::File;
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
        height: i64,
        width: i64,
        duration: i64,
    },
    Document {
        pages: Option<i64>,
    },
    Link {
        title: String,
        description: String,
        url: String,
    },

    PlainText,
    Archive,
}

// Currently all unrecognized/unsupported format will be parsed to PlainText
impl From<CanonicalPathBuf> for ResourceKind {
    fn from(path: CanonicalPathBuf) -> Self {
        // fn is_plain(_: &[u8]) -> bool {
        //     true
        // };
        // let g = infer::get_from_path(path.as_path()).unwrap().unwrap_or(infer::Type::new(infer::MatcherType::Text, "text/plain", "txt", is_plain))
        // match g.matcher_type() {
        //     MatcherType::Image => {
        //         match g.mime_type() {
        //             "image/jpeg"| "image/jpg"| "image/png"| "image/gif" => {

        //             },
        //             _ => {}
        //         }
        //     }
        //     MatcherType::Archive => {
        //         match g.mime_type() {

        //         }
        //     }
        //     _ => {}
        // }
        let ext = path
            .extension()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default();

        if ext == "link" {
            let link =  match parse_link(&path) {
                Ok(x) => x,
                Err(e) => {
                    log::error!(
                        "cannot parse link: {}, fallback to default plaintext factory",
                        path.display()
                    );
                    Self::PlainText
                }
            };
            return link;
        }

        let accepted_doc = ["pdf", "doc", "docx", "odt", "ods", "md"];
        let accepted_img = ["jpg", "jpeg", "png", "svg", "gif"];
        let accepted_ar = ["zip", "7z", "rar", "tar.gz", "tar.xz"];
        let generic_text = ["txt", ""];
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
            if ext != "pdf" {
                return ResourceKind::Document { pages: None };
            } else {
                // TODO: Wait for
                let pb = pdf::initialize_pdfium();
                let pages = Some(
                    Pdfium::new(pb)
                        .load_pdf_from_file(path.as_path(), None)
                        .unwrap()
                        .pages()
                        .len() as i64,
                );

                return ResourceKind::Document { pages };
            }
        }

        if accepted_video.contains(&ext) {
            // TODO: Read Video Info
            return ResourceKind::PlainText;
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
#[derive(Deserialize, Serialize)]
pub struct LinkFile {
    pub title: String,
    pub desc: String,
    pub url: String,
}

fn parse_link(path: &CanonicalPathBuf) -> Result<ResourceKind, Error> {
    let file = File::open(path).unwrap();
    let mut zip = zip::ZipArchive::new(file).unwrap();
    let j_raw = zip.by_name("link.json").unwrap();

    let j = serde_json::from_reader::<_, LinkFile>(j_raw).map(|x| {
        ResourceKind::Link {
            description: x.desc,
            title: x.title,
            url: x.url,
        }
    })?;
    Ok(j)
}
