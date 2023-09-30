use crate::atomic_file::modify_json;
use crate::id::ResourceId;
use crate::meta::load_meta_bytes;
use crate::{ArklibError, Result,
    AtomicFile, LINK_STORAGE_FOLDER, METADATA_STORAGE_FOLDER,
    PREVIEWS_STORAGE_FOLDER, PROPERTIES_STORAGE_FOLDER, ARK_FOLDER,
};
use anyhow::Error;
use reqwest::header::HeaderValue;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::Path;
use std::str::{self, FromStr};
use std::{io::Write, path::PathBuf};
use url::Url;


use crate::meta::store_meta;


#[derive(Debug, Deserialize, Serialize)]
pub struct Link {
    pub url: Url,
    pub meta: Metadata,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Metadata {
    pub title: String,
    pub desc: Option<String>,
}

impl Link {
    pub fn new(url: Url, title: String, desc: Option<String>) -> Self {
        Self {
            url,
            meta: Metadata { title, desc },
        }
    }

    pub fn id(&self) -> Result<ResourceId> {
        ResourceId::compute_bytes(self.url.as_str().as_bytes())
    }

    /// Load a link with its metadata from file
    pub fn load<P: AsRef<Path>>(root: P, path: P) -> Result<Self> {
        let p = path.as_ref().to_path_buf();
        let url = Self::load_url(p)?;
        let id = ResourceId::compute_bytes(url.as_str().as_bytes())?;
        let bytes = load_meta_bytes::<PathBuf>(root.as_ref().to_owned(), id)?;
        let meta: Metadata =
            serde_json::from_slice(&bytes).map_err(|_| ArklibError::Parse)?;

        Ok(Self { url, meta })
    }

    pub async fn save<P: AsRef<Path>>(
        &self,
        root: P,
        with_preview: bool,
    ) -> Result<()> {
        let id = self.id()?;
        let id = id.to_string();
        let folder = root
            .as_ref()
            .join(STORAGES_FOLDER)
            .join(LINK_STORAGE_FOLDER)
            .join(&id);
        let link_file = AtomicFile::new(folder)?;
        let tmp = link_file.make_temp()?;
        (&tmp).write_all(self.url.as_str().as_bytes())?;
        let current_link = link_file.load()?;
        link_file.compare_and_swap(&current_link, tmp)?;

        //User defined properties
        let prop_folder = root
            .as_ref()
            .join(STORAGES_FOLDER)
            .join(PROPERTIES_STORAGE_FOLDER)
            .join(&id);
        let prop_file = AtomicFile::new(prop_folder)?;
        modify_json(&prop_file, |data: &mut Option<Metadata>| {
            let metadata = self.meta.clone();
            match data {
                Some(data) => {
                    // Hack currently overwrites
                    *data = metadata;
                }
                None => *data = Some(metadata),
            }
        })?;

        // Generated data
        let url = (&self.url).to_string();
        if let Ok(data) = Link::get_preview(url).await {
            let graph_folder = Path::new(STORAGES_FOLDER)
                .join(METADATA_STORAGE_FOLDER)
                .join(&id);
            let file = AtomicFile::new(graph_folder)?;
            modify_json(&file, |file_data: &mut Option<OpenGraph>| {
                let graph = data.clone();
                match file_data {
                    Some(file_data) => {
                        // Hack currently overwrite
                        *file_data = graph;
                    }
                    None => *file_data = Some(graph),
                }
            })?;
            if with_preview {
                if let Some(preview_data) = data.fetch_image().await {
                    self.save_preview(root, preview_data)?;
                }
            }
        }
        Ok(())
    }

    fn save_preview<P: AsRef<Path>>(
        &self,
        root: P,
        image_data: Vec<u8>,
        id: &ResourceId,
    ) -> Result<()> {
        let path = root
            .as_ref()
            .join(ARK_FOLDER)
            .join(PREVIEWS_STORAGE_FOLDER)
            .join(id.to_string());
        let file = AtomicFile::new(path)?;
        let tmp = file.make_temp()?;
        (&tmp).write_all(&image_data)?;
        let current_preview = file.load()?;
        file.compare_and_swap(&current_preview, tmp)?;
        Ok(())
    }

    /// Get metadata of the link (synced).
    pub fn get_preview_synced<S>(url: S) -> Result<OpenGraph>
    where
        S: Into<String>,
    {
        let runtime =
            tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        return runtime.block_on(Link::get_preview(url));
    }

    /// Get metadata of the link.
    pub async fn get_preview<S>(url: S) -> Result<OpenGraph>
    where
        S: Into<String>,
    {
        let mut header = reqwest::header::HeaderMap::new();
        header.insert(
            "User-Agent",
            HeaderValue::from_static(
                "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0",
            ),
        );
        let client = reqwest::Client::builder()
            .default_headers(header)
            .build()?;
        let scraper = client
            .get(url.into())
            .send()
            .await?
            .text()
            .await?;
        let html = Html::parse_document(&scraper.as_str());
        let title =
            select_og(&html, OpenGraphTag::Title).or(select_title(&html));
        Ok(OpenGraph {
            title,
            description: select_og(&html, OpenGraphTag::Description)
                .or(select_desc(&html)),
            url: select_og(&html, OpenGraphTag::Url),
            image: select_og(&html, OpenGraphTag::Image),
            object_type: select_og(&html, OpenGraphTag::Type),
            locale: select_og(&html, OpenGraphTag::Locale),
        })
    }

    fn load_url(path: PathBuf) -> Result<Url, Error> {
        let file = AtomicFile::new(path)?;
        let read_file = file.load()?;
        let content = read_file.read_to_string()?;
        let url_str = str::from_utf8(content.as_bytes())?;
        Ok(Url::from_str(url_str)?)
    }
}

fn select_og(html: &Html, tag: OpenGraphTag) -> Option<String> {
    let selector =
        Selector::parse(&format!("meta[property=\"og:{}\"]", tag.as_str()))
            .unwrap();

    if let Some(element) = html.select(&selector).next() {
        if let Some(value) = element.value().attr("content") {
            return Some(value.to_string());
        }
    }

    None
}
fn select_desc(html: &Html) -> Option<String> {
    let selector = Selector::parse("meta[name=\"description\"]").unwrap();

    if let Some(element) = html.select(&selector).next() {
        if let Some(value) = element.value().attr("content") {
            return Some(value.to_string());
        }
    }

    None
}
fn select_title(html: &Html) -> Option<String> {
    let selector = Selector::parse("title").unwrap();
    if let Some(element) = html.select(&selector).next() {
        return element.text().next().map(|x| x.to_string());
    }

    None
}
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct OpenGraph {
    /// Represents the "og:title" OpenGraph meta tag.
    ///
    /// The title of your object as it should appear within
    /// the graph, e.g., "The Rock".
    pub title: Option<String>,
    /// Represents the "og:description" OpenGraph meta tag
    pub description: Option<String>,
    /// Represents the "og:url" OpenGraph meta tag
    pub url: Option<String>,
    /// Represents the "og:image" OpenGraph meta tag
    pub image: Option<String>,
    /// Represents the "og:type" OpenGraph meta tag
    ///
    /// The type of your object, e.g., "video.movie". Depending on the type
    /// you specify, other properties may also be required.
    object_type: Option<String>,
    /// Represents the "og:locale" OpenGraph meta tag
    locale: Option<String>,
}
impl OpenGraph {
    pub async fn fetch_image(&self) -> Option<Vec<u8>> {
        if let Some(url) = &self.image {
            let res = reqwest::get(url).await.unwrap();
            Some(res.bytes().await.unwrap().to_vec())
        } else {
            None
        }
    }

    pub fn fetch_image_synced(&self) -> Option<Vec<u8>> {
        let runtime =
            tokio::runtime::Runtime::new().expect("Unable to create a runtime");
        return runtime.block_on(self.fetch_image());
    }
}
/// OpenGraphTag meta tags collection
pub enum OpenGraphTag {
    /// Represents the "og:title" OpenGraph meta tag.
    ///
    /// The title of your object as it should appear within
    /// the graph, e.g., "The Rock".
    Title,
    /// Represents the "og:url" OpenGraph meta tag
    Url,
    /// Represents the "og:image" OpenGraph meta tag
    Image,
    /// Represents the "og:type" OpenGraph meta tag
    ///
    /// The type of your object, e.g., "video.movie". Depending on the type
    /// you specify, other properties may also be required.
    Type,
    /// Represents the "og:description" OpenGraph meta tag
    Description,
    /// Represents the "og:locale" OpenGraph meta tag
    Locale,
    /// Represents the "og:image:height" OpenGraph meta tag
    ImageHeight,
    /// Represents the "og:image:width" OpenGraph meta tag
    ImageWidth,
    /// Represents the "og:site_name" OpenGraph meta tag
    SiteName,
}

impl fmt::Debug for OpenGraphTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl OpenGraphTag {
    fn as_str(&self) -> &str {
        match self {
            OpenGraphTag::Title => "title",
            OpenGraphTag::Url => "url",
            OpenGraphTag::Image => "image",
            OpenGraphTag::Type => "type",
            OpenGraphTag::Description => "description",
            OpenGraphTag::Locale => "locale",
            OpenGraphTag::ImageHeight => "image:height",
            OpenGraphTag::ImageWidth => "image:width",
            OpenGraphTag::SiteName => "site_name",
        }
    }
}

#[tokio::test]
async fn test_create_link_file() {
    use tempdir::TempDir;
    let dir = TempDir::new("arklib_test").unwrap();
    let root = dir.path();
    println!("temporary root: {}", root.display());
    let url = Url::parse("https://example.com/").unwrap();
    let link =
        Link::new(url, String::from("title"), Some(String::from("desc")));

    let path = root.join("test.link");

    for save_preview in [false, true] {
        link.save(root, save_preview).await.unwrap();
        let file = AtomicFile::new(&path).unwrap();
        let current = file.load().unwrap();
        let current_bytes = current.read_to_string().unwrap();
        let url: Url =
            Url::from_str(str::from_utf8(current_bytes.as_bytes()).unwrap())
                .unwrap();
        assert_eq!(url.as_str(), "https://example.com/");
        let link = Link::load(root.clone(), path.as_path()).unwrap();
        assert_eq!(link.url.as_str(), url.as_str());
        assert_eq!(link.meta.desc.unwrap(), "desc");
        assert_eq!(link.meta.title, "title");

        let id = ResourceId::compute_bytes(current_bytes.as_bytes()).unwrap();
        println!("resource: {}, {}", id.crc32, id.data_size);

        if Path::new(root)
            .join(ARK_FOLDER)
            .join(PREVIEWS_STORAGE_FOLDER)
            .join(id.to_string())
            .exists()
        {
            assert_eq!(save_preview, true)
        } else {
            assert_eq!(save_preview, false)
        }
    }
}
