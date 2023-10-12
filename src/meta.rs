use super::atomic_file::{modify_json, AtomicFile};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::io::Read;
use std::path::Path;

use crate::id::ResourceId;
use crate::{
    Result, ARK_FOLDER, METADATA_STORAGE_FOLDER, PROPERTIES_STORAGE_FOLDER,
};

/// Dynamic metadata: stored as JSON and
/// interpreted differently depending on kind of a resource
pub fn store_meta<
    S: Serialize + DeserializeOwned + Clone + Debug,
    P: AsRef<Path>,
>(
    root: P,
    id: ResourceId,
    metadata: S,
) -> Result<()> {
    let file = AtomicFile::new(
        root.as_ref()
            .join(ARK_FOLDER)
            .join(METADATA_STORAGE_FOLDER)
            .join(id.to_string()),
    )?;
    modify_json(&file, |previous_data: &mut Option<S>| {
        match previous_data {
            Some(previous_data) => {
                // HACK: Find how to handle case where simultaneous writing happens. What is the expected result
                // Overwrite the data for now
                *previous_data = metadata.clone();
            }
            None => *previous_data = Some(metadata.clone()),
        }
    })?;
    Ok(())
}

/// The file must exist if this method is called
pub fn load_prop_bytes<P: AsRef<Path>>(
    root: P,
    id: ResourceId,
) -> Result<Vec<u8>> {
    let storage = root
        .as_ref()
        .join(ARK_FOLDER)
        .join(PROPERTIES_STORAGE_FOLDER)
        .join(id.to_string());
    let file = AtomicFile::new(storage)?;
    let read_file = file.load()?;
    if let Some(mut real_file) = read_file.open()? {
        let mut content = vec![];
        real_file.read_to_end(&mut content)?;
        Ok(content)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        ))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;

    use std::collections::HashMap;
    type TestMetadata = HashMap<String, String>;

    #[test]
    fn test_store_and_load() {
        let dir = TempDir::new("arklib_test").unwrap();
        let root = dir.path();
        log::debug!("temporary root: {}", root.display());

        let id = ResourceId {
            crc32: 0x342a3d4a,
            data_size: 1,
        };

        let mut meta = TestMetadata::new();
        meta.insert("abc".to_string(), "def".to_string());
        meta.insert("xyz".to_string(), "123".to_string());

        store_meta(root, id, meta.clone()).unwrap();

        let bytes = load_prop_bytes(root, id).unwrap();
        let meta2: TestMetadata = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(meta, meta2);
    }
}
