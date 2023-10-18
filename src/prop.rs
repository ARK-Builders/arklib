use super::atomic_file::{modify_json, AtomicFile};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::io::Read;
use std::path::Path;

use crate::id::ResourceId;
use crate::{Result, ARK_FOLDER, PROPERTIES_STORAGE_FOLDER};

/// Dynamic metadata: stored as JSON and
/// interpreted differently depending on kind of a resource
pub fn store_properties<
    S: Serialize + DeserializeOwned + Clone + Debug,
    P: AsRef<Path>,
>(
    root: P,
    id: ResourceId,
    properties: S,
) -> Result<()> {
    let file = AtomicFile::new(
        root.as_ref()
            .join(ARK_FOLDER)
            .join(PROPERTIES_STORAGE_FOLDER)
            .join(id.to_string()),
    )?;
    modify_json(&file, |previous_data: &mut Option<S>| match previous_data {
        Some(previous_data) => match serde_json::to_value(&previous_data) {
            Ok(mut previous_object) => {
                // may return error if data in file is not an object
                let previous_value = previous_object.as_object_mut().unwrap();
                let mut new_object = serde_json::to_value(&properties).unwrap();
                // May return error if properties is not an object
                let new_object = new_object.as_object_mut().unwrap();
                for (key, value) in new_object {
                    if previous_value.contains_key(key) {
                        let previous_saved = previous_value.get(key).unwrap();
                        let new_data = match previous_saved {
                            serde_json::Value::Array(data) => {
                                let mut data = data.to_vec();
                                data.push(value.clone());
                                serde_json::Value::Array(data)
                            }
                            _ => serde_json::Value::Array(vec![
                                previous_saved.clone(),
                                value.clone(),
                            ]),
                        };
                        previous_value.insert(key.clone(), new_data);
                    } else {
                        previous_value.insert(
                            key.clone(),
                            serde_json::Value::Array(vec![value.clone()]),
                        );
                    }
                }
            }
            Err(_) => *previous_data = properties.clone(),
        },
        None => *previous_data = Some(properties.clone()),
    })?;
    Ok(())
}

/// The file must exist if this method is called
pub fn load_raw_properties<P: AsRef<Path>>(
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
    type TestProperties = HashMap<String, String>;

    #[test]
    fn test_store_and_load() {
        let dir = TempDir::new("arklib_test").unwrap();
        let root = dir.path();
        log::debug!("temporary root: {}", root.display());

        let id = ResourceId {
            crc32: 0x342a3d4a,
            data_size: 1,
        };

        let mut prop = TestProperties::new();
        prop.insert("abc".to_string(), "def".to_string());
        prop.insert("xyz".to_string(), "123".to_string());

        store_properties(root, id, prop.clone()).unwrap();

        let bytes = load_raw_properties(root, id).unwrap();
        let prop2: TestProperties = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(prop, prop2);
    }
}
