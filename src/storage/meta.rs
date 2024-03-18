use crate::atomic::{modify_json, AtomicFile};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use std::path::Path;

use crate::id::ResourceId;
use crate::{Result, ARK_FOLDER, METADATA_STORAGE_FOLDER};

pub fn store_metadata<
    S: Serialize + DeserializeOwned + Clone + Debug,
    P: AsRef<Path>,
>(
    root: P,
    id: ResourceId,
    metadata: &S,
) -> Result<()> {
    let file = AtomicFile::new(
        root.as_ref()
            .join(ARK_FOLDER)
            .join(METADATA_STORAGE_FOLDER)
            .join(id.to_string()),
    )?;
    modify_json(&file, |current_meta: &mut Option<S>| {
        let new_meta = metadata.clone();
        match current_meta {
            Some(file_data) => {
                // This is fine because generated metadata must always
                // be generated in same way on any device.
                *file_data = new_meta;
                // Different versions of the lib should
                // not be used on synced devices.
            }
            None => *current_meta = Some(new_meta),
        }
    })?;
    Ok(())
}
