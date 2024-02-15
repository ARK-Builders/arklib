use anyhow::anyhow;
use itertools::Itertools;
use log;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::BufReader;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use walkdir::{DirEntry, WalkDir};

use crate::{id::ResourceId, ArklibError, Result, ARK_FOLDER, INDEX_PATH};

pub const RESOURCE_UPDATED_THRESHOLD: Duration = Duration::from_millis(1);
pub type Paths = HashSet<PathBuf>;

/// IndexEntry represents a [`ResourceId`] and the time it was last modified
#[derive(
    Eq, Ord, PartialEq, PartialOrd, Hash, Clone, Debug, Serialize, Deserialize,
)]
pub struct IndexEntry {
    /// The time the resource was last modified
    pub modified: SystemTime,
    /// The resource's ID
    pub id: ResourceId,
}

/// Represents an index of resources in the system
///
/// This struct maintains mappings between resource IDs and their corresponding
/// file paths, as well as mappings between file paths and index entries
/// Additionally, it keeps track of collisions that occur during indexing
#[serde_as]
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ResourceIndex {
    /// A mapping of resource IDs to their corresponding file paths
    #[serde_as(as = "Vec<(_, _)>")]
    pub id2path: HashMap<ResourceId, PathBuf>,
    /// A mapping of file paths to their corresponding index entries
    pub path2id: HashMap<PathBuf, IndexEntry>,
    /// A mapping of resource IDs to the number of collisions they have
    pub collisions: HashMap<ResourceId, usize>,
    /// The root path of the index
    root: PathBuf,
}

/// Represents an update to the resource index
///
/// This struct holds information about resources that have been deleted
/// or added during an update operation on the resource index
#[derive(PartialEq, Debug)]
pub struct IndexUpdate {
    /// Set of resource IDs that have been deleted
    pub deleted: HashSet<ResourceId>,
    /// Map of file paths to resource IDs that have been added
    pub added: HashMap<PathBuf, ResourceId>,
}

impl ResourceIndex {
    /// Returns the number of entries in the index
    ///
    /// Note that the actual size is lower in presence of collisions
    pub fn size(&self) -> usize {
        self.path2id.len()
    }

    /// Builds a new resource index from scratch using the root path
    ///
    /// This function recursively scans the directory structure starting from
    /// the root path, constructs index entries for each resource found, and
    /// populates the resource index
    pub fn build<P: AsRef<Path>>(root_path: P) -> Self {
        let root_path = root_path.as_ref().to_owned();
        log::info!(
            "Building the index from scratch for directory: {}",
            &root_path.display()
        );

        let entries = discover_files(&root_path);
        let entries = scan_entries(entries);
        let mut index = ResourceIndex {
            id2path: HashMap::new(),
            path2id: HashMap::new(),
            collisions: HashMap::new(),
            root: root_path,
        };
        for (path, entry) in entries {
            index.insert_entry(path, entry);
        }

        log::info!("Index built");
        index
    }

    /// Loads a previously stored resource index from the root path
    ///
    /// This function reads the index from the file system and returns a new
    /// [`ResourceIndex`] instance. It looks for the index fie in
    /// `root_path/.ark/index`
    pub fn load<P: AsRef<Path>>(root_path: P) -> Result<Self> {
        let index_path = root_path
            .as_ref()
            .join(ARK_FOLDER)
            .join(INDEX_PATH);
        log::info!("Loading the index from file: {}", index_path.display());

        let file = File::open(index_path)?;
        let reader = BufReader::new(file);
        let index: ResourceIndex = serde_json::from_reader(reader)?;

        Ok(index)
    }

    /// Stores the resource index to the file system
    ///
    /// This function writes the index to the file system. It writes the index
    /// to `root_path/.ark/index` and creates the directory if it does not exist
    pub fn store(&self) -> Result<()> {
        log::info!("Storing the index to file");
        let start = SystemTime::now();

        let ark_folder = self.root.join(ARK_FOLDER);
        if !ark_folder.exists() {
            fs::create_dir(ark_folder.clone())?;
        }
        let index_path = ark_folder.join(INDEX_PATH);
        let file = File::create(index_path)?;

        let writer = BufWriter::new(file);
        serde_json::to_writer(writer, self)?;

        log::trace!(
            "Storing the index took {:?}",
            start
                .elapsed()
                .map_err(|_| ArklibError::Other(anyhow!("SystemTime error")))
        );
        Ok(())
    }

    pub fn provide<P: AsRef<Path>>(root_path: P) -> Result<Self> {
        match Self::load(&root_path) {
            Ok(mut index) => {
                log::debug!("Index loaded: {} entries", index.path2id.len());

                match index.update_all() {
                    Ok(update) => {
                        log::debug!(
                            "Index updated: {} added, {} deleted",
                            update.added.len(),
                            update.deleted.len()
                        );
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to update index: {}",
                            e.to_string()
                        );
                    }
                }

                if let Err(e) = index.store() {
                    log::error!("{}", e.to_string());
                }
                Ok(index)
            }
            Err(e) => {
                log::warn!("{}", e.to_string());
                Ok(Self::build(root_path))
            }
        }
    }

    pub fn update_all(&mut self) -> Result<IndexUpdate> {
        log::debug!("Updating the index");
        log::trace!("[update] known paths: {:?}", self.path2id.keys());

        let curr_entries = discover_files(self.root.clone());

        //assuming that collections manipulation is
        // quicker than asking `path.exists()` for every path
        let curr_paths: Paths = curr_entries.keys().cloned().collect();
        let prev_paths: Paths = self.path2id.keys().cloned().collect();
        let preserved_paths: Paths = curr_paths
            .intersection(&prev_paths)
            .cloned()
            .collect();

        let created_paths: HashMap<PathBuf, DirEntry> = curr_entries
            .iter()
            .filter_map(|(path, entry)| {
                if !preserved_paths.contains(path) {
                    Some((path.clone(), entry.clone()))
                } else {
                    None
                }
            })
            .collect();

        log::debug!("Checking updated paths");
        let updated_paths: HashMap<PathBuf, DirEntry> = curr_entries
            .into_iter()
            .filter(|(path, dir_entry)| {
                if !preserved_paths.contains(path) {
                    false
                } else {
                    let our_entry = &self.path2id[path];
                    let prev_modified = our_entry.modified;

                    let result = dir_entry.metadata();
                    match result {
                        Err(msg) => {
                            log::error!(
                                "Couldn't retrieve metadata for {}: {}",
                                &path.display(),
                                msg
                            );
                            false
                        }
                        Ok(metadata) => match metadata.modified() {
                            Err(msg) => {
                                log::error!(
                                    "Couldn't retrieve timestamp for {}: {}",
                                    &path.display(),
                                    msg
                                );
                                false
                            }
                            Ok(curr_modified) => {
                                let elapsed = curr_modified
                                    .duration_since(prev_modified)
                                    .unwrap();

                                let was_updated =
                                    elapsed >= RESOURCE_UPDATED_THRESHOLD;
                                if was_updated {
                                    log::trace!(
                                        "[update] modified {} by path {}
                                        \twas {:?}
                                        \tnow {:?}
                                        \telapsed {:?}",
                                        our_entry.id,
                                        path.display(),
                                        prev_modified,
                                        curr_modified,
                                        elapsed
                                    );
                                }

                                was_updated
                            }
                        },
                    }
                }
            })
            .collect();

        let mut deleted: HashSet<ResourceId> = HashSet::new();

        // treating both deleted and updated paths as deletions
        prev_paths
            .difference(&preserved_paths)
            .cloned()
            .chain(updated_paths.keys().cloned())
            .for_each(|path| {
                if let Some(entry) = self.path2id.remove(&path) {
                    let k = self.collisions.remove(&entry.id).unwrap_or(1);
                    if k > 1 {
                        self.collisions.insert(entry.id, k - 1);
                    } else {
                        log::trace!(
                            "[delete] {} by path {}",
                            entry.id,
                            path.display()
                        );
                        self.id2path.remove(&entry.id);
                        deleted.insert(entry.id);
                    }
                } else {
                    log::warn!("Path {} was not known", path.display());
                }
            });

        let added: HashMap<PathBuf, IndexEntry> = scan_entries(updated_paths)
            .into_iter()
            .chain({
                log::debug!("Checking added paths");
                scan_entries(created_paths).into_iter()
            })
            .filter(|(_, entry)| !self.id2path.contains_key(&entry.id))
            .collect();

        for (path, entry) in added.iter() {
            if deleted.contains(&entry.id) {
                // emitting the resource as both deleted and added
                // (renaming a duplicate might remain undetected)
                log::trace!(
                    "[update] moved {} to path {}",
                    entry.id,
                    path.display()
                );
            }

            self.insert_entry(path.clone(), entry.clone());
        }

        let added: HashMap<PathBuf, ResourceId> = added
            .into_iter()
            .map(|(path, entry)| (path, entry.id))
            .collect();

        Ok(IndexUpdate { deleted, added })
    }

    // the caller must ensure that:
    // * the index is up-to-date except this single path
    // * the path hasn't been indexed before
    pub fn index_new(&mut self, path: &dyn AsRef<Path>) -> Result<IndexUpdate> {
        log::debug!("Indexing a new path");

        if !path.as_ref().exists() {
            return Err(ArklibError::Path(
                "Absent paths cannot be indexed".into(),
            ));
        }

        let path_buf = fs::canonicalize(path)?;
        let path = path_buf.as_path();

        return match fs::metadata(path) {
            Err(_) => {
                return Err(ArklibError::Path(
                    "Couldn't to retrieve file metadata".into(),
                ));
            }
            Ok(metadata) => match scan_entry(path, metadata) {
                Err(_) => {
                    return Err(ArklibError::Path(
                        "The path points to a directory or empty file".into(),
                    ));
                }
                Ok(new_entry) => {
                    let id = new_entry.id;

                    if let Some(nonempty) = self.collisions.get_mut(&id) {
                        *nonempty += 1;
                    }

                    let mut added = HashMap::new();
                    added.insert(path_buf.clone(), id);

                    self.id2path.insert(id, path_buf.clone());
                    self.path2id.insert(path_buf, new_entry);

                    Ok(IndexUpdate {
                        added,
                        deleted: HashSet::new(),
                    })
                }
            },
        };
    }

    // the caller must ensure that:
    // * the index is up-to-date except this single path
    // * the path has been indexed before
    // * the path maps into `old_id`
    // * the content by the path has been modified
    pub fn update_one(
        &mut self,
        path: &dyn AsRef<Path>,
        old_id: ResourceId,
    ) -> Result<IndexUpdate> {
        log::debug!("Updating a single entry in the index");

        if !path.as_ref().exists() {
            return self.forget_id(old_id);
        }

        let path_buf = fs::canonicalize(path)?;
        let path = path_buf.as_path();

        log::trace!(
            "[update] paths {:?} has id {:?}",
            path,
            self.path2id[path]
        );

        return match fs::metadata(path) {
            Err(_) => {
                // updating the index after resource removal
                // is a correct scenario
                self.forget_path(path, old_id)
            }
            Ok(metadata) => {
                match scan_entry(path, metadata) {
                    Err(_) => {
                        // a directory or empty file exists by the path
                        self.forget_path(path, old_id)
                    }
                    Ok(new_entry) => {
                        // valid resource exists by the path

                        let curr_entry = &self.path2id.get(path);
                        if curr_entry.is_none() {
                            // if the path is not indexed, then we can't have
                            // `old_id` if you want
                            // to index new path, use `index_new` method
                            return Err(ArklibError::Path(
                                "Couldn't find the path in the index".into(),
                            ));
                        }
                        let curr_entry = curr_entry.unwrap();

                        if curr_entry.id == new_entry.id {
                            // in rare cases we are here due to hash collision
                            if curr_entry.modified == new_entry.modified {
                                log::warn!("path {:?} was not modified", &path);
                            } else {
                                log::warn!("path {:?} was modified but not its content", &path);
                            }

                            // the caller must have ensured that the path was
                            // indeed update
                            return Err(ArklibError::Collision(
                                "New content has the same id".into(),
                            ));
                        }

                        // new resource exists by the path
                        self.forget_path(path, old_id).map(|mut update| {
                            update
                                .added
                                .insert(path_buf.clone(), new_entry.id);
                            self.insert_entry(path_buf, new_entry);

                            update
                        })
                    }
                }
            }
        };
    }

    pub fn forget_id(&mut self, old_id: ResourceId) -> Result<IndexUpdate> {
        let old_path = self
            .path2id
            .drain()
            .filter_map(|(k, v)| {
                if v.id == old_id {
                    Some(k)
                } else {
                    None
                }
            })
            .collect_vec();
        for p in old_path {
            self.path2id.remove(&p);
        }
        self.id2path.remove(&old_id);
        let mut deleted = HashSet::new();
        deleted.insert(old_id);

        Ok(IndexUpdate {
            added: HashMap::new(),
            deleted,
        })
    }

    fn insert_entry(&mut self, path: PathBuf, entry: IndexEntry) {
        log::trace!("[add] {} by path {}", entry.id, path.display());
        let id = entry.id;

        if let std::collections::hash_map::Entry::Vacant(e) =
            self.id2path.entry(id)
        {
            e.insert(path.clone());
        } else if let Some(nonempty) = self.collisions.get_mut(&id) {
            *nonempty += 1;
        } else {
            self.collisions.insert(id, 2);
        }

        self.path2id.insert(path, entry);
    }

    fn forget_path(
        &mut self,
        path: &Path,
        old_id: ResourceId,
    ) -> Result<IndexUpdate> {
        self.path2id.remove(path);

        if let Some(collisions) = self.collisions.get_mut(&old_id) {
            debug_assert!(
                *collisions > 1,
                "Any collision must involve at least 2 resources"
            );
            *collisions -= 1;

            if *collisions == 1 {
                self.collisions.remove(&old_id);
            }

            // minor performance issue:
            // we must find path of one of the collided
            // resources and use it as new value
            let maybe_collided_path =
                self.path2id.iter().find_map(|(path, entry)| {
                    if entry.id == old_id {
                        Some(path)
                    } else {
                        None
                    }
                });

            if let Some(collided_path) = maybe_collided_path {
                let old_path =
                    self.id2path.insert(old_id, collided_path.clone());

                debug_assert_eq!(
                    old_path.unwrap().as_path(),
                    path,
                    "Must forget the requested path"
                );
            } else {
                return Err(ArklibError::Collision(
                    "Illegal state of collision tracker".into(),
                ));
            }
        } else {
            self.id2path.remove(&old_id);
        }

        let mut deleted = HashSet::new();
        deleted.insert(old_id);

        Ok(IndexUpdate {
            added: HashMap::new(),
            deleted,
        })
    }
}

/// Discovers all files under the specified root path
///
/// Returns a hashmap of canonical file paths to directory entries
fn discover_files<P: AsRef<Path>>(root_path: P) -> HashMap<PathBuf, DirEntry> {
    log::debug!(
        "Discovering all files under path {}",
        root_path.as_ref().display()
    );

    let mut discovered_files = HashMap::new();
    let walker = WalkDir::new(root_path)
        .into_iter()
        .filter_entry(|entry| {
            // skip hidden files and directories
            !entry
                .file_name()
                .to_string_lossy()
                .starts_with('.')
        });

    for entry in walker {
        match entry {
            Ok(entry) => {
                let path = entry.path().to_path_buf();
                if !entry.file_type().is_dir() {
                    // canonicalize the path to avoid duplicates
                    match fs::canonicalize(&path) {
                        Ok(canonical_path) => {
                            discovered_files.insert(canonical_path, entry);
                        }
                        Err(msg) => {
                            log::warn!(
                                "Couldn't canonicalize {}:\n{}",
                                path.display(),
                                msg
                            );
                        }
                    }
                }
            }
            Err(msg) => {
                log::error!("Error during walking: {}", msg);
            }
        }
    }

    discovered_files
}

fn scan_entry(path: &Path, metadata: Metadata) -> Result<IndexEntry> {
    if metadata.is_dir() {
        return Err(ArklibError::Path("Path is expected to be a file".into()));
    }

    let size = metadata.len();
    if size == 0 {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Empty resource",
        ))?;
    }

    let id = ResourceId::compute(size, path)?;
    let modified = metadata.modified()?;

    Ok(IndexEntry { id, modified })
}

fn scan_entries(
    entries: HashMap<PathBuf, DirEntry>,
) -> HashMap<PathBuf, IndexEntry> {
    entries
        .into_iter()
        .filter_map(|(path_buf, entry)| {
            let metadata = entry.metadata().ok()?;

            let path = path_buf.as_path();
            let result = scan_entry(path, metadata);
            match result {
                Err(msg) => {
                    log::error!(
                        "Couldn't retrieve metadata for {}:\n{}",
                        path.display(),
                        msg
                    );
                    None
                }
                Ok(entry) => Some((path_buf, entry)),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::fs;
    use crate::id::ResourceId;
    use crate::index::{discover_files, IndexEntry};
    use crate::initialize;
    use crate::ResourceIndex;
    use std::fs::File;
    #[cfg(target_os = "linux")]
    use std::fs::Permissions;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;

    use std::path::PathBuf;
    use std::time::SystemTime;
    use uuid::Uuid;

    const FILE_SIZE_1: u64 = 10;
    const FILE_SIZE_2: u64 = 11;

    const FILE_NAME_1: &str = "test1.txt";
    const FILE_NAME_2: &str = "test2.txt";
    const FILE_NAME_3: &str = "test3.txt";

    const CRC32_1: u32 = 3817498742;
    const CRC32_2: u32 = 1804055020;

    fn get_temp_dir() -> PathBuf {
        create_dir_at(std::env::temp_dir())
    }

    fn create_dir_at(path: PathBuf) -> PathBuf {
        let mut dir_path = path.clone();
        dir_path.push(Uuid::new_v4().to_string());
        std::fs::create_dir(&dir_path).expect("Could not create temp dir");
        dir_path
    }

    fn create_file_at(
        path: PathBuf,
        size: Option<u64>,
        name: Option<&str>,
    ) -> (File, PathBuf) {
        let mut file_path = path.clone();
        if let Some(file_name) = name {
            file_path.push(file_name);
        } else {
            file_path.push(Uuid::new_v4().to_string());
        }
        let file = File::create(file_path.clone())
            .expect("Could not create temp file");
        file.set_len(size.unwrap_or(0))
            .expect("Could not set file size");
        (file, file_path)
    }

    fn run_test_and_clean_up(
        test: impl FnOnce(PathBuf) + std::panic::UnwindSafe,
    ) {
        initialize();

        let path = get_temp_dir();
        let result = std::panic::catch_unwind(|| test(path.clone()));
        std::fs::remove_dir_all(path.clone())
            .expect("Could not clean up after test");
        if result.is_err() {
            panic!("{}", result.err().map(|_| "Test panicked").unwrap())
        }
        assert!(result.is_ok());
    }

    // resource index build

    #[test]
    fn resource_index_load_store() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(3), Some(FILE_NAME_1));
            create_file_at(path.clone(), Some(10), Some(FILE_NAME_2));
            let index = ResourceIndex::build(path.clone());

            index
                .store()
                .expect("Should store index successfully");

            let loaded_index = ResourceIndex::load(path.clone())
                .expect("Should load index successfully");

            assert_eq!(index, loaded_index);
        })
    }

    #[test]
    fn index_build_should_process_1_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);

            let actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 1);
            assert_eq!(actual.id2path.len(), 1);
            assert!(actual.id2path.contains_key(&ResourceId {
                data_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 1);
        })
    }

    #[test]
    fn index_build_should_process_colliding_files_correctly() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);

            let actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 2);
            assert_eq!(actual.id2path.len(), 1);
            assert!(actual.id2path.contains_key(&ResourceId {
                data_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert_eq!(actual.collisions.len(), 1);
            assert_eq!(actual.size(), 2);
        })
    }

    // resource index update

    #[test]
    fn update_all_should_handle_renamed_file_correctly() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));
            create_file_at(path.clone(), Some(FILE_SIZE_2), Some(FILE_NAME_2));

            let mut actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);

            // rename test2.txt to test3.txt
            let mut name_from = path.clone();
            name_from.push(FILE_NAME_2);
            let mut name_to = path.clone();
            name_to.push(FILE_NAME_3);
            std::fs::rename(name_from, name_to)
                .expect("Should rename file successfully");

            let update = actual
                .update_all()
                .expect("Should update index correctly");

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            assert_eq!(update.deleted.len(), 1);
            assert_eq!(update.added.len(), 1);
        })
    }

    #[test]
    fn update_all_should_index_new_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);

            let mut actual = ResourceIndex::build(path.clone());

            let (_, expected_path) =
                create_file_at(path.clone(), Some(FILE_SIZE_2), None);

            let update = actual
                .update_all()
                .expect("Should update index correctly");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 2);
            assert_eq!(actual.id2path.len(), 2);
            assert!(actual.id2path.contains_key(&ResourceId {
                data_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert!(actual.id2path.contains_key(&ResourceId {
                data_size: FILE_SIZE_2,
                crc32: CRC32_2,
            }));
            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 1);

            let added_key = fs::canonicalize(expected_path.clone())
                .expect("CanonicalPathBuf should be fine");
            assert_eq!(
                update
                    .added
                    .get(&added_key)
                    .expect("Key exists")
                    .clone(),
                ResourceId {
                    data_size: FILE_SIZE_2,
                    crc32: CRC32_2
                }
            )
        })
    }

    #[test]
    fn index_new_should_index_new_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);
            let mut index = ResourceIndex::build(path.clone());

            let (_, new_path) =
                create_file_at(path.clone(), Some(FILE_SIZE_2), None);

            let update = index
                .index_new(&new_path)
                .expect("Should update index correctly");

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 2);
            assert_eq!(index.id2path.len(), 2);
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: FILE_SIZE_1,
                crc32: CRC32_1,
            }));
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: FILE_SIZE_2,
                crc32: CRC32_2,
            }));
            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 1);

            let added_key = fs::canonicalize(new_path.clone())
                .expect("CanonicalPathBuf should be fine");
            assert_eq!(
                update
                    .added
                    .get(&added_key)
                    .expect("Key exists")
                    .clone(),
                ResourceId {
                    data_size: FILE_SIZE_2,
                    crc32: CRC32_2
                }
            )
        })
    }

    #[test]
    fn update_one_should_error_on_new_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);
            let mut index = ResourceIndex::build(path.clone());

            let (_, new_path) =
                create_file_at(path.clone(), Some(FILE_SIZE_2), None);

            let update = index.update_one(
                &new_path,
                ResourceId {
                    data_size: FILE_SIZE_2,
                    crc32: CRC32_2,
                },
            );

            assert!(update.is_err())
        })
    }

    #[test]
    fn update_one_should_index_delete_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));

            let mut actual = ResourceIndex::build(path.clone());

            let mut file_path = path.clone();
            file_path.push(FILE_NAME_1);
            std::fs::remove_file(file_path.clone())
                .expect("Should remove file successfully");

            let update = actual
                .update_one(
                    &file_path.clone(),
                    ResourceId {
                        data_size: FILE_SIZE_1,
                        crc32: CRC32_1,
                    },
                )
                .expect("Should update index successfully");

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 0);
            assert_eq!(actual.id2path.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 0);
            assert_eq!(update.deleted.len(), 1);
            assert_eq!(update.added.len(), 0);

            assert!(update.deleted.contains(&ResourceId {
                data_size: FILE_SIZE_1,
                crc32: CRC32_1
            }))
        })
    }

    #[test]
    fn update_all_should_error_on_files_without_permissions() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));
            let (file, _) = create_file_at(
                path.clone(),
                Some(FILE_SIZE_2),
                Some(FILE_NAME_2),
            );

            let mut actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            #[cfg(target_os = "linux")]
            file.set_permissions(Permissions::from_mode(0o222))
                .expect("Should be fine");

            let update = actual
                .update_all()
                .expect("Should update index correctly");

            assert_eq!(actual.collisions.len(), 0);
            assert_eq!(actual.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 0);
        })
    }

    // error cases

    #[test]
    fn update_one_should_not_update_absent_path() {
        run_test_and_clean_up(|path| {
            let mut missing_path = path.clone();
            missing_path.push("missing/directory");
            let mut actual = ResourceIndex::build(path.clone());
            let old_id = ResourceId {
                data_size: 1,
                crc32: 2,
            };
            let result = actual
                .update_one(&missing_path, old_id)
                .map(|i| i.deleted.clone().take(&old_id))
                .ok()
                .flatten();

            assert_eq!(
                result,
                Some(ResourceId {
                    data_size: 1,
                    crc32: 2,
                })
            );
        })
    }

    #[test]
    fn update_one_should_index_new_path() {
        run_test_and_clean_up(|path| {
            let mut missing_path = path.clone();
            missing_path.push("missing/directory");
            let mut actual = ResourceIndex::build(path.clone());
            let old_id = ResourceId {
                data_size: 1,
                crc32: 2,
            };
            let result = actual
                .update_one(&missing_path, old_id)
                .map(|i| i.deleted.clone().take(&old_id))
                .ok()
                .flatten();

            assert_eq!(
                result,
                Some(ResourceId {
                    data_size: 1,
                    crc32: 2,
                })
            );
        })
    }

    #[test]
    fn should_not_index_empty_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(0), None);
            let actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 0);
            assert_eq!(actual.id2path.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
        })
    }

    #[test]
    fn should_not_index_hidden_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(FILE_SIZE_1), Some(".hidden"));
            let actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 0);
            assert_eq!(actual.id2path.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
        })
    }

    #[test]
    fn should_not_index_1_empty_directory() {
        run_test_and_clean_up(|path| {
            create_dir_at(path.clone());

            let actual = ResourceIndex::build(path.clone());

            assert_eq!(actual.root, path.clone());
            assert_eq!(actual.path2id.len(), 0);
            assert_eq!(actual.id2path.len(), 0);
            assert_eq!(actual.collisions.len(), 0);
        })
    }

    #[test]
    fn discover_paths_should_not_walk_on_invalid_path() {
        run_test_and_clean_up(|path| {
            let mut missing_path = path.clone();
            missing_path.push("missing/directory");
            let actual = discover_files(missing_path);
            assert_eq!(actual.len(), 0);
        })
    }

    #[test]
    fn index_entry_order() {
        let old1 = IndexEntry {
            id: ResourceId {
                data_size: 1,
                crc32: 2,
            },
            modified: SystemTime::UNIX_EPOCH,
        };
        let old2 = IndexEntry {
            id: ResourceId {
                data_size: 2,
                crc32: 1,
            },
            modified: SystemTime::UNIX_EPOCH,
        };

        let new1 = IndexEntry {
            id: ResourceId {
                data_size: 1,
                crc32: 1,
            },
            modified: SystemTime::now(),
        };
        let new2 = IndexEntry {
            id: ResourceId {
                data_size: 1,
                crc32: 2,
            },
            modified: SystemTime::now(),
        };

        assert_eq!(new1, new1);
        assert_eq!(new2, new2);
        assert_eq!(old1, old1);
        assert_eq!(old2, old2);

        assert_ne!(new1, new2);
        assert_ne!(new1, old1);

        assert!(old2 > old1);
        assert!(new1 > old1);
        assert!(new1 > old2);
        assert!(new2 > old1);
        assert!(new2 > old2);
        assert!(new2 > new1);
    }

    /// Test the performance of `ResourceIndex::build` on a specific directory.
    ///
    /// This test evaluates the performance of building a resource
    /// index using the `ResourceIndex::build` method on a given directory.
    /// It measures the time taken to build the resource index and prints the
    /// number of collisions detected.
    #[test]
    fn test_build_resource_index() {
        use std::time::Instant;

        let path = "tests/"; // The path to the directory to index
        assert!(
            std::path::Path::new(path).is_dir(),
            "The provided path is not a directory or does not exist"
        );

        let start_time = Instant::now();
        let index = ResourceIndex::build(path.to_string());
        let elapsed_time = start_time.elapsed();

        println!("Number of paths: {}", index.id2path.len());
        println!("Number of resources: {}", index.id2path.len());
        println!("Number of collisions: {}", index.collisions.len());
        println!("Time taken: {:?}", elapsed_time);
    }
}
