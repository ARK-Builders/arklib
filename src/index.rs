use anyhow::anyhow;
use log;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::UNIX_EPOCH;
use std::time::{Duration, SystemTime};
use walkdir::{DirEntry, WalkDir};

use crate::{
    resource::ResourceId, ArklibError, Result, ARK_FOLDER, INDEX_PATH,
};

pub const RESOURCE_UPDATED_THRESHOLD: Duration = Duration::from_millis(1);
pub type Paths = HashSet<PathBuf>;
use crate::resource::ResourceIdTrait;

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

/// Represents an index of resources stored as files
/// in the filesystem under some prefix, or "root".
///
/// This struct maintains a mapping from resource IDs to their corresponding
/// file paths, as well as the mapping of the opposite direction from file
/// paths to index entries, which are simply resource IDs with modification
/// timestamps.
///
/// Additionally, it keeps track of collisions that occur during
/// indexing using non-cryptographic hash functions.
#[serde_as]
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ResourceIndex {
    /// A mapping of resource IDs to their corresponding file paths
    #[serde_as(as = "Vec<(_, _)>")]
    id2path: HashMap<ResourceId, PathBuf>,
    /// A mapping of file paths to their corresponding index entries
    path2id: HashMap<PathBuf, IndexEntry>,
    /// A mapping of resource IDs to the number of collisions they have
    pub collisions: HashMap<ResourceId, usize>,
    /// The root path of the index
    root: PathBuf,
}

/// Represents an external modification detected in the filesystem.
///
/// This struct holds information about resources that have been deleted
/// or added during an update operation on the resource index. Modification
/// of a resource is always represented as a deletion followed by an addition.
/// Renaming of a file doesn't really introduces any new resources, but
/// for consistency is represented same as modification
/// of the underlying file.
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
    /// Note that the amount of resource can be lower in presence of collisions
    pub fn count_files(&self) -> usize {
        self.path2id.len()
    }

    /// Returns the number of resources in the index
    pub fn count_resources(&self) -> usize {
        self.id2path.len()
    }

    /// Builds a new resource index from scratch using the root path
    ///
    /// This function recursively scans the directory structure starting from
    /// the root path, constructs index entries for each resource found, and
    /// populates the resource index
    pub fn build<P: AsRef<Path>>(root_path: P) -> Self {
        let root_path = fs::canonicalize(root_path.as_ref())
            .expect("Failed to canonicalize root path");

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
    /// [`ResourceIndex`] instance. It looks for the index file in
    /// `$root_path/.ark/index`.
    ///
    /// Note that the loaded index can be outdated and `update_all` needs to
    /// be called explicitly by the end-user. For automated updating and
    /// persisting the new index version, use [`ResourceIndex::provide()`] method.
    pub fn load<P: AsRef<Path>>(root_path: P) -> Result<Self> {
        let root_path: PathBuf = root_path.as_ref().to_owned();
        let root_path = fs::canonicalize(root_path)?;

        let index_path: PathBuf = root_path.join(ARK_FOLDER).join(INDEX_PATH);
        log::info!("Loading the index from file {}", index_path.display());
        let file = File::open(&index_path)?;
        let mut index = ResourceIndex {
            id2path: HashMap::new(),
            path2id: HashMap::new(),
            collisions: HashMap::new(),
            root: root_path.clone(),
        };

        // We should not return early in case of missing files
        let lines = BufReader::new(file).lines();
        for line in lines {
            let line = line?;

            let mut parts = line.split(' ');

            let modified = {
                let str = parts.next().ok_or(ArklibError::Parse)?;
                UNIX_EPOCH.add(Duration::from_millis(
                    str.parse().map_err(|_| ArklibError::Parse)?,
                ))
            };

            let id = {
                let str = parts.next().ok_or(ArklibError::Parse)?;
                ResourceId::from_str(str)?
            };

            let path: String =
                itertools::Itertools::intersperse(parts, " ").collect();
            let path: PathBuf = root_path.join(Path::new(&path));
            match fs::canonicalize(&path) {
                Ok(path) => {
                    log::trace!("[load] {} -> {}", id, path.display());
                    index.insert_entry(path, IndexEntry { id, modified });
                }
                Err(_) => {
                    log::warn!("File {} not found", path.display());
                    continue;
                }
            }
        }

        Ok(index)
    }

    /// Stores the resource index to the file system
    ///
    /// This function writes the index to the file system. It writes the index
    /// to `$root_path/.ark/index` and creates the directory if it's absent.
    pub fn store(&self) -> Result<()> {
        log::info!("Storing the index to file");

        let start = SystemTime::now();

        let index_path = self
            .root
            .to_owned()
            .join(ARK_FOLDER)
            .join(INDEX_PATH);

        let ark_dir = index_path.parent().unwrap();
        fs::create_dir_all(ark_dir)?;

        let mut file = File::create(index_path)?;

        let mut path2id: Vec<(&PathBuf, &IndexEntry)> =
            self.path2id.iter().collect();
        path2id.sort_by_key(|(_, entry)| *entry);

        for (path, entry) in path2id.iter() {
            log::trace!("[store] {} by path {}", entry.id, path.display());

            let timestamp = entry
                .modified
                .duration_since(UNIX_EPOCH)
                .map_err(|_| {
                    ArklibError::Other(anyhow!("Error using duration since"))
                })?
                .as_millis();

            let path =
                pathdiff::diff_paths(path.to_str().unwrap(), self.root.clone())
                    .ok_or(ArklibError::Path(
                        "Couldn't calculate path diff".into(),
                    ))?;

            writeln!(file, "{} {} {}", timestamp, entry.id, path.display())?;
        }

        log::trace!(
            "Storing the index took {:?}",
            start
                .elapsed()
                .map_err(|_| ArklibError::Other(anyhow!("SystemTime error")))
        );
        Ok(())
    }

    /// Provides the resource index, loading it if available or building it from
    /// scratch if not
    ///
    /// If the index exists at the provided `root_path`, it will be loaded,
    /// updated, and stored. If it doesn't exist, a new index will be built
    /// from scratch
    pub fn provide<P: AsRef<Path>>(root_path: P) -> Result<Self> {
        match Self::load(&root_path) {
            Ok(mut index) => {
                log::debug!("Index loaded: {} entries", index.path2id.len());

                let update = index.update_all()?;
                log::debug!(
                    "Index updated: {} added, {} deleted",
                    update.added.len(),
                    update.deleted.len()
                );
                index.store()?;

                Ok(index)
            }
            Err(e) => {
                log::warn!("{}", e.to_string());
                log::info!("Building the index from scratch");
                Ok(Self::build(root_path))
            }
        }
    }

    /// Updates the index based on the current state of the file system
    ///
    /// Returns an [`IndexUpdate`] object containing the paths of deleted and
    /// added resources
    pub fn update_all(&mut self) -> Result<IndexUpdate> {
        log::debug!("Updating the index");
        log::trace!("[update] known paths: {:?}", self.path2id.keys());

        let curr_entries = discover_files(self.root.clone());

        // assuming that collections manipulation is
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
        let mut updated_paths: HashMap<PathBuf, DirEntry> = HashMap::new();
        for (path, dir_entry) in curr_entries.iter() {
            if !preserved_paths.contains(path) {
                continue;
            }

            let our_entry = &self.path2id[path];
            let prev_modified = our_entry.modified;

            let result = dir_entry.metadata();
            if result.is_err() {
                log::error!(
                    "Couldn't retrieve metadata for {}: {}",
                    &path.display(),
                    result.err().unwrap()
                );
                continue;
            }
            let metadata = result.unwrap();

            let result = metadata.modified();
            if result.is_err() {
                log::error!(
                    "Couldn't retrieve timestamp for {}: {}",
                    &path.display(),
                    result.err().unwrap()
                );
                continue;
            }
            let curr_modified = result.unwrap();

            let elapsed = curr_modified
                .duration_since(prev_modified)
                .map_err(|e| {
                    ArklibError::Other(anyhow!("SystemTime error: {}", e))
                })?;

            if elapsed >= RESOURCE_UPDATED_THRESHOLD {
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
                updated_paths.insert(path.clone(), dir_entry.clone());
            }
        }

        let mut deleted: HashSet<ResourceId> = HashSet::new();
        // Get the paths to be deleted
        let paths_to_delete = prev_paths
            .difference(&preserved_paths)
            .cloned()
            .chain(updated_paths.keys().cloned());
        // Process each path: remove from the index and update the collisions
        for path in paths_to_delete {
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
                log::warn!(
                    "Path {} was not found in the index",
                    path.display()
                );
            }
        }

        // Scan entries for updated paths
        log::debug!("Checking added paths");
        let mut updated_entries = scan_entries(updated_paths);
        let created_entries = scan_entries(created_paths);
        // Combine updated and created entries
        updated_entries.extend(created_entries);
        // Filter entries not contained in id2path
        let added: HashMap<PathBuf, IndexEntry> = updated_entries
            .into_iter()
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

    /// Indexes a new entry identified by the provided path, updating the index
    /// accordingly.
    ///
    /// The caller must ensure that:
    /// - The index is up-to-date except for this single path
    /// - The path hasn't been indexed before
    ///
    /// Returns an error if:
    /// - The path does not exist
    /// - Metadata retrieval fails
    pub fn index_new(&mut self, path: &dyn AsRef<Path>) -> Result<IndexUpdate> {
        log::debug!(
            "{}",
            format!("Indexing a new entry: {}", path.as_ref().display())
        );

        if !path.as_ref().exists() {
            return Err(ArklibError::Path(format!(
                "Path {} doesn't exist",
                path.as_ref().display()
            )));
        }

        let path_buf = fs::canonicalize(path)?;
        let path = path_buf.as_path();

        let metadata = fs::metadata(path).map_err(|e| {
            ArklibError::Path(format!(
                "Couldn't to retrieve file metadata: {}",
                e
            ))
        })?;
        let new_entry = scan_entry(path, metadata)?;
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

    /// Updates a single entry in the index with a new resource located at the
    /// specified path, replacing the old resource associated with the given
    /// ID.
    ///
    /// # Restrictions
    ///
    /// The caller must ensure that:
    /// * the index is up-to-date except for this single path
    /// * the path has been indexed before
    /// * the path maps into `old_id`
    /// * the content by the path has been modified
    ///
    /// # Errors
    ///
    /// Returns an error if the path does not exist, if the path is a directory
    /// or an empty file, if the index cannot find the specified path, or if
    /// the content of the path has not been modified.
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

        let metadata = fs::metadata(path);
        if metadata.is_err() {
            log::debug!("Path {:?} was removed", &path);
            return self.forget_id(old_id);
        }
        // we are sure that the path exists
        let metadata = metadata.unwrap();

        let new_entry = scan_entry(path, metadata);
        if new_entry.is_err() {
            log::debug!("Path {:?} is a directory or empty file", &path);
            return self.forget_path(path, old_id);
        }
        // we are sure that the path is a file and not empty
        let new_entry = new_entry.unwrap();

        // valid resource exists by the path

        let curr_entry = self.path2id.get(path).ok_or(
            // if the path is not indexed, then we can't have
            // `old_id`
            // if you want to index new path, use `index_new` method
            ArklibError::Path("Couldn't find the path in the index".into()),
        )?;

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

    /// Inserts an entry into the index, updating associated data structures
    ///
    /// If the entry ID already exists in the index, it handles collisions
    /// appropriately
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

    /// Removes the given resource ID from the index and returns an update
    /// containing the deleted entries
    pub fn forget_id(&mut self, old_id: ResourceId) -> Result<IndexUpdate> {
        log::debug!("Forgetting a single entry in the index");

        // Collect all paths associated with the old ID
        let mut old_paths = Vec::new();
        for (path, entry) in &self.path2id {
            if entry.id == old_id {
                old_paths.push(path.clone());
            }
        }

        // Remove entries from path2id and id2path
        for path in &old_paths {
            self.path2id.remove(path);
        }
        self.id2path.remove(&old_id);

        let mut deleted = HashSet::new();
        deleted.insert(old_id);

        Ok(IndexUpdate {
            added: HashMap::new(),
            deleted,
        })
    }

    /// Removes an entry with the specified path and updates the collision
    /// information accordingly
    ///
    /// Returns an update containing the deleted entries
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
            match self.id2path.get(&old_id) {
                Some(collided_path) => {
                    let old_path =
                        self.id2path.insert(old_id, collided_path.clone());

                    debug_assert_eq!(
                        old_path.unwrap().as_path(),
                        path,
                        "Must forget the requested path"
                    );
                }
                None => {
                    return Err(ArklibError::Collision(
                        "Illegal state of collision tracker".into(),
                    ));
                }
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
        .min_depth(1)
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

/// Scans a single file entry and extracts its metadata to create an index entry
///
/// Returns an error if the path is a directory or if the file is empty
fn scan_entry(path: &Path, metadata: Metadata) -> Result<IndexEntry> {
    if metadata.is_dir() {
        return Err(ArklibError::Path("Path is expected to be a file".into()));
    }

    let size = metadata.len();
    if size == 0 {
        return Err(ArklibError::Path("Empty file".into()));
    }

    let id = ResourceId::compute(size, path)?;
    let modified = metadata.modified()?;

    // We need to keep precision up to milliseconds only to avoid
    // compatibility issues with different file systems (eg. Android)
    let duration = modified
        .duration_since(UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!")
        .as_millis();
    let modified =
        UNIX_EPOCH + std::time::Duration::from_millis(duration as u64);

    Ok(IndexEntry { id, modified })
}

/// Scans multiple file entries and creates index entries for each one
///
/// Returns a hashmap of file paths to their corresponding index entries
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
    use crate::index::{discover_files, IndexEntry};
    use crate::initialize;
    use crate::resource::ResourceId;
    use crate::ResourceIndex;
    use std::fs::File;
    #[cfg(target_family = "unix")]
    use std::fs::Permissions;
    #[cfg(target_family = "unix")]
    use std::os::unix::fs::PermissionsExt;
    use tempdir::TempDir;

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

    #[test]
    fn resource_index_load_store() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let temp_dir = temp_dir.into_path();

        create_file_at(
            temp_dir.to_owned(),
            Some(FILE_SIZE_1),
            Some(FILE_NAME_1),
        );
        let index = ResourceIndex::build(temp_dir.to_owned());

        index
            .store()
            .expect("Should store index successfully");

        let loaded_index = ResourceIndex::load(temp_dir.to_owned())
            .expect("Should load index successfully");

        // Assert that the loaded index is equal to the original index
        assert_eq!(index, loaded_index);
    }

    #[test]
    fn index_build_should_process_1_file_successfully() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let temp_dir = temp_dir.into_path();

        create_file_at(temp_dir.to_owned(), Some(FILE_SIZE_1), None);
        let actual = ResourceIndex::build(temp_dir.to_owned());

        let canonical_path = fs::canonicalize(temp_dir.clone())
            .expect("CanonicalPathBuf should be fine");

        assert_eq!(actual.root, canonical_path.to_owned());
        assert_eq!(actual.path2id.len(), 1);
        assert_eq!(actual.id2path.len(), 1);
        assert!(actual.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_1,
            hash: CRC32_1,
        }));
        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 1);
    }

    #[test]
    fn index_build_should_process_colliding_files_correctly() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.to_owned(), Some(FILE_SIZE_1), None);
        create_file_at(path.to_owned(), Some(FILE_SIZE_1), None);
        let actual = ResourceIndex::build(path.to_owned());

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(actual.root, canonical_path.to_owned());
        assert_eq!(actual.path2id.len(), 2);
        assert_eq!(actual.id2path.len(), 1);
        assert!(actual.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_1,
            hash: CRC32_1,
        }));
        assert_eq!(actual.collisions.len(), 1);
        assert_eq!(actual.count_files(), 2);
    }

    #[test]
    fn update_all_should_handle_renamed_file_correctly() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.to_owned(), Some(FILE_SIZE_1), Some(FILE_NAME_1));
        create_file_at(path.to_owned(), Some(FILE_SIZE_2), Some(FILE_NAME_2));
        let mut actual = ResourceIndex::build(path.to_owned());

        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 2);

        // rename test2.txt to test3.txt
        let mut name_from = path.to_owned();
        name_from.push(FILE_NAME_2);
        let mut name_to = path.to_owned();
        name_to.push(FILE_NAME_3);
        std::fs::rename(name_from, name_to)
            .expect("Should rename file successfully");

        let update = actual
            .update_all()
            .expect("Should update index correctly");

        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 2);
        assert_eq!(update.deleted.len(), 1);
        assert_eq!(update.added.len(), 1);
    }

    #[test]
    fn update_all_should_index_new_file_successfully() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.to_owned(), Some(FILE_SIZE_1), None);
        let mut actual = ResourceIndex::build(path.to_owned());
        let (_, expected_path) =
            create_file_at(path.to_owned(), Some(FILE_SIZE_2), None);
        let update = actual
            .update_all()
            .expect("Should update index correctly");

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(actual.root, canonical_path);
        assert_eq!(actual.path2id.len(), 2);
        assert_eq!(actual.id2path.len(), 2);
        assert!(actual.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_1,
            hash: CRC32_1,
        }));
        assert!(actual.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_2,
            hash: CRC32_2,
        }));
        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 2);
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
                hash: CRC32_2
            }
        )
    }

    #[test]
    fn index_new_adds_canonical_path() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        let (_, new_path) =
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);
        let mut index = ResourceIndex::build(path.clone());

        let canonical_path =
            fs::canonicalize(&new_path).expect("Failed to canonicalize path");

        let update = index.index_new(&new_path);

        // Ensure that the non-canonical path is added to the index with its
        // canonicalized form
        assert!(update.is_ok());
        assert_eq!(index.id2path.values().next(), Some(&canonical_path));
    }

    #[test]
    fn index_new_should_index_new_file_successfully() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.clone(), Some(FILE_SIZE_1), None);
        let mut index = ResourceIndex::build(path.clone());
        let (_, new_path) =
            create_file_at(path.clone(), Some(FILE_SIZE_2), None);
        let update = index
            .index_new(&new_path)
            .expect("Should update index correctly");

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(index.root, canonical_path.clone());
        assert_eq!(index.path2id.len(), 2);
        assert_eq!(index.id2path.len(), 2);
        assert!(index.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_1,
            hash: CRC32_1,
        }));
        assert!(index.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_2,
            hash: CRC32_2,
        }));
        assert_eq!(index.collisions.len(), 0);
        assert_eq!(index.count_files(), 2);
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
                hash: CRC32_2
            }
        )
    }

    #[test]
    fn update_one_should_error_on_new_file() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.clone(), Some(FILE_SIZE_1), None);
        let mut index = ResourceIndex::build(path.clone());
        let (_, new_path) =
            create_file_at(path.clone(), Some(FILE_SIZE_2), None);
        let update = index.update_one(
            &new_path,
            ResourceId {
                data_size: FILE_SIZE_2,
                hash: CRC32_2,
            },
        );

        assert!(update.is_err())
    }

    #[test]
    fn update_one_should_index_delete_file_successfully() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

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
                    hash: CRC32_1,
                },
            )
            .expect("Should update index successfully");

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(actual.root, canonical_path);
        assert_eq!(actual.path2id.len(), 0);
        assert_eq!(actual.id2path.len(), 0);
        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 0);
        assert_eq!(update.deleted.len(), 1);
        assert_eq!(update.added.len(), 0);

        assert!(update.deleted.contains(&ResourceId {
            data_size: FILE_SIZE_1,
            hash: CRC32_1
        }))
    }

    #[test]
    fn update_all_should_error_on_files_without_permissions() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.clone(), Some(FILE_SIZE_1), Some(FILE_NAME_1));
        let (file, _) =
            create_file_at(path.clone(), Some(FILE_SIZE_2), Some(FILE_NAME_2));
        let mut actual = ResourceIndex::build(path.clone());

        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 2);
        #[cfg(target_family = "unix")]
        file.set_permissions(Permissions::from_mode(0o222))
            .expect("Should be fine");

        let update = actual
            .update_all()
            .expect("Should update index correctly");

        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 2);
        assert_eq!(update.deleted.len(), 0);
        assert_eq!(update.added.len(), 0);
    }

    // error cases

    #[test]
    fn update_one_should_not_update_absent_path() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        let mut missing_path = path.clone();
        missing_path.push("missing/directory");
        let mut actual = ResourceIndex::build(path.clone());
        let old_id = ResourceId {
            data_size: 1,
            hash: 2,
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
                hash: 2,
            })
        );
    }

    #[test]
    fn update_one_should_index_new_path() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        let mut missing_path = path.clone();
        missing_path.push("missing/directory");
        let mut actual = ResourceIndex::build(path.clone());
        let old_id = ResourceId {
            data_size: 1,
            hash: 2,
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
                hash: 2,
            })
        )
    }

    #[test]
    fn should_not_index_empty_file() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.clone(), Some(0), None);
        let actual = ResourceIndex::build(path.clone());

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(actual.root, canonical_path);
        assert_eq!(actual.path2id.len(), 0);
        assert_eq!(actual.id2path.len(), 0);
        assert_eq!(actual.collisions.len(), 0);
    }

    #[test]
    fn should_not_index_hidden_file() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_file_at(path.clone(), Some(FILE_SIZE_1), Some(".hidden"));
        let actual = ResourceIndex::build(path.clone());

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(actual.root, canonical_path);
        assert_eq!(actual.path2id.len(), 0);
        assert_eq!(actual.id2path.len(), 0);
        assert_eq!(actual.collisions.len(), 0);
    }

    #[test]
    fn should_not_index_1_empty_directory() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        create_dir_at(path.clone());

        let actual = ResourceIndex::build(path.clone());

        let canonical_path = fs::canonicalize(path.clone())
            .expect("CanonicalPathBuf should be fine");
        assert_eq!(actual.root, canonical_path);
        assert_eq!(actual.path2id.len(), 0);
        assert_eq!(actual.id2path.len(), 0);
        assert_eq!(actual.collisions.len(), 0);
    }

    #[test]
    fn discover_paths_should_not_walk_on_invalid_path() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        let mut missing_path = path.clone();
        missing_path.push("missing/directory");
        let actual = discover_files(missing_path);

        assert_eq!(actual.len(), 0);
    }

    #[test]
    fn discover_files_adds_canonical_paths() {
        let temp_dir = TempDir::new("arklib_test")
            .expect("Failed to create temporary directory");
        let path = temp_dir.into_path();

        let (_, file1_path) =
            create_file_at(path.clone(), Some(FILE_SIZE_1), None);
        let (_, file2_path) =
            create_file_at(path.clone(), Some(FILE_SIZE_2), None);

        let discovered_files = discover_files(path.clone());

        let canonical_file1_path =
            fs::canonicalize(&file1_path).expect("Failed to canonicalize path");
        let canonical_file2_path =
            fs::canonicalize(&file2_path).expect("Failed to canonicalize path");

        // Ensure that the discovered files contain the canonical paths
        assert_eq!(discovered_files.len(), 2);
        assert!(discovered_files.contains_key(&canonical_file1_path));
        assert!(discovered_files.contains_key(&canonical_file2_path));
    }

    #[test]
    fn test_index_hidden_directory() {
        let temp_dir = TempDir::new(".arklib_test")
            .expect("Failed to create temporary directory");
        let temp_dir = temp_dir.into_path();

        create_file_at(temp_dir.to_owned(), Some(FILE_SIZE_1), None);
        let actual = ResourceIndex::build(temp_dir.to_owned());

        let canonical_path = fs::canonicalize(temp_dir.clone())
            .expect("CanonicalPathBuf should be fine");

        assert_eq!(actual.root, canonical_path.to_owned());
        assert_eq!(actual.path2id.len(), 1);
        assert_eq!(actual.id2path.len(), 1);
        assert!(actual.id2path.contains_key(&ResourceId {
            data_size: FILE_SIZE_1,
            hash: CRC32_1,
        }));
        assert_eq!(actual.collisions.len(), 0);
        assert_eq!(actual.count_files(), 1);
    }

    #[test]
    fn index_entry_order() {
        let old1 = IndexEntry {
            id: ResourceId {
                data_size: 1,
                hash: 2,
            },
            modified: SystemTime::UNIX_EPOCH,
        };
        let old2 = IndexEntry {
            id: ResourceId {
                data_size: 2,
                hash: 1,
            },
            modified: SystemTime::UNIX_EPOCH,
        };

        let new1 = IndexEntry {
            id: ResourceId {
                data_size: 1,
                hash: 1,
            },
            modified: SystemTime::now(),
        };
        let new2 = IndexEntry {
            id: ResourceId {
                data_size: 1,
                hash: 2,
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
