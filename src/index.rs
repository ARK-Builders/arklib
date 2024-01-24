use anyhow::anyhow;
use canonical_path::{CanonicalPath, CanonicalPathBuf};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata};
use std::io::{BufRead, BufReader, Write};
use std::ops::Add;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use walkdir::{DirEntry, WalkDir};

use log;

use crate::id::ResourceId;
use crate::{ArklibError, Result, ARK_FOLDER, INDEX_PATH};

#[derive(Eq, Ord, PartialEq, PartialOrd, Hash, Clone, Debug)]
pub struct IndexEntry {
    pub modified: SystemTime,
    pub id: ResourceId,
}

#[derive(PartialEq, Clone, Debug)]
pub struct ResourceIndex {
    pub id2path: HashMap<ResourceId, CanonicalPathBuf>,
    pub path2id: HashMap<CanonicalPathBuf, IndexEntry>,

    pub collisions: HashMap<ResourceId, usize>,
    root: PathBuf,
}

#[derive(PartialEq, Debug)]
pub struct IndexUpdate {
    pub deleted: HashSet<ResourceId>,
    pub added: HashMap<CanonicalPathBuf, ResourceId>,
}

impl IndexUpdate {
    pub fn empty() -> Self {
        IndexUpdate {
            deleted: HashSet::new(),
            added: HashMap::new(),
        }
    }

    pub fn added(path: CanonicalPathBuf, id: ResourceId) -> Self {
        let mut added = HashMap::new();
        added.insert(path, id);

        IndexUpdate {
            deleted: HashSet::new(),
            added,
        }
    }

    pub fn deleted(id: ResourceId) -> Self {
        let mut deleted = HashSet::new();
        deleted.insert(id);

        IndexUpdate {
            deleted,
            added: HashMap::new(),
        }
    }
}

pub const RESOURCE_UPDATED_THRESHOLD: Duration = Duration::from_millis(1);

pub type Paths = HashSet<CanonicalPathBuf>;

impl ResourceIndex {
    pub fn size(&self) -> usize {
        //the actual size is lower in presence of collisions
        self.path2id.len()
    }

    pub fn build<P: AsRef<Path>>(root_path: P) -> Self {
        log::info!("Building the index from scratch");
        let root_path: PathBuf = root_path.as_ref().to_owned();

        let entries = discover_paths(&root_path);
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

    pub fn load<P: AsRef<Path>>(root_path: P) -> Result<Self> {
        let root_path: PathBuf = root_path.as_ref().to_owned();

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
        for line in BufReader::new(file).lines().flatten() {
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
            match CanonicalPathBuf::canonicalize(&path) {
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

        let mut path2id: Vec<(&CanonicalPathBuf, &IndexEntry)> =
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

        let curr_entries = discover_paths(self.root.clone());

        // assuming that collections manipulation is
        // quicker than asking `path.exists()` for every path
        let curr_paths: Paths = curr_entries.keys().cloned().collect();
        let prev_paths: Paths = self.path2id.keys().cloned().collect();
        let preserved_paths: Paths = curr_paths
            .intersection(&prev_paths)
            .cloned()
            .collect();

        let created_paths: HashMap<CanonicalPathBuf, DirEntry> = curr_entries
            .iter()
            .filter_map(|(path, entry)| {
                if !preserved_paths.contains(path.as_canonical_path()) {
                    Some((path.clone(), entry.clone()))
                } else {
                    None
                }
            })
            .collect();

        log::debug!("Checking updated paths");
        let updated_paths: HashMap<CanonicalPathBuf, DirEntry> = curr_entries
            .into_iter()
            .filter(|(path, dir_entry)| {
                if !preserved_paths.contains(path.as_canonical_path()) {
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
                if let Some(entry) =
                    self.path2id.remove(path.as_canonical_path())
                {
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

        let added: HashMap<CanonicalPathBuf, IndexEntry> =
            scan_entries(updated_paths)
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

        let added: HashMap<CanonicalPathBuf, ResourceId> = added
            .into_iter()
            .map(|(path, entry)| (path, entry.id))
            .collect();

        Ok(IndexUpdate { deleted, added })
    }

    // The caller must ensure that:
    // * the index is up-to-date except this single path
    // * the path hasn't been indexed before
    //
    // Should only be used if reactive updates are not possible.
    pub fn track_addition(
        &mut self,
        path: &dyn AsRef<Path>,
    ) -> Result<IndexUpdate> {
        log::debug!("Tracking a single addition in the index");

        if !path.as_ref().exists() {
            return Err(ArklibError::Path(
                "Absent paths cannot be indexed".into(),
            ));
        }

        let path_buf = CanonicalPathBuf::canonicalize(path)?;
        let path = path_buf.as_canonical_path();

        match fs::metadata(path) {
            Err(_) => Err(ArklibError::Path(
                "Couldn't to retrieve file metadata".into(),
            )),
            Ok(metadata) => match scan_entry(path, metadata) {
                Err(_) => Err(ArklibError::Path(
                    "The path points to a directory or empty file".into(),
                )),
                Ok(entry) => {
                    let result = IndexUpdate::added(path_buf.clone(), entry.id);
                    self.insert_entry(path_buf, entry);

                    Ok(result)
                }
            },
        }
    }

    // The caller must ensure that:
    // * the index is up-to-date except this single id
    // * the resource with this id has been indexed before
    // * the resource with this id doesn't exist anymore
    //
    // Should only be used if reactive updates are not possible.
    pub fn track_deletion(&mut self, id: ResourceId) -> Result<IndexUpdate> {
        log::debug!("Tracking a single deletion in the index");

        let indexed_path = self.id2path.get(&id);
        if indexed_path.is_none() {
            return Err(ArklibError::Path(
                "The id cannot be found in the index".into(),
            ));
        }

        let indexed_path = indexed_path.unwrap().clone();
        self.forget_entry(indexed_path.as_canonical_path(), id);

        Ok(IndexUpdate::deleted(id))
    }

    // The caller must ensure that:
    // * the index is up-to-date except this single path
    // * the path has been indexed before
    // * the path has been mapped into `old_id`
    //
    // Should only be used if reactive updates are not possible.
    // Returns an empty update if the resource hasn't been really updated.
    pub fn track_update(
        &mut self,
        path: &dyn AsRef<Path>,
        old_id: ResourceId,
    ) -> Result<IndexUpdate> {
        log::debug!("Tracking a single update in the index");

        if let Some(indexed_path) = self.id2path.get(&old_id) {
            if indexed_path.as_path() != path.as_ref() {
                return Err(ArklibError::Path(
                    "The path isn't indexed or doesn't map to the id".into(),
                ));
            }
        }

        return fs::metadata(path)
            .map_err(ArklibError::Io)
            .and_then(|metadata| {
                let path_buf = CanonicalPathBuf::canonicalize(path)?;
                let path = path_buf.as_canonical_path();

                log::trace!(
                    "[update] paths {:?} has id {:?}",
                    path,
                    self.path2id[path]
                );

                let curr_entry = &self.path2id.get(path);
                if curr_entry.is_none() {
                    return Err(ArklibError::Path("The path hasn't been indexed before".into()));
                }

                let curr_entry = curr_entry.unwrap();

                match scan_entry(path, metadata) {
                    Err(e) => Err(e),
                    Ok(new_entry) => Ok({
                        // valid resource exists by the path

                        if curr_entry.id != new_entry.id {
                            // new resource exists by the path

                            let mut deleted = HashSet::new();
                            deleted.insert(old_id);

                            let mut added = HashMap::new();
                            added.insert(path_buf.clone(), new_entry.id);

                            self.forget_entry(path, old_id);
                            self.insert_entry(path_buf, new_entry);

                            IndexUpdate { deleted, added }

                        } else {
                            // the content wasn't really updated because hash didn't change
                            // in rare cases we are here due to hash collision

                            if curr_entry.modified == new_entry.modified {
                                log::warn!("path {:?} was not modified", &path);
                            } else {
                                log::warn!(
                                    "path {:?} was modified but not its content",
                                    &path
                                );
                            }

                            IndexUpdate::empty()
                        }
                    }),
                }
            });
    }

    fn insert_entry(&mut self, path: CanonicalPathBuf, entry: IndexEntry) {
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

    fn forget_entry(&mut self, path: &CanonicalPath, id: ResourceId) {
        let removed = self.path2id.remove(path);
        debug_assert!(removed.is_some(), "The path is not indexed");
        let removed = removed.unwrap();

        debug_assert_eq!(removed.id, id, "The path is mapped to another id");

        if let Some(collisions) = self.collisions.get_mut(&id) {
            debug_assert!(
                *collisions > 1,
                "Any collision must involve at least 2 resources"
            );
            *collisions -= 1;

            if *collisions == 1 {
                self.collisions.remove(&id);
            }

            // minor performance issue:
            // we must find path of one of the collided
            // resources and use it as new value
            let collided_path =
                self.path2id.iter().find_map(|(path, entry)| {
                    if entry.id == id {
                        Some(path)
                    } else {
                        None
                    }
                });

            debug_assert!(
                collided_path.is_some(),
                "Illegal state of collision tracker"
            );
            let collided_path = collided_path.unwrap();

            let old_path = self.id2path.insert(id, collided_path.clone());
            debug_assert_eq!(
                old_path.unwrap().as_canonical_path(),
                path,
                "Must forget the requested path"
            );
        } else {
            self.id2path.remove(&id);
        }
    }
}

fn discover_paths<P: AsRef<Path>>(
    root_path: P,
) -> HashMap<CanonicalPathBuf, DirEntry> {
    log::debug!(
        "Discovering all files under path {}",
        root_path.as_ref().display()
    );

    WalkDir::new(root_path)
        .into_iter()
        .filter_entry(|entry| !is_hidden(entry))
        .filter_map(|result| match result {
            Ok(entry) => {
                let path = entry.path();
                if !entry.file_type().is_dir() {
                    match CanonicalPathBuf::canonicalize(path) {
                        Ok(canonical_path) => Some((canonical_path, entry)),
                        Err(msg) => {
                            log::warn!(
                                "Couldn't canonicalize {}:\n{}",
                                path.display(),
                                msg
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            }
            Err(msg) => {
                log::error!("Error during walking: {}", msg);
                None
            }
        })
        .collect()
}

fn scan_entry(path: &CanonicalPath, metadata: Metadata) -> Result<IndexEntry> {
    if metadata.is_dir() {
        return Err(ArklibError::Path("Path is expected to be a file".into()));
    }

    let size = metadata.len();
    if size == 0 {
        return Err(ArklibError::Path("Resource cannot be empty".into()));
    }

    let id = ResourceId::compute(size, path)?;
    let modified = metadata.modified()?;

    Ok(IndexEntry { id, modified })
}

fn scan_entries(
    entries: HashMap<CanonicalPathBuf, DirEntry>,
) -> HashMap<CanonicalPathBuf, IndexEntry> {
    entries
        .into_iter()
        .filter_map(|(path_buf, entry)| {
            let metadata = entry.metadata().ok()?;

            let path = path_buf.as_canonical_path();
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

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use crate::id::ResourceId;
    use crate::index::{discover_paths, IndexEntry};
    use crate::initialize;
    use crate::ResourceIndex;
    use canonical_path::CanonicalPathBuf;
    use std::default;
    use std::fs::File;
    #[cfg(target_os = "linux")]
    use std::fs::Permissions;
    use std::io::Write;
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::PermissionsExt;

    use rand::Rng;

    use std::path::PathBuf;
    use std::time::SystemTime;
    use uuid::Uuid;

    const DATA_SIZE_1: u64 = 10;
    const DATA_SIZE_2: u64 = 11;
    const MODIFIED_SIZE: u64 = 12;

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

    fn modify_file(file: &mut File) {
        let bytes = rand::thread_rng().gen::<[u8; MODIFIED_SIZE as usize]>();
        file.write(&bytes)
            .expect("Couldn't write into the file");
    }

    fn resource_id_1() -> ResourceId {
        ResourceId {
            data_size: DATA_SIZE_1,
            crc32: CRC32_1,
        }
    }

    fn resource_id_2() -> ResourceId {
        ResourceId {
            data_size: DATA_SIZE_2,
            crc32: CRC32_2,
        }
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
    fn index_build_should_ignore_empty_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(0), None);
            let index = ResourceIndex::build(path.clone());

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 0);
            assert_eq!(index.id2path.len(), 0);
            assert_eq!(index.collisions.len(), 0);
        })
    }

    #[test]
    fn index_build_should_ignore_hidden_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), Some(".hidden"));
            let index = ResourceIndex::build(path.clone());

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 0);
            assert_eq!(index.id2path.len(), 0);
            assert_eq!(index.collisions.len(), 0);
        })
    }

    #[test]
    fn index_build_should_ignore_empty_directory() {
        run_test_and_clean_up(|path| {
            create_dir_at(path.clone());
            let index = ResourceIndex::build(path.clone());

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 0);
            assert_eq!(index.id2path.len(), 0);
            assert_eq!(index.collisions.len(), 0);
        })
    }

    #[test]
    fn index_build_should_handle_1_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), None);
            let index = ResourceIndex::build(path.clone());

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 1);
            assert_eq!(index.id2path.len(), 1);
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: DATA_SIZE_1,
                crc32: CRC32_1,
            }));
            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 1);
        })
    }

    #[test]
    fn index_build_should_handle_colliding_files_correctly() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), None);
            create_file_at(path.clone(), Some(DATA_SIZE_1), None);
            let index = ResourceIndex::build(path.clone());

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 2);
            assert_eq!(index.id2path.len(), 1);
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: DATA_SIZE_1,
                crc32: CRC32_1,
            }));
            assert_eq!(index.collisions.len(), 1);
            assert_eq!(index.size(), 2);
        })
    }

    // resource index update

    #[test]
    fn update_all_should_handle_renamed_file_correctly() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), Some(FILE_NAME_1));
            create_file_at(path.clone(), Some(DATA_SIZE_2), Some(FILE_NAME_2));
            let mut index = ResourceIndex::build(path.clone());

            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);

            // rename test2.txt to test3.txt
            let mut name_from = path.clone();
            name_from.push(FILE_NAME_2);
            let mut name_to = path.clone();
            name_to.push(FILE_NAME_3);
            std::fs::rename(name_from, name_to)
                .expect("Should rename file successfully");

            let update = index
                .update_all()
                .expect("Should update index correctly");

            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);
            assert_eq!(update.deleted.len(), 1);
            assert_eq!(update.added.len(), 1);
        })
    }

    #[test]
    fn update_all_should_index_new_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), None);
            let mut index = ResourceIndex::build(path.clone());

            let (_, expected_path) =
                create_file_at(path.clone(), Some(DATA_SIZE_2), None);

            let update = index
                .update_all()
                .expect("Should update index correctly");

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 2);
            assert_eq!(index.id2path.len(), 2);
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: DATA_SIZE_1,
                crc32: CRC32_1,
            }));
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: DATA_SIZE_2,
                crc32: CRC32_2,
            }));
            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 1);

            let added_key =
                CanonicalPathBuf::canonicalize(expected_path.clone())
                    .expect("CanonicalPathBuf should be fine");
            assert_eq!(
                update
                    .added
                    .get(&added_key)
                    .expect("Key exists")
                    .clone(),
                ResourceId {
                    data_size: DATA_SIZE_2,
                    crc32: CRC32_2
                }
            )
        })
    }

    #[test]
    fn update_all_should_error_on_files_without_permissions() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), Some(FILE_NAME_1));
            let (file, _) = create_file_at(
                path.clone(),
                Some(DATA_SIZE_2),
                Some(FILE_NAME_2),
            );

            let mut index = ResourceIndex::build(path.clone());

            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);
            #[cfg(target_os = "linux")]
            file.set_permissions(Permissions::from_mode(0o222))
                .expect("Should be fine");

            let update = index
                .update_all()
                .expect("Should update index correctly");

            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 0);
        })
    }

    #[test]
    fn track_addition_should_index_new_file_successfully() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), None);
            let mut index = ResourceIndex::build(path.clone());

            let (_, new_path) =
                create_file_at(path.clone(), Some(DATA_SIZE_2), None);

            let update = index
                .track_addition(&new_path)
                .expect("Should update index correctly");

            assert_eq!(index.root, path.clone());
            assert_eq!(index.path2id.len(), 2);
            assert_eq!(index.id2path.len(), 2);
            assert!(index.id2path.contains_key(&ResourceId {
                data_size: DATA_SIZE_1,
                crc32: CRC32_1,
            }));
            assert!(index.id2path.contains_key(&resource_id_2()));
            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 2);
            assert_eq!(update.deleted.len(), 0);
            assert_eq!(update.added.len(), 1);

            let added_key = CanonicalPathBuf::canonicalize(new_path.clone())
                .expect("CanonicalPathBuf should be fine");
            assert_eq!(
                update
                    .added
                    .get(&added_key)
                    .expect("Key exists")
                    .clone(),
                ResourceId {
                    data_size: DATA_SIZE_2,
                    crc32: CRC32_2
                }
            )
        })
    }

    #[test]
    fn track_update_should_error_on_new_file() {
        run_test_and_clean_up(|path| {
            create_file_at(path.clone(), Some(DATA_SIZE_1), None);
            let mut index = ResourceIndex::build(path.clone());

            let (_, new_path) =
                create_file_at(path.clone(), Some(DATA_SIZE_2), None);

            let update = index.track_update(&new_path, resource_id_2());

            assert!(update.is_err())
        })
    }

    #[test]
    fn track_update_should_error_on_absent_path() {
        run_test_and_clean_up(|path| {
            let mut index = ResourceIndex::build(path.clone());

            let mut missing_path = path.clone();
            missing_path.push("missing/directory");

            let result = index.track_update(&missing_path, resource_id_1());

            assert!(result.is_err());
        })
    }

    #[test]
    fn track_update_should_index_modified_paths() {
        run_test_and_clean_up(|root_path| {
            let (mut file, file_path) = create_file_at(
                root_path.clone(),
                Some(DATA_SIZE_1),
                Some(FILE_NAME_1),
            );
            let mut index = ResourceIndex::build(root_path.clone());

            modify_file(&mut file);

            let result = index
                .track_update(&file_path, resource_id_1())
                .unwrap();

            assert_eq!(result.deleted.len(), 1);
            assert_eq!(result.added.len(), 1);

            let deleted = result.deleted.into_iter().next().unwrap();
            let added = result.added.into_iter().next().unwrap();

            assert_eq!(deleted.data_size, DATA_SIZE_1);
            assert_eq!(deleted.crc32, CRC32_1);
            assert_eq!(added.0.as_path(), file_path.as_path());
            assert_eq!(added.1.data_size, MODIFIED_SIZE);
        })
    }

    #[test]
    fn track_deletion_should_unindex_deleted_file() {
        run_test_and_clean_up(|root_path| {
            let (_, file_path) = create_file_at(
                root_path.clone(),
                Some(DATA_SIZE_1),
                Some(FILE_NAME_1),
            );
            let mut index = ResourceIndex::build(root_path.clone());

            std::fs::remove_file(file_path.clone())
                .expect("Should remove file successfully");

            let update = index
                .track_deletion(resource_id_1())
                .expect("Should update index successfully");

            assert_eq!(index.root, root_path.clone());
            assert_eq!(index.path2id.len(), 0);
            assert_eq!(index.id2path.len(), 0);
            assert_eq!(index.collisions.len(), 0);
            assert_eq!(index.size(), 0);
            assert_eq!(update.deleted.len(), 1);
            assert_eq!(update.added.len(), 0);

            assert!(update.deleted.contains(&ResourceId {
                data_size: DATA_SIZE_1,
                crc32: CRC32_1
            }))
        })
    }

    #[test]
    fn discover_paths_should_not_walk_on_invalid_path() {
        run_test_and_clean_up(|path| {
            let mut missing_path = path.clone();
            missing_path.push("missing/directory");
            let actual = discover_paths(missing_path);
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

    const FILE_DIR_1: &str = "folder_1";
    const FILE_DIR_2: &str = "folder_2";
    const FILE_NAME: &str = "test_";
    const FILE_COUNT: i32 = 10;

    fn generate_random_update(
        root_path: PathBuf,
        folder_name: Option<&str>,
        name: Option<&str>,
    ) -> i32 {

        let mut rng = rand::thread_rng();
        let mut rnd_num = rng.gen_range(1..=4);

        let mut folder_1 = root_path.clone();
        let mut folder_2 = root_path.clone();
        folder_1.push(FILE_DIR_1);
        folder_2.push(FILE_DIR_2);

        let mut cur_file_name = "";
        let mut cur_file_path = folder_1.clone();
        if let Some(file_name) = name {
            cur_file_name = file_name.clone();
            cur_file_path.push(file_name);
        }

        match rnd_num {
            // create
            1 => {
                let mut file_name_new = String::from(cur_file_name);
                file_name_new.push_str("_new.txt");
                create_file_at(
                    folder_1.clone(),
                    Some(DATA_SIZE_2),
                    Some(&file_name_new),
                );
            }
            // update
            2 => {
                let mut file = File::create(cur_file_path.as_path())
                    .expect("Unable to create file");
                modify_file(&mut file);
            }
            // delete
            3 => {
                std::fs::remove_file(cur_file_path.clone())
                    .expect("Should remove file successfully");
            }
            // move
            4 => {
                let mut name_to = folder_2.clone();
                name_to.push(cur_file_name);
                std::fs::rename(cur_file_path, name_to)
                    .expect("Should rename file successfully");
            }
            _ => println!("rnd_num error"),
        }

        return rnd_num;
    }

    fn init_tmp_directory(root_path: PathBuf) {
        let mut folder_1 = root_path.clone();
        let mut folder_2 = root_path.clone();
        folder_1.push(FILE_DIR_1);
        folder_2.push(FILE_DIR_2);

        std::fs::create_dir(&folder_1).expect("Could not create temp dir");
        std::fs::create_dir(&folder_2).expect("Could not create temp dir");

        for i in 1..FILE_COUNT {
            let data_size: u64 = (i + 10).try_into().unwrap();
            let mut create_file_name = String::from(FILE_NAME);
            create_file_name.push_str(&i.to_string());
            create_file_name.push_str(".txt");

            let (_, new_path) = create_file_at(
                folder_1.clone(),
                Some(data_size),
                Some(&create_file_name),
            );
        }
    }

    #[test]
    fn update_all_compare_track_one_methods() {
        run_test_and_clean_up(|path| {
            // Initial folders and files are created in this function.
            init_tmp_directory(path.clone());
            let initial_index = ResourceIndex::build(path.clone());
            let mut index1 = initial_index.clone();
            let mut index2 = initial_index.clone();

            for i in 1..FILE_COUNT {

                let mut file_name = String::from("test_");
                file_name.push_str(&i.to_string());
                file_name.push_str(".txt");

                let mut file_path = path.clone();
                let mut move_file_path = path.clone();
                file_path.push(FILE_DIR_1);
                file_path.push(file_name.clone());

                move_file_path.push(FILE_DIR_2);
                move_file_path.push(file_name.clone());

                let old_id: ResourceId;
                match CanonicalPathBuf::canonicalize(&file_path) {
                    Ok(canonicalized_path) => {
                        old_id = initial_index.path2id
                            [&canonicalized_path.clone()]
                            .id;
                    }
                    Err(_) => {
                        log::warn!(
                            "File {} not found",
                            file_path.to_str().unwrap_or("no_path_buf")
                        );
                        continue;
                    }
                }

                let update_state = generate_random_update(
                    path.clone(),
                    Some(FILE_DIR_1),
                    Some(&file_name),
                );

                match update_state {
                    // create
                    1 => {
                        index1.track_addition(&file_path);
                    }
                    // update
                    2 => {
                        index1.track_update(&file_path, old_id);
                    }
                    // delete
                    3 => {
                        index1.track_deletion(old_id);
                    }
                    //move
                    4 => {
                        index1.track_deletion(old_id);
                        index1.track_addition(&move_file_path);
                    }
                    _ => println!("rnd_num error"),
                }
            }

            index2
                .update_all()
                .expect("Should update index correctly");

             assert_eq!(index1, index2);
        })
    }

    #[test]
    fn update_all_compare_track_addition() 
    {
        run_test_and_clean_up(|path| {
            
            create_file_at(path.clone(), Some(DATA_SIZE_1), Some(FILE_NAME_1));
            
            let initial_index = ResourceIndex::build(path.clone());
            let mut index_track_addition = initial_index.clone();
            let mut index_update_all = initial_index.clone();
           
            create_file_at(path.clone(), Some(DATA_SIZE_1), Some(FILE_NAME_2));

            let mut added_file_path = path.clone();
            added_file_path.push(FILE_NAME_2);
            index_track_addition.track_addition(&added_file_path);

            index_update_all
                .update_all()
                .expect("Should update index correctly");
            
            assert_eq!(index_track_addition, index_update_all);
        })
    }
}
