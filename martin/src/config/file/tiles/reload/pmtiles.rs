//! PMTiles reloader for the [`TileSourceManager`](crate::config::file::reload::TileSourceManager).
//!
//! [`PMTilesReloader`] is the bridge between the filesystem and the TSM for
//! PMTiles tile sources.  Call [`load_file`](PMTilesReloader::load_file) to add
//! (or replace) a single source, or [`load_files`](PMTilesReloader::load_files)
//! for a batch.
//!
//! Only **local** file paths are supported; remote object-store URLs (S3, GCS,
//! Azure, …) cannot be watched by the filesystem watcher and must not be passed
//! to these functions.
//!
//! The [`start`](PMTilesReloader::start) method embeds a [`TileFileWatcher`]
//! and translates raw [`FileChange`] events into [`ReloadAdvisory`] values.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use martin_core::tiles::pmtiles::{PmtCache, PmtCacheInstance, PmtilesSource};
use url::Url;

use crate::MartinResult;
use crate::config::file::ConfigFileError;
use crate::config::primitives::IdResolver;
use crate::config::file::reload::{ReloadAdvisory, TileSourceManager};

/// Loads and reloads PMTiles tile sources.
pub struct PMTilesReloader;

/// Monotonically increasing counter used to assign unique cache IDs.
static NEXT_CACHE_ID: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

impl PMTilesReloader {
    /// Spawn a background task that watches `id_to_path` connections and
    /// `watched_dirs` for new `.pmtiles` files.
    ///
    /// Raw filesystem events are received from an embedded [`TileFileWatcher`]
    /// and translated into [`ReloadAdvisory`] values sent to `tx`.
    #[cfg(feature = "_file_watcher")]
    pub async fn start(
        idr: IdResolver,
        tx: tokio::sync::mpsc::Sender<ReloadAdvisory>,
        id_to_path: HashMap<String, PathBuf>,
        watched_dirs: Vec<PathBuf>,
    ) {
        use dashmap::DashMap;
        use tracing::{info, warn};

        use super::watcher::{FileChange, TileFileWatcher};

        let path_to_id: DashMap<PathBuf, String> = id_to_path
            .iter()
            .map(|(id, path)| {
                let canon = path.canonicalize().unwrap_or_else(|_| path.clone());
                (canon, id.clone())
            })
            .collect();

        let tracked_files: Vec<PathBuf> = path_to_id.iter().map(|e| e.key().clone()).collect();

        let (file_tx, mut file_rx) = tokio::sync::mpsc::channel::<FileChange>(256);
        TileFileWatcher::start(tracked_files, watched_dirs, file_tx).await;

        tokio::spawn(async move {
            while let Some(change) = file_rx.recv().await {
                match change {
                    FileChange::Modified(canon) => {
                        if !canon.extension().is_some_and(|e| e == "pmtiles") {
                            continue;
                        }
                        if let Some(id) = path_to_id.get(&canon).map(|r| r.clone()) {
                            info!("Reloading source `{id}` (file changed: {})", canon.display());
                            let advisory = match Self::reload_source(&id, canon).await {
                                Ok(a) => Some(a),
                                Err(e) => {
                                    warn!("Reload of `{id}` failed: {e}");
                                    None
                                }
                            };
                            if let Some(advisory) = advisory {
                                if tx.send(advisory).await.is_err() {
                                    warn!("Advisory channel closed; dropping reload advisory");
                                }
                            }
                        }
                    }
                    FileChange::Deleted(canon) => {
                        if let Some((_, id)) = path_to_id.remove(&canon) {
                            info!(
                                "Removing source `{id}` (file deleted: {})",
                                canon.display()
                            );
                            let advisory = ReloadAdvisory {
                                added: vec![],
                                changed: vec![],
                                removed: vec![id],
                            };
                            if tx.send(advisory).await.is_err() {
                                warn!("Advisory channel closed; dropping reload advisory");
                            }
                        }
                    }
                    FileChange::New(canon) => {
                        if !canon.extension().is_some_and(|e| e == "pmtiles") {
                            continue;
                        }
                        info!("Loading new source from {}", canon.display());
                        let result = match Self::load_file(&idr, canon.clone()).await {
                            Ok(a) => Some(a),
                            Err(e) => {
                                warn!("Failed to load {}: {e}", canon.display());
                                None
                            }
                        };
                        if let Some(advisory) = result {
                            let added_ids = advisory.added_ids();
                            if tx.send(advisory).await.is_err() {
                                warn!("Advisory channel closed; dropping reload advisory");
                            }
                            for id in added_ids {
                                path_to_id.insert(canon.clone(), id);
                            }
                        }
                    }
                }
            }
        });
    }

    /// Opens the PMTiles file at `path` and returns a [`ReloadAdvisory`] with
    /// the new source in [`ReloadAdvisory::added`].
    ///
    /// The source ID is derived from the file stem (e.g. `world` for
    /// `world.pmtiles`) and made unique / non-reserved via `idr`.
    pub async fn load_file(idr: &IdResolver, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let id = idr.resolve(&name, path.display().to_string());
        let source = Self::open_source(id, path).await?;
        Ok(ReloadAdvisory {
            added: vec![Box::new(source)],
            changed: vec![],
            removed: vec![],
        })
    }

    /// Loads multiple PMTiles files, applying each advisory to `tsm`, and
    /// returns all assigned source IDs.
    pub async fn load_files(
        tsm: &TileSourceManager,
        paths: Vec<PathBuf>,
    ) -> MartinResult<Vec<String>> {
        let idr = tsm.id_resolver();
        let mut ids = Vec::with_capacity(paths.len());
        for path in paths {
            let advisory = Self::load_file(&idr, path).await?;
            let new_ids = advisory.added_ids();
            tsm.apply_advisory(advisory);
            ids.extend(new_ids);
        }
        Ok(ids)
    }

    /// Re-opens the PMTiles file at `path` and returns a [`ReloadAdvisory`]
    /// with the refreshed source in [`ReloadAdvisory::changed`].
    pub async fn reload_source(id: &str, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let source = Self::open_source(id.to_string(), path).await?;
        Ok(ReloadAdvisory {
            added: vec![],
            changed: vec![Box::new(source)],
            removed: vec![],
        })
    }

    /// Creates a [`PmtilesSource`] from a local file path.
    async fn open_source(id: String, path: PathBuf) -> MartinResult<PmtilesSource> {
        let path = path
            .canonicalize()
            .map_err(|e| ConfigFileError::IoError(e, path.clone()))?;
        let path = std::path::absolute(&path)
            .map_err(|e| ConfigFileError::IoError(e, path.clone()))?;
        let url = Url::from_file_path(&path)
            .or(Err(ConfigFileError::PathNotConvertibleToUrl(path.clone())))?;

        let cache_id = NEXT_CACHE_ID.fetch_add(1, Ordering::SeqCst);
        let cache = PmtCacheInstance::new(cache_id, PmtCache::default());

        let (store, store_path) =
            object_store::parse_url_opts(&url, &HashMap::<String, String>::new())
                .map_err(|e| ConfigFileError::ObjectStoreUrlParsing(e, id.clone()))?;

        let source = PmtilesSource::new(cache, id, store, store_path).await?;
        Ok(source)
    }
}

