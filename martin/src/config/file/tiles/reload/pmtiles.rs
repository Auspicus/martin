//! PMTiles reloader for the [`TileSourceManager`](crate::config::file::reload::TileSourceManager).
//!
//! [`PMTilesReloader`] is the bridge between the filesystem and the TSM for
//! PMTiles tile sources.  Call [`load_file`](PMTilesReloader::load_file) to add
//! a single source, or [`load_files`](PMTilesReloader::load_files) for a batch.
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
use crate::config::file::reload::ReloadAdvisory;
use crate::config::primitives::IdResolver;

use notify::event::AccessKind;
use notify::{Config, Event, EventKind, RecommendedWatcher, Watcher};
use tracing::{info, warn};

/// Loads and reloads PMTiles tile sources.
pub struct PMTilesReloader;

/// Monotonically increasing counter used to assign unique cache IDs.
static NEXT_CACHE_ID: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

impl PMTilesReloader {
    /// Spawn a background task that watches `watched_dirs` for new `.pmtiles`
    /// files and for deletions of previously-discovered files.
    ///
    /// Raw filesystem events are received from an embedded [`TileFileWatcher`]
    /// and translated into [`ReloadAdvisory`] values sent to `tx`.
    ///
    /// Only **directory connections** are watched.  Individually configured
    /// file sources are static for the lifetime of the process.
    #[cfg(feature = "_file_watcher")]
    pub fn start(
        idr: IdResolver,
        tx: tokio::sync::mpsc::Sender<ReloadAdvisory>,
        watched_dirs: Vec<PathBuf>,
    ) {
        let (fs_tx, mut fs_rx) = tokio::sync::mpsc::channel::<Event>(256);

        let mut watcher = match RecommendedWatcher::new(
            move |result: notify::Result<Event>| {
                if let Ok(event) = result {
                    let _ = fs_tx.blocking_send(event);
                }
            },
            Config::default(),
        ) {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to initialise filesystem watcher: {e}");
                return;
            }
        };

        for dir in &watched_dirs {
            if let Err(e) = watcher.watch(dir, notify::RecursiveMode::NonRecursive) {
                warn!("Failed to watch {}: {e}", dir.display());
            }
        }

        tokio::spawn(async move {
            let _watcher = watcher;
            while let Some(e) = fs_rx.recv().await {
                for canon in e
                    .paths
                    .iter()
                    .map(|p| p.canonicalize().unwrap_or_else(|_| p.clone()))
                {
                    use notify::event::AccessMode;

                    if !canon.extension().is_some_and(|e| e == "pmtiles") {
                        continue;
                    }

                    match e.kind {
                        EventKind::Remove(_) => {
                            let Some(name) = canon.file_stem().and_then(|s| s.to_str()) else {
                                continue;
                            };
                            let id = idr.resolve(name, canon.display().to_string());
                            info!("Removing source `{id}` (file deleted: {})", canon.display());
                            let advisory = ReloadAdvisory {
                                removed: vec![id],
                                ..Default::default()
                            };
                            if tx.send(advisory).await.is_err() {
                                warn!("Advisory channel closed; dropping reload advisory");
                            }
                        }
                        EventKind::Create(_) | EventKind::Access(AccessKind::Close(AccessMode::Write)) => {
                            info!("Loading new source from {}", canon.display());
                            let result = match Self::load_file(&idr, canon.clone()).await {
                                Ok(a) => Some(a),
                                Err(e) => {
                                    warn!("Failed to load {}: {e}", canon.display());
                                    None
                                }
                            };
                            if let Some(advisory) = result {
                                if tx.send(advisory).await.is_err() {
                                    warn!("Advisory channel closed; dropping reload advisory");
                                }
                            }
                        }
                        _ => {}
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
            .ok_or_else(|| ConfigFileError::NoFileStem(path.clone()))?
            .to_string();
        let id = idr.resolve(&name, path.display().to_string());
        let path = path
            .canonicalize()
            .map_err(|e| ConfigFileError::IoError(e, path.clone()))?;
        let path =
            std::path::absolute(&path).map_err(|e| ConfigFileError::IoError(e, path.clone()))?;
        let url = Url::from_file_path(&path)
            .or(Err(ConfigFileError::PathNotConvertibleToUrl(path.clone())))?;

        let cache_id = NEXT_CACHE_ID.fetch_add(1, Ordering::SeqCst);
        let cache = PmtCacheInstance::new(cache_id, PmtCache::default());

        let (store, store_path) =
            object_store::parse_url_opts(&url, &HashMap::<String, String>::new())
                .map_err(|e| ConfigFileError::ObjectStoreUrlParsing(e, id.clone()))?;

        let source = PmtilesSource::new(cache, id, store, store_path).await?;

        Ok(ReloadAdvisory {
            added: vec![Box::new(source)],
            ..Default::default()
        })
    }
}
