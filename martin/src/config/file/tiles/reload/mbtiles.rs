//! MBTiles reloader for the [`TileSourceManager`](crate::config::file::reload::TileSourceManager).
//!
//! [`MBTilesReloader`] is the bridge between the filesystem and the TSM for
//! MBTiles tile sources.  Call [`load_file`](MBTilesReloader::load_file) to add
//! a single source, or [`load_files`](MBTilesReloader::load_files) for a batch.
//!
//! The [`start`](MBTilesReloader::start) method embeds a [`TileFileWatcher`]
//! and translates raw [`FileChange`] events into [`ReloadAdvisory`] values that
//! are forwarded to the shared advisory channel.

use std::path::PathBuf;

use martin_core::tiles::mbtiles::MbtSource;

use crate::MartinResult;
use crate::config::primitives::IdResolver;
use crate::config::file::reload::{ReloadAdvisory, TileSourceManager};

/// Loads and reloads MBTiles tile sources.
pub struct MBTilesReloader;

impl MBTilesReloader {
    /// Spawn a background task that watches `watched_dirs` for new `.mbtiles`
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
        use dashmap::DashMap;
        use tracing::{info, warn};

        use super::watcher::{FileChange, TileFileWatcher};

        // Maps canonical file path → source ID for files discovered at runtime.
        // Starts empty; populated by New events, cleaned up by Deleted events.
        let path_to_id: DashMap<PathBuf, String> = DashMap::new();

        let (file_tx, mut file_rx) = tokio::sync::mpsc::channel::<FileChange>(256);
        TileFileWatcher::start(watched_dirs, file_tx);

        tokio::spawn(async move {
            while let Some(change) = file_rx.recv().await {
                match change {
                    FileChange::Deleted(canon) => {
                        if let Some((_, id)) = path_to_id.remove(&canon) {
                            info!(
                                "Removing source `{id}` (file deleted: {})",
                                canon.display()
                            );
                            let advisory = ReloadAdvisory {
                                removed: vec![id],
                                ..Default::default()
                            };
                            if tx.send(advisory).await.is_err() {
                                warn!("Advisory channel closed; dropping reload advisory");
                            }
                        }
                    }
                    FileChange::New(canon) => {
                        if !canon.extension().is_some_and(|e| e == "mbtiles") {
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
                            if let Some(id) = added_ids.into_iter().next() {
                                path_to_id.insert(canon, id);
                            }
                        }
                    }
                }
            }
        });
    }

    /// Opens the MBTiles file at `path` and returns a [`ReloadAdvisory`] with
    /// the new source in [`ReloadAdvisory::added`].
    ///
    /// The source ID is derived from the file stem (e.g. `world_cities` for
    /// `world_cities.mbtiles`) and made unique / non-reserved via `idr`.
    pub async fn load_file(idr: &IdResolver, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let id = idr.resolve(&name, path.display().to_string());
        let source = MbtSource::new(id, path).await?;
        Ok(ReloadAdvisory {
            added: vec![Box::new(source)],
            ..Default::default()
        })
    }

    /// Loads multiple MBTiles files, applying each advisory to `tsm`, and
    /// returns all assigned source IDs.
    ///
    /// This is a convenience helper for batch loading (e.g. in tests).  It
    /// applies advisories directly to the TSM rather than going through the
    /// advisory channel.
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
}
