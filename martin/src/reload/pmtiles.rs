//! `PMTiles` reloader for the [`TileSourceManager`](super::TileSourceManager).
//!
//! [`PMTilesReloader`] is the bridge between the filesystem and the TSM for
//! PMTiles tile sources. Call [`load_file`](PMTilesReloader::load_file) to add
//! (or replace) a single source, or [`load_files`](PMTilesReloader::load_files)
//! for a batch.
//!
//! Only **local** file paths are supported; remote object-store URLs (S3, GCS,
//! Azure, …) cannot be watched by the filesystem watcher and must not be passed
//! to these functions.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use martin_core::tiles::pmtiles::{PmtCache, PmtCacheInstance, PmtilesSource};
use url::Url;

use super::{ReloadAdvisory, TileSourceManager, TileSourceWatcher};
use crate::MartinResult;
use crate::config::file::ConfigFileError;

/// Loads and reloads PMTiles tile sources into a [`TileSourceManager`].
pub struct PMTilesReloader;

/// Monotonically increasing counter used to assign unique cache IDs.
///
/// Each `PmtCacheInstance` must have a distinct ID so that independent sources
/// do not share their directory caches.
static NEXT_CACHE_ID: LazyLock<AtomicUsize> = LazyLock::new(|| AtomicUsize::new(0));

#[async_trait::async_trait]
impl TileSourceWatcher for PMTilesReloader {
    fn can_handle(&self, path: &Path) -> bool {
        path.extension().is_some_and(|e| e == "pmtiles")
    }

    async fn load_file(&self, tsm: &TileSourceManager, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        Self::load_file(tsm, path).await
    }

    async fn reload_source(
        &self,
        tsm: &TileSourceManager,
        id: &str,
        path: PathBuf,
    ) -> MartinResult<ReloadAdvisory> {
        Self::reload_source(tsm, id, path).await
    }
}

impl PMTilesReloader {
    /// Opens the PMTiles file at `path`, registers it with `tsm`, and returns
    /// the assigned source ID.
    ///
    /// The source ID is derived from the file stem (e.g. `world` for
    /// `world.pmtiles`) and made unique / non-reserved via the TSM's
    /// [`IdResolver`](crate::config::primitives::IdResolver).
    ///
    /// If a source with the same ID already exists it is replaced in-place and
    /// its tile-cache entries are invalidated.
    pub async fn load_file(tsm: &TileSourceManager, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let id = tsm.resolve_id(&name, path.display().to_string());
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
        let mut ids = Vec::with_capacity(paths.len());
        for path in paths {
            let advisory = Self::load_file(tsm, path).await?;
            let new_ids = advisory.added_ids();
            tsm.apply_advisory(advisory);
            ids.extend(new_ids);
        }
        Ok(ids)
    }

    /// Re-opens the PMTiles file at `path` and replaces the source currently
    /// registered under `id`.
    ///
    /// This is the hot-reload path: the caller supplies the stable `id` that was
    /// returned by a previous [`load_file`](Self::load_file) call so the same
    /// URL remains valid after the reload.
    pub async fn reload_source(
        _tsm: &TileSourceManager,
        id: &str,
        path: PathBuf,
    ) -> MartinResult<ReloadAdvisory> {
        let source = Self::open_source(id.to_string(), path).await?;
        Ok(ReloadAdvisory {
            added: vec![],
            changed: vec![Box::new(source)],
            removed: vec![],
        })
    }

    /// Creates a [`PmtilesSource`] from a local file path.
    ///
    /// Converts the path to a `file://` URL and delegates to
    /// [`PmtilesSource::new`] via `object_store`, mirroring the logic in
    /// [`PmtConfig::new_sources`](crate::config::file::tiles::pmtiles::PmtConfig).
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
