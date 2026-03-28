//! `MBTiles` reloader for the [`TileSourceManager`](super::TileSourceManager).
//!
//! [`MBTilesReloader`] is the bridge between the filesystem and the TSM for
//! MBTiles tile sources. Call [`load_file`](MBTilesReloader::load_file) to add
//! (or replace) a single source, or [`load_files`](MBTilesReloader::load_files)
//! for a batch.

#[cfg(feature = "_file_watcher")]
use std::path::Path;
use std::path::PathBuf;

use martin_core::tiles::mbtiles::MbtSource;

#[cfg(feature = "_file_watcher")]
use super::TileSourceWatcher;
use super::{ReloadAdvisory, TileSourceManager};
use crate::MartinResult;
use crate::config::primitives::IdResolver;

/// Loads and reloads MBTiles tile sources into a [`TileSourceManager`].
pub struct MBTilesReloader;

#[cfg(feature = "_file_watcher")]
#[async_trait::async_trait]
impl TileSourceWatcher for MBTilesReloader {
    fn can_handle(&self, path: &Path) -> bool {
        path.extension().is_some_and(|e| e == "mbtiles")
    }

    async fn load_file(&self, idr: &IdResolver, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        Self::load_file(idr, path).await
    }

    async fn reload_source(&self, id: &str, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        Self::reload_source(id, path).await
    }
}

impl MBTilesReloader {
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
        // Use the full display path as the unique discriminator so that two
        // files with the same stem get distinct IDs.
        let id = idr.resolve(&name, path.display().to_string());
        let source = MbtSource::new(id, path).await?;
        Ok(ReloadAdvisory {
            added: vec![Box::new(source)],
            changed: vec![],
            removed: vec![],
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

    /// Re-opens the MBTiles file at `path` and returns a [`ReloadAdvisory`]
    /// with the refreshed source in [`ReloadAdvisory::changed`].
    ///
    /// The caller supplies the stable `id` so the same URL remains valid after
    /// the reload.
    pub async fn reload_source(id: &str, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let source = MbtSource::new(id.to_string(), path).await?;
        Ok(ReloadAdvisory {
            added: vec![],
            changed: vec![Box::new(source)],
            removed: vec![],
        })
    }
}
