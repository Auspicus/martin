//! `MBTiles` reloader for the [`TileSourceManager`](super::TileSourceManager).
//!
//! [`MBTilesReloader`] is the bridge between the filesystem and the TSM for
//! MBTiles tile sources. Call [`load_file`](MBTilesReloader::load_file) to add
//! (or replace) a single source, or [`load_files`](MBTilesReloader::load_files)
//! for a batch.

use std::path::{Path, PathBuf};

use martin_core::tiles::mbtiles::MbtSource;

use super::{FileSourceLoader, TileSourceManager};
use crate::MartinResult;

/// Loads and reloads MBTiles tile sources into a [`TileSourceManager`].
pub struct MBTilesReloader;

#[async_trait::async_trait]
impl FileSourceLoader for MBTilesReloader {
    fn can_handle(&self, path: &Path) -> bool {
        path.extension().is_some_and(|e| e == "mbtiles")
    }

    async fn load_file(&self, tsm: &TileSourceManager, path: PathBuf) -> MartinResult<String> {
        Self::load_file(tsm, path).await
    }

    async fn reload_source(
        &self,
        tsm: &TileSourceManager,
        id: &str,
        path: PathBuf,
    ) -> MartinResult<()> {
        Self::reload_source(tsm, id, path).await
    }
}

impl MBTilesReloader {
    /// Opens the MBTiles file at `path`, registers it with `tsm`, and returns
    /// the assigned source ID.
    ///
    /// The source ID is derived from the file stem (e.g. `world_cities` for
    /// `world_cities.mbtiles`) and made unique / non-reserved via the TSM's
    /// [`IdResolver`](crate::config::primitives::IdResolver).
    ///
    /// If a source with the same ID already exists it is replaced in-place and
    /// its tile-cache entries are invalidated.
    pub async fn load_file(tsm: &TileSourceManager, path: PathBuf) -> MartinResult<String> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        // Use the full display path as the unique discriminator so that two
        // files with the same stem get distinct IDs.
        let id = tsm.resolve_id(&name, path.display().to_string());
        let source = MbtSource::new(id.clone(), path).await?;
        tsm.upsert_source(Box::new(source));
        Ok(id)
    }

    /// Loads multiple MBTiles files in order, returning all assigned IDs.
    pub async fn load_files(
        tsm: &TileSourceManager,
        paths: Vec<PathBuf>,
    ) -> MartinResult<Vec<String>> {
        let mut ids = Vec::with_capacity(paths.len());
        for path in paths {
            ids.push(Self::load_file(tsm, path).await?);
        }
        Ok(ids)
    }

    /// Re-opens the MBTiles file at `path` and replaces the source currently
    /// registered under `id`.
    ///
    /// This is the hot-reload path: the caller supplies the stable `id` that was
    /// returned by a previous [`load_file`](Self::load_file) call so the same
    /// URL remains valid after the reload.
    pub async fn reload_source(
        tsm: &TileSourceManager,
        id: &str,
        path: PathBuf,
    ) -> MartinResult<()> {
        let source = MbtSource::new(id.to_string(), path).await?;
        tsm.upsert_source(Box::new(source));
        Ok(())
    }
}
