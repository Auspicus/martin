//! Cloud Optimized GeoTIFF reloader for the [`TileSourceManager`](super::TileSourceManager).
//!
//! [`COGReloader`] is the bridge between the filesystem and the TSM for COG
//! tile sources. Call [`load_file`](COGReloader::load_file) to add (or replace)
//! a single source, or [`load_files`](COGReloader::load_files) for a batch.

use std::path::{Path, PathBuf};

use martin_core::tiles::cog::CogSource;

use super::{ReloadAdvisory, TileSourceManager, TileSourceWatcher};
use crate::MartinResult;
use crate::config::primitives::IdResolver;

/// Loads and reloads COG tile sources into a [`TileSourceManager`].
pub struct COGReloader;

#[async_trait::async_trait]
impl TileSourceWatcher for COGReloader {
    fn can_handle(&self, path: &Path) -> bool {
        path.extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("tif") || e.eq_ignore_ascii_case("tiff"))
    }

    async fn load_file(&self, idr: &IdResolver, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        Self::load_file(idr, path).await
    }

    async fn reload_source(&self, id: &str, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        Self::reload_source(id, path).await
    }
}

impl COGReloader {
    /// Opens the COG file at `path` and returns a [`ReloadAdvisory`] with the
    /// new source in [`ReloadAdvisory::added`].
    ///
    /// The source ID is derived from the file stem (e.g. `naip` for
    /// `naip.tif`) and made unique / non-reserved via `idr`.
    pub async fn load_file(idr: &IdResolver, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let id = idr.resolve(&name, path.display().to_string());
        let source = CogSource::new(id, path)?;
        Ok(ReloadAdvisory {
            added: vec![Box::new(source)],
            changed: vec![],
            removed: vec![],
        })
    }

    /// Loads multiple COG files, applying each advisory to `tsm`, and
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

    /// Re-opens the COG file at `path` and returns a [`ReloadAdvisory`] with
    /// the refreshed source in [`ReloadAdvisory::changed`].
    ///
    /// The caller supplies the stable `id` so the same URL remains valid after
    /// the reload.
    pub async fn reload_source(id: &str, path: PathBuf) -> MartinResult<ReloadAdvisory> {
        let source = CogSource::new(id.to_string(), path)?;
        Ok(ReloadAdvisory {
            added: vec![],
            changed: vec![Box::new(source)],
            removed: vec![],
        })
    }
}
