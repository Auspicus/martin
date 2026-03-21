//! Dynamic tile source reload architecture.
//!
//! The [`TileSourceManager`] (TSM) is the single point of serialization for
//! catalog updates. Specialized reloaders (one per source type) discover
//! changes and push them here; the TSM applies each change atomically so
//! clients never observe a partial or inconsistent catalog.
//!
//! ```text
//! Main Process
//! ├── PostgresReloader  ──┐
//! ├── MBTilesReloader   ──┤──► TileSourceManager ──► Public Catalog
//! ├── PMTilesReloader   ──┤
//! └── COGReloader       ──┘
//! ```

use std::sync::Arc;

use actix_web::error::ErrorNotFound;
use dashmap::DashMap;
use martin_core::tiles::catalog::TileCatalog;
use martin_core::tiles::{BoxedSource, OptTileCache};
use martin_tile_utils::TileInfo;
use tracing::debug;

use crate::config::primitives::IdResolver;
use crate::srv::RESERVED_KEYWORDS;

#[cfg(feature = "mbtiles")]
pub mod mbtiles;

#[cfg(feature = "_file_watcher")]
pub mod watcher;
#[cfg(feature = "_file_watcher")]
pub use watcher::WatchPaths;

#[cfg(all(feature = "pmtiles", feature = "_file_watcher"))]
pub mod pmtiles;

#[cfg(all(feature = "unstable-cog", feature = "_file_watcher"))]
pub mod cog;

#[cfg(feature = "postgres")]
pub mod postgres;

/// Trait implemented by each file-based tile source format to plug into [`TileFileWatcher`](watcher::TileFileWatcher).
///
/// A loader encapsulates format-specific knowledge (e.g. which file extension
/// it handles) so the generic watcher stays free of any per-format details.
#[cfg(feature = "_file_watcher")]
#[async_trait::async_trait]
pub trait FileSourceLoader: Send + Sync {
    /// Returns `true` if this loader can handle the given file path.
    fn can_handle(&self, path: &std::path::Path) -> bool;

    /// Open the file at `path`, register it in `tsm`, and return the assigned source ID.
    async fn load_file(
        &self,
        tsm: &TileSourceManager,
        path: std::path::PathBuf,
    ) -> crate::MartinResult<String>;

    /// Re-open the file at `path` and replace the source registered under `id`.
    async fn reload_source(
        &self,
        tsm: &TileSourceManager,
        id: &str,
        path: std::path::PathBuf,
    ) -> crate::MartinResult<()>;
}

/// Central coordinator for live tile-source catalog updates.
///
/// All mutable access to the source registry goes through this struct.
/// Individual reloaders hold an `Arc`-clone and push updates; callers on the
/// read path take a snapshot via [`get_catalog`](Self::get_catalog) or
/// retrieve individual sources via [`get_source`](Self::get_source).
#[derive(Clone)]
pub struct TileSourceManager {
    sources: Arc<DashMap<String, BoxedSource>>,
    cache: OptTileCache,
    id_resolver: IdResolver,
}

impl TileSourceManager {
    /// Creates a new, empty `TileSourceManager`.
    ///
    /// Pass [`martin_core::tiles::NO_TILE_CACHE`] if tile caching is not needed.
    #[must_use]
    pub fn new(cache: OptTileCache) -> Self {
        Self {
            sources: Arc::new(DashMap::new()),
            cache,
            id_resolver: IdResolver::new(RESERVED_KEYWORDS),
        }
    }

    /// Creates a `TileSourceManager` pre-populated from an existing source collection.
    ///
    /// Used at server startup to seed the TSM from the sources that were
    /// resolved from the config file. Sources are inserted with their
    /// already-assigned IDs; no ID resolution is performed.
    #[must_use]
    pub fn from_sources(sources: Vec<BoxedSource>, cache: OptTileCache) -> Self {
        let tsm = Self::new(cache);
        for source in sources {
            tsm.upsert_source(source);
        }
        tsm
    }

    /// Resolves a stable, unique, non-reserved ID for a new source.
    ///
    /// Delegates to the internal [`IdResolver`]; see its docs for the
    /// disambiguation rules.
    #[must_use]
    pub fn resolve_id(&self, name: &str, unique_name: String) -> String {
        self.id_resolver.resolve(name, unique_name)
    }

    /// Inserts or replaces a source in the registry.
    ///
    /// When a source with the same ID already exists it is replaced and the
    /// corresponding tile-cache entries are invalidated so stale tiles are
    /// never served from the old source.
    pub fn upsert_source(&self, source: BoxedSource) {
        let id = source.get_id().to_string();
        let old = self.sources.insert(id.clone(), source);
        if old.is_some() {
            if let Some(cache) = &self.cache {
                cache.invalidate_source(&id);
            }
        }
    }

    /// Removes a source from the registry and invalidates its cached tiles.
    ///
    /// Returns `true` if the source existed and was removed.
    pub fn remove_source(&self, id: &str) -> bool {
        if self.sources.remove(id).is_some() {
            if let Some(cache) = &self.cache {
                cache.invalidate_source(id);
            }
            true
        } else {
            false
        }
    }

    /// Returns a cloned copy of the source with the given ID, if present.
    #[must_use]
    pub fn get_source(&self, id: &str) -> Option<BoxedSource> {
        self.sources.get(id).map(|r| r.clone_source())
    }

    /// Returns all source IDs currently in the registry.
    #[must_use]
    pub fn source_ids(&self) -> Vec<String> {
        self.sources.iter().map(|r| r.key().clone()).collect()
    }

    /// Builds a [`TileCatalog`] snapshot from the current registry contents.
    #[must_use]
    pub fn get_catalog(&self) -> TileCatalog {
        self.sources
            .iter()
            .map(|r| (r.key().clone(), r.get_catalog_entry()))
            .collect()
    }

    /// Looks up a single source by ID, returning a 404 actix error if absent.
    pub fn get_source_actix(&self, id: &str) -> actix_web::Result<BoxedSource> {
        self.get_source(id)
            .ok_or_else(|| ErrorNotFound(format!("Source {id} does not exist")))
    }

    /// Resolves one or more comma-separated source IDs for a tile request.
    ///
    /// Validates that all requested sources share the same [`TileInfo`] (format
    /// + encoding) — mixing formats in a composite tile is not supported.
    /// Sources outside the requested zoom level are silently dropped.
    ///
    /// Returns `(sources, supports_url_query, merged_tile_info)`.
    pub fn get_sources(
        &self,
        source_ids: &str,
        zoom: Option<u8>,
    ) -> actix_web::Result<(Vec<BoxedSource>, bool, TileInfo)> {
        let mut sources = Vec::new();
        let mut info: Option<TileInfo> = None;
        let mut use_url_query = false;

        for id in source_ids.split(',') {
            let src = self.get_source_actix(id)?;
            let src_inf = src.get_tile_info();
            use_url_query |= src.support_url_query();

            match info {
                Some(inf) if inf == src_inf => {}
                Some(inf) => Err(ErrorNotFound(format!(
                    "Cannot merge sources with {inf} with {src_inf}"
                )))?,
                None => info = Some(src_inf),
            }

            if match zoom {
                Some(zoom) => {
                    let valid = src.is_valid_zoom(zoom);
                    if !valid {
                        debug!("Zoom {zoom} is not valid for source {id}");
                    }
                    valid
                }
                None => true,
            } {
                sources.push(src);
            }
        }

        Ok((sources, use_url_query, info.unwrap()))
    }
}
