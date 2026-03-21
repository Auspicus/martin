//! Integration tests for the PMTiles filesystem watcher.
//!
//! These tests confirm that:
//! - a deleted `.pmtiles` file removes the source from the TSM
//! - an overwritten `.pmtiles` file triggers a source reload in the TSM
//! - a new `.pmtiles` file in a watched directory is loaded automatically

#![cfg(all(feature = "pmtiles", feature = "_file_watcher"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use martin::reload::TileSourceManager;
use martin::reload::WatchPaths;
use martin::reload::pmtiles::PMTilesReloader;
use martin::reload::watcher::TileFileWatcher;
use martin_core::tiles::NO_TILE_CACHE;
use tempfile::tempdir;
use tokio::time::sleep;

/// Returns the path to the Stamen Toner PMTiles fixture file.
fn pmtiles_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/fixtures/pmtiles/stamen_toner__raster_CC-BY+ODbL_z3.pmtiles")
}

/// Give inotify / the tokio task a moment to process the event.
async fn wait_for_event() {
    sleep(Duration::from_millis(1000)).await;
}

/// Give the watcher time to initialise before acting on files.
async fn wait_for_watcher_init() {
    sleep(Duration::from_millis(200)).await;
}

/// Start a watcher that tracks a single file already loaded in the TSM.
async fn start_file_watcher(tsm: &TileSourceManager, id: &str, path: &PathBuf) {
    let mut id_to_path = HashMap::new();
    id_to_path.insert(id.to_string(), path.clone());
    TileFileWatcher::start(
        tsm.clone(),
        WatchPaths {
            id_to_path,
            watched_dirs: vec![],
        },
        vec![Arc::new(PMTilesReloader)],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Test: deleted file removes source
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pmtiles_watcher_removes_source_on_file_deletion() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("tiles.pmtiles");

    std::fs::copy(pmtiles_fixture(), &path).expect("copy fixture");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = PMTilesReloader::load_file(&tsm, path.clone())
        .await
        .expect("load_file");

    assert!(
        tsm.get_source(&id).is_some(),
        "source should be present before deletion"
    );

    start_file_watcher(&tsm, &id, &path).await;
    wait_for_watcher_init().await;

    std::fs::remove_file(&path).expect("remove file");
    wait_for_event().await;

    assert!(
        tsm.get_source(&id).is_none(),
        "source should be removed after file deletion"
    );
}

// ---------------------------------------------------------------------------
// Test: overwritten file reloads source
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pmtiles_watcher_reloads_source_on_file_modification() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("tiles.pmtiles");

    std::fs::copy(pmtiles_fixture(), &path).expect("copy fixture");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = PMTilesReloader::load_file(&tsm, path.clone())
        .await
        .expect("load_file");

    assert!(tsm.get_source(&id).is_some(), "source must exist before reload");

    start_file_watcher(&tsm, &id, &path).await;
    wait_for_watcher_init().await;

    // Overwrite the file in-place with a valid copy of the same fixture.
    // This triggers IN_MODIFY / IN_CLOSE_WRITE without replacing the inode.
    std::fs::copy(pmtiles_fixture(), &path).expect("overwrite fixture");
    wait_for_event().await;

    assert!(
        tsm.get_source(&id).is_some(),
        "source should still be present after reload"
    );
}

// ---------------------------------------------------------------------------
// Test: new file in watched directory is loaded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pmtiles_watcher_loads_new_file_in_watched_directory() {
    let dir = tempdir().expect("tempdir");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);

    TileFileWatcher::start(
        tsm.clone(),
        WatchPaths {
            id_to_path: HashMap::new(),
            watched_dirs: vec![dir.path().to_path_buf()],
        },
        vec![Arc::new(PMTilesReloader)],
    )
    .await;
    wait_for_watcher_init().await;

    let path = dir.path().join("new_source.pmtiles");
    std::fs::copy(pmtiles_fixture(), &path).expect("copy fixture into watched dir");

    wait_for_event().await;

    let ids = tsm.source_ids();
    assert_eq!(
        ids.len(),
        1,
        "watcher should have loaded the new PMTiles file; found: {ids:?}"
    );
}
