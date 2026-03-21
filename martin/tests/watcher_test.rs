//! Integration tests for the MBTiles filesystem watcher.
//!
//! These tests confirm that:
//! - a modified `.mbtiles` file triggers a source reload in the TSM
//! - a deleted `.mbtiles` file removes the source from the TSM
//! - a new `.mbtiles` file in a watched directory is loaded automatically

#![cfg(all(feature = "mbtiles", feature = "_file_watcher"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use martin::reload::WatchPaths;
use martin::reload::TileSourceManager;
use martin::reload::mbtiles::MBTilesReloader;
use martin::reload::watcher::TileFileWatcher;
use martin_core::tiles::NO_TILE_CACHE;
use mbtiles::sqlx::{self, Connection as _};
use mbtiles::Mbtiles;
use tempfile::tempdir;
use tokio::time::sleep;

/// SQL that populates a minimal MVT mbtiles suitable for loading.
const MVT_SQL: &str = include_str!("../../tests/fixtures/mbtiles/world_cities.sql");


/// Create a real on-disk mbtiles file from a SQL script.
async fn create_mbtiles_file(path: &PathBuf, sql: &str) {
    let mbt = Mbtiles::new(path).expect("create Mbtiles handle");
    let mut conn = mbt.open_or_new().await.expect("open_or_new");
    sqlx::raw_sql(sql).execute(&mut conn).await.expect("execute sql");
    conn.close().await.expect("close connection");
    // Connection dropped here — SQLite flushes WAL to the main file.
}

/// Modify an existing mbtiles file in-place by appending a metadata row.
///
/// This changes the file bytes (triggering an inotify Modify event) without
/// removing or replacing the inode (which would generate a Remove event instead).
async fn touch_mbtiles_file(path: &PathBuf) {
    let mbt = Mbtiles::new(path).expect("create Mbtiles handle");
    let mut conn = mbt.open().await.expect("open for writing");
    sqlx::query("INSERT OR REPLACE INTO metadata VALUES ('_watcher_test', 'updated')")
        .execute(&mut conn)
        .await
        .expect("insert metadata");
    conn.close().await.expect("close");
}

/// Give inotify / the tokio task a moment to process the event.
async fn wait_for_event() {
    sleep(Duration::from_millis(1000)).await;
}

/// Give the watcher time to initialise before acting on files.
async fn wait_for_watcher_init() {
    sleep(Duration::from_millis(200)).await;
}

// ---------------------------------------------------------------------------
// Helper: start watcher for a single tracked file
// ---------------------------------------------------------------------------

async fn start_file_watcher(
    tsm: &TileSourceManager,
    id: &str,
    path: &PathBuf,
) {
    let mut id_to_path = HashMap::new();
    id_to_path.insert(id.to_string(), path.clone());
    TileFileWatcher::start(
        tsm.clone(),
        WatchPaths {
            id_to_path,
            watched_dirs: vec![],
        },
        vec![Arc::new(MBTilesReloader)],
    )
    .await;
}

// ---------------------------------------------------------------------------
// Test: deleted file removes source
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watcher_removes_source_on_file_deletion() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("cities.mbtiles");

    // Create a real file on disk and load it.
    create_mbtiles_file(&path, MVT_SQL).await;
    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path.clone())
        .await
        .expect("load_file");

    // Sanity: source is present.
    assert!(tsm.get_source(&id).is_some(), "source should be present before deletion");

    // Start the watcher and wait for it to initialise.
    start_file_watcher(&tsm, &id, &path).await;
    wait_for_watcher_init().await;

    // Delete the file.
    std::fs::remove_file(&path).expect("remove file");

    // Wait for the watcher to process the Remove event.
    wait_for_event().await;

    assert!(
        tsm.get_source(&id).is_none(),
        "source should be removed after file deletion"
    );
}

// ---------------------------------------------------------------------------
// Test: modified file reloads source
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watcher_reloads_source_on_file_modification() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("cities.mbtiles");

    // Create initial file.
    create_mbtiles_file(&path, MVT_SQL).await;
    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path.clone())
        .await
        .expect("load_file");

    assert!(tsm.get_source(&id).is_some(), "source must exist before reload");

    // Start the watcher and let it initialise.
    start_file_watcher(&tsm, &id, &path).await;
    wait_for_watcher_init().await;

    // Modify the file in-place (triggers a Modify event, not Remove).
    touch_mbtiles_file(&path).await;
    wait_for_event().await;

    // The source should still be registered (reload does not remove it).
    assert!(
        tsm.get_source(&id).is_some(),
        "source should still be present after reload"
    );
}

// ---------------------------------------------------------------------------
// Test: new file in watched directory is loaded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watcher_loads_new_file_in_watched_directory() {
    let dir = tempdir().expect("tempdir");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);

    // Start the watcher with an empty id_to_path but watching the directory.
    TileFileWatcher::start(
        tsm.clone(),
        WatchPaths {
            id_to_path: HashMap::new(),
            watched_dirs: vec![dir.path().to_path_buf()],
        },
        vec![Arc::new(MBTilesReloader)],
    )
    .await;
    wait_for_watcher_init().await;

    // Create a new mbtiles file inside the watched directory.
    let path = dir.path().join("new_source.mbtiles");
    create_mbtiles_file(&path, MVT_SQL).await;

    wait_for_event().await;

    // TSM should now have exactly one source.
    let ids = tsm.source_ids();
    assert_eq!(
        ids.len(),
        1,
        "watcher should have loaded the new file; found: {ids:?}"
    );
}
