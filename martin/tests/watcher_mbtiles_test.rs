//! Integration tests for the MBTiles filesystem watcher.
//!
//! These tests confirm that:
//! - a new `.mbtiles` file in a watched directory is loaded automatically
//! - a `.mbtiles` file that was discovered via the watcher is removed when deleted

#![cfg(all(feature = "mbtiles", feature = "_file_watcher"))]

use std::path::PathBuf;
use std::time::Duration;

use martin::config::file::reload::ReloadAdvisory;
use martin::config::file::reload::TileSourceManager;
use martin::config::file::tiles::reload::mbtiles::MBTilesReloader;
use martin_core::tiles::NO_TILE_CACHE;
use mbtiles::Mbtiles;
use mbtiles::sqlx::{self, Connection as _};
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::sleep;

/// SQL that populates a minimal MVT mbtiles suitable for loading.
const MVT_SQL: &str = include_str!("../../tests/fixtures/mbtiles/world_cities.sql");

/// Create a real on-disk mbtiles file from a SQL script.
async fn create_mbtiles_file(path: &PathBuf, sql: &str) {
    let mbt = Mbtiles::new(path).expect("create Mbtiles handle");
    let mut conn = mbt.open_or_new().await.expect("open_or_new");
    sqlx::raw_sql(sql)
        .execute(&mut conn)
        .await
        .expect("execute sql");
    conn.close().await.expect("close connection");
    // Connection dropped here — SQLite flushes WAL to the main file.
}

/// Give inotify / the tokio task a moment to process the event.
async fn wait_for_event() {
    sleep(Duration::from_millis(1000)).await;
}

/// Give the watcher time to initialise before acting on files.
async fn wait_for_watcher_init() {
    sleep(Duration::from_millis(200)).await;
}

/// Create an advisory channel wired to `tsm` and return the sender half.
fn make_advisory_tx(tsm: &TileSourceManager) -> mpsc::Sender<ReloadAdvisory> {
    let (tx, rx) = mpsc::channel(64);
    tsm.clone().run_advisory_loop(rx);
    tx
}

// ---------------------------------------------------------------------------
// Test: new file in watched directory is loaded
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watcher_loads_new_file_in_watched_directory() {
    let dir = tempdir().expect("tempdir");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let tx = make_advisory_tx(&tsm);

    MBTilesReloader::start(tsm.id_resolver(), tx, vec![dir.path().to_path_buf()]);
    wait_for_watcher_init().await;

    let path = dir.path().join("new_source.mbtiles");
    create_mbtiles_file(&path, MVT_SQL).await;

    wait_for_event().await;

    let ids = tsm.source_ids();
    assert_eq!(
        ids.len(),
        1,
        "watcher should have loaded the new file; found: {ids:?}"
    );
}

// ---------------------------------------------------------------------------
// Test: file discovered by watcher is removed when deleted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn watcher_removes_source_on_file_deletion() {
    let dir = tempdir().expect("tempdir");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let tx = make_advisory_tx(&tsm);

    // Start the watcher watching the directory.
    MBTilesReloader::start(tsm.id_resolver(), tx, vec![dir.path().to_path_buf()]);
    wait_for_watcher_init().await;

    // Create the file — watcher discovers it and loads it into the TSM.
    let path = dir.path().join("cities.mbtiles");
    create_mbtiles_file(&path, MVT_SQL).await;
    wait_for_event().await;

    let ids = tsm.source_ids();
    assert_eq!(
        ids.len(),
        1,
        "source should be present after discovery; found: {ids:?}"
    );
    let id = ids.into_iter().next().unwrap();

    // Delete the file — watcher should remove it from the TSM.
    std::fs::remove_file(&path).expect("remove file");
    wait_for_event().await;

    assert!(
        tsm.get_source(&id).is_none(),
        "source should be removed after file deletion"
    );
}
