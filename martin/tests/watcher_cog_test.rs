//! Integration tests for the COG filesystem watcher.
//!
//! These tests confirm that:
//! - a new `.tif` file in a watched directory is loaded automatically
//! - a `.tif` file that was discovered via the watcher is removed when deleted

#![cfg(all(feature = "unstable-cog", feature = "_file_watcher"))]

use std::path::{Path, PathBuf};
use std::time::Duration;

use martin::config::file::reload::ReloadAdvisory;
use martin::config::file::reload::TileSourceManager;
use martin::config::file::tiles::reload::cog::COGReloader;
use martin_core::tiles::NO_TILE_CACHE;
use tempfile::tempdir;
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Returns the path to a small COG fixture file.
fn cog_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/fixtures/cog/usda_naip_128_none_z2.tif")
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
async fn cog_watcher_loads_new_file_in_watched_directory() {
    let dir = tempdir().expect("tempdir");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let tx = make_advisory_tx(&tsm);

    COGReloader::start(tsm.id_resolver(), tx, vec![dir.path().to_path_buf()]);
    wait_for_watcher_init().await;

    let path = dir.path().join("new_source.tif");
    std::fs::copy(cog_fixture(), &path).expect("copy fixture into watched dir");

    wait_for_event().await;

    let ids = tsm.source_ids();
    assert_eq!(
        ids.len(),
        1,
        "watcher should have loaded the new COG file; found: {ids:?}"
    );
}

// ---------------------------------------------------------------------------
// Test: file discovered by watcher is removed when deleted
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cog_watcher_removes_source_on_file_deletion() {
    let dir = tempdir().expect("tempdir");

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let tx = make_advisory_tx(&tsm);

    // Start the watcher watching the directory.
    COGReloader::start(tsm.id_resolver(), tx, vec![dir.path().to_path_buf()]);
    wait_for_watcher_init().await;

    // Create the file — watcher discovers it and loads it into the TSM.
    let path = dir.path().join("naip.tif");
    std::fs::copy(cog_fixture(), &path).expect("copy fixture");
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
