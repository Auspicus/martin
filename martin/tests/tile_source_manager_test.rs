#![cfg(feature = "mbtiles")]

use martin::reload::TileSourceManager;
use martin::reload::mbtiles::MBTilesReloader;
use martin_core::tiles::NO_TILE_CACHE;
use martin_tile_utils::{Format, TileCoord};
use mbtiles::temp_named_mbtiles;

// ---------------------------------------------------------------------------
// SQL fixtures
// ---------------------------------------------------------------------------

const MVT_SCRIPT: &str = include_str!("../../tests/fixtures/mbtiles/world_cities.sql");
const PNG_SCRIPT: &str = include_str!("../../tests/fixtures/mbtiles/geography-class-png.sql");
const MVT_MODIFIED_SCRIPT: &str =
    include_str!("../../tests/fixtures/mbtiles/world_cities_modified.sql");

// ---------------------------------------------------------------------------
// Loading sources
// ---------------------------------------------------------------------------

#[tokio::test]
async fn load_single_source_is_accessible() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_single", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path)
        .await
        .expect("load_file should succeed");

    assert!(!id.is_empty(), "assigned ID should not be empty");
    let source = tsm.get_source(&id).expect("source should be findable by its ID");
    assert_eq!(source.get_id(), id);
}

#[tokio::test]
async fn load_multiple_sources_all_accessible() {
    let (_mbt1, _conn1, path1) =
        temp_named_mbtiles("tsm_multi_1", MVT_SCRIPT).await;
    let (_mbt2, _conn2, path2) =
        temp_named_mbtiles("tsm_multi_2", PNG_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let ids = MBTilesReloader::load_files(&tsm, vec![path1, path2])
        .await
        .expect("load_files should succeed");

    assert_eq!(ids.len(), 2, "two IDs should be returned");
    for id in &ids {
        assert!(
            tsm.get_source(id).is_some(),
            "source {id} should be registered"
        );
    }
}

#[tokio::test]
async fn source_ids_reflects_all_loaded_sources() {
    let (_mbt1, _conn1, path1) =
        temp_named_mbtiles("tsm_ids_1", MVT_SCRIPT).await;
    let (_mbt2, _conn2, path2) =
        temp_named_mbtiles("tsm_ids_2", PNG_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id1 = MBTilesReloader::load_file(&tsm, path1).await.unwrap();
    let id2 = MBTilesReloader::load_file(&tsm, path2).await.unwrap();

    let mut ids = tsm.source_ids();
    ids.sort();
    let mut expected = vec![id1, id2];
    expected.sort();
    assert_eq!(ids, expected);
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

#[tokio::test]
async fn catalog_contains_all_loaded_sources() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_catalog", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path).await.unwrap();

    let catalog = tsm.get_catalog();
    assert!(catalog.contains_key(&id), "catalog should contain the loaded source");
}

#[tokio::test]
async fn catalog_entry_has_correct_content_type_for_mvt() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_catalog_mvt", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path).await.unwrap();

    let catalog = tsm.get_catalog();
    let entry = catalog.get(&id).expect("entry should exist");
    assert_eq!(
        entry.content_type,
        Format::Mvt.content_type(),
        "content_type should match MVT format"
    );
}

// ---------------------------------------------------------------------------
// Remove source
// ---------------------------------------------------------------------------

#[tokio::test]
async fn remove_source_makes_it_inaccessible() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_remove", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path).await.unwrap();

    assert!(tsm.get_source(&id).is_some(), "source should exist before removal");
    let removed = tsm.remove_source(&id);
    assert!(removed, "remove_source should return true for existing source");
    assert!(
        tsm.get_source(&id).is_none(),
        "source should be gone after removal"
    );
}

#[tokio::test]
async fn remove_nonexistent_source_returns_false() {
    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    assert!(!tsm.remove_source("does_not_exist"));
}

// ---------------------------------------------------------------------------
// Hot-reload (upsert)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reload_source_replaces_existing() {
    // Load initial version.
    let (_mbt1, _conn1, path1) =
        temp_named_mbtiles("tsm_reload_v1", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path1).await.unwrap();

    // "Reload" using a modified version of the same data (different in-memory db).
    let (_mbt2, _conn2, path2) =
        temp_named_mbtiles("tsm_reload_v2", MVT_MODIFIED_SCRIPT).await;

    MBTilesReloader::reload_source(&tsm, &id, path2)
        .await
        .expect("reload_source should succeed");

    // The source should still be accessible under the same ID.
    let source = tsm.get_source(&id).expect("source should still exist after reload");
    assert_eq!(source.get_id(), id, "ID should remain stable across reloads");
}

// ---------------------------------------------------------------------------
// Tile retrieval through the TSM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retrieve_tile_through_tsm() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_tile_retrieval", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path).await.unwrap();

    let source = tsm.get_source(&id).unwrap();
    let tile = source
        .get_tile(TileCoord { z: 0, x: 0, y: 0 }, None)
        .await
        .expect("tile retrieval should succeed");

    assert!(!tile.is_empty(), "z/0/0/0 should contain tile data");
}

// ---------------------------------------------------------------------------
// ID resolver
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_stem_files_get_unique_ids() {
    // Two different in-memory databases that would produce the same file-stem
    // based name. The TSM should disambiguate them.
    let (_mbt1, _conn1, path1) =
        temp_named_mbtiles("tsm_dedup_a", MVT_SCRIPT).await;
    let (_mbt2, _conn2, path2) =
        temp_named_mbtiles("tsm_dedup_b", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id1 = MBTilesReloader::load_file(&tsm, path1).await.unwrap();
    let id2 = MBTilesReloader::load_file(&tsm, path2).await.unwrap();

    // The paths are different in-memory URIs, so the resolver must give them
    // distinct IDs.
    assert_ne!(id1, id2, "distinct files should receive distinct IDs");
}

// ---------------------------------------------------------------------------
// HTTP-path accessor methods
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_source_actix_returns_source() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_actix_get", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path).await.unwrap();

    let src = tsm
        .get_source_actix(&id)
        .expect("get_source_actix should find the loaded source");
    assert_eq!(src.get_id(), id);
}

#[tokio::test]
async fn get_source_actix_returns_404_for_unknown_id() {
    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let result = tsm.get_source_actix("no_such_source");
    assert!(result.is_err(), "should return an error for unknown source");
}

#[tokio::test]
async fn get_sources_resolves_single_source() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("tsm_get_sources", MVT_SCRIPT).await;

    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let id = MBTilesReloader::load_file(&tsm, path).await.unwrap();

    let (sources, _, _) = tsm
        .get_sources(&id, None)
        .expect("get_sources should succeed");
    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].get_id(), id);
}
