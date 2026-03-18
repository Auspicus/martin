#![cfg(feature = "mbtiles")]

use martin_core::tiles::Source as _;
use martin_core::tiles::mbtiles::{MbtSource, MbtilesError};
use martin_tile_utils::{Format, TileCoord};
use mbtiles::temp_named_mbtiles;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const MVT_SCRIPT: &str = include_str!("../../tests/fixtures/mbtiles/world_cities.sql");
const PNG_SCRIPT: &str = include_str!("../../tests/fixtures/mbtiles/geography-class-png.sql");
const JSON_SCRIPT: &str = include_str!("../../tests/fixtures/mbtiles/json.sql");

async fn mvt_source(test_name: &str) -> (MbtSource, mbtiles::Mbtiles, mbtiles::sqlx::SqliteConnection) {
    let (_mbt, _conn, path) =
        temp_named_mbtiles(&format!("mbt_core_{test_name}_mvt"), MVT_SCRIPT).await;
    let source = MbtSource::new(test_name.to_string(), path)
        .await
        .expect("MbtSource::new should succeed for world_cities");
    (source, _mbt, _conn)
}

async fn png_source(test_name: &str) -> (MbtSource, mbtiles::Mbtiles, mbtiles::sqlx::SqliteConnection) {
    let (_mbt, _conn, path) =
        temp_named_mbtiles(&format!("mbt_core_{test_name}_png"), PNG_SCRIPT).await;
    let source = MbtSource::new(test_name.to_string(), path)
        .await
        .expect("MbtSource::new should succeed for geography-class-png");
    (source, _mbt, _conn)
}

// ---------------------------------------------------------------------------
// Metadata / identity tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mvt_source_id() {
    let (source, _mbt, _conn) = mvt_source("mvt_source_id").await;
    assert_eq!(source.get_id(), "mvt_source_id");
}

#[tokio::test]
async fn mvt_source_format_is_mvt() {
    let (source, _mbt, _conn) = mvt_source("mvt_fmt").await;
    assert_eq!(source.get_tile_info().format, Format::Mvt);
}

#[tokio::test]
async fn png_source_format_is_png() {
    let (source, _mbt, _conn) = png_source("png_fmt").await;
    assert_eq!(source.get_tile_info().format, Format::Png);
}

#[tokio::test]
async fn mvt_source_tilejson_has_zoom_bounds() {
    let (source, _mbt, _conn) = mvt_source("mvt_tilejson").await;
    let tj = source.get_tilejson();
    assert!(tj.minzoom.is_some(), "minzoom should be set");
    assert!(tj.maxzoom.is_some(), "maxzoom should be set");
}

#[tokio::test]
async fn multiple_sources_have_unique_ids() {
    let (_mbt1, _conn1, path1) =
        temp_named_mbtiles("mbt_core_unique_ids_1", MVT_SCRIPT).await;
    let (_mbt2, _conn2, path2) =
        temp_named_mbtiles("mbt_core_unique_ids_2", MVT_SCRIPT).await;

    let s1 = MbtSource::new("source1".to_string(), path1).await.unwrap();
    let s2 = MbtSource::new("source2".to_string(), path2).await.unwrap();

    assert_eq!(s1.get_id(), "source1");
    assert_eq!(s2.get_id(), "source2");
    assert_ne!(s1.get_id(), s2.get_id());
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nonexistent_file_returns_error() {
    let result =
        MbtSource::new("bad".to_string(), "/no/such/file.mbtiles".into()).await;
    assert!(
        matches!(result, Err(MbtilesError::IoError(..))),
        "expected IoError, got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Tile retrieval tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retrieve_valid_mvt_tile() {
    let (source, _mbt, _conn) = mvt_source("get_tile_mvt").await;
    let tile = source
        .get_tile(TileCoord { z: 0, x: 0, y: 0 }, None)
        .await
        .expect("get_tile should succeed");
    assert!(!tile.is_empty(), "z/0/0/0 should contain tile data");
}

#[tokio::test]
async fn retrieve_valid_png_tile() {
    let (source, _mbt, _conn) = png_source("get_tile_png").await;
    let tile = source
        .get_tile(TileCoord { z: 1, x: 0, y: 0 }, None)
        .await
        .expect("get_tile should succeed");
    assert!(!tile.is_empty(), "z/1/0/0 should contain tile data");
}

#[tokio::test]
async fn missing_tile_returns_empty_vec() {
    let (source, _mbt, _conn) = mvt_source("missing_tile").await;
    let tile = source
        .get_tile(
            TileCoord {
                z: 20,
                x: 999_999,
                y: 999_999,
            },
            None,
        )
        .await
        .expect("get_tile should not error for missing tile");
    assert!(tile.is_empty(), "missing tile should return empty data");
}

#[tokio::test]
async fn repeated_tile_requests_return_same_data() {
    let (source, _mbt, _conn) = mvt_source("repeated_tile").await;
    let coord = TileCoord { z: 0, x: 0, y: 0 };

    let t1 = source.get_tile(coord, None).await.expect("first request");
    let t2 = source.get_tile(coord, None).await.expect("second request");
    let t3 = source.get_tile(coord, None).await.expect("third request");

    assert_eq!(t1, t2, "first and second results should match");
    assert_eq!(t2, t3, "second and third results should match");
    assert!(!t1.is_empty(), "tile data should not be empty");
}

// ---------------------------------------------------------------------------
// ETag tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tile_with_etag_is_non_empty() {
    let (source, _mbt, _conn) = mvt_source("etag_basic").await;
    let tile = source
        .get_tile_with_etag(TileCoord { z: 0, x: 0, y: 0 }, None)
        .await
        .expect("get_tile_with_etag should succeed");

    assert!(!tile.data.is_empty(), "tile data should not be empty");
    assert!(!tile.etag.is_empty(), "ETag should not be empty");
    assert_eq!(tile.info.format, Format::Mvt, "format should be MVT");
}

#[tokio::test]
async fn repeated_requests_return_same_etag() {
    let (source, _mbt, _conn) = mvt_source("etag_consistency").await;
    let coord = TileCoord { z: 0, x: 0, y: 0 };

    let t1 = source.get_tile_with_etag(coord, None).await.expect("first");
    let t2 = source.get_tile_with_etag(coord, None).await.expect("second");

    assert_eq!(t1.etag, t2.etag, "ETags should be stable across requests");
    assert_eq!(t1.data, t2.data, "tile data should be stable");
}

#[tokio::test]
async fn empty_tile_still_has_etag() {
    let (source, _mbt, _conn) = mvt_source("etag_empty").await;
    let tile = source
        .get_tile_with_etag(
            TileCoord {
                z: 20,
                x: 999_999,
                y: 999_999,
            },
            None,
        )
        .await
        .expect("get_tile_with_etag should not error");

    assert!(tile.data.is_empty(), "missing tile should have no data");
    assert!(!tile.etag.is_empty(), "missing tile should still have an ETag");
}

// ---------------------------------------------------------------------------
// JSON source (non-raster, non-MVT)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn json_source_format_is_json() {
    let (_mbt, _conn, path) =
        temp_named_mbtiles("mbt_core_json_fmt", JSON_SCRIPT).await;
    let source = MbtSource::new("json_source".to_string(), path)
        .await
        .expect("json source creation should succeed");
    assert_eq!(source.get_tile_info().format, Format::Json);
}
