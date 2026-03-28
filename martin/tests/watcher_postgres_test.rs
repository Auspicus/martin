//! Integration tests for the PostgreSQL poll watcher.
//!
//! These tests confirm that:
//! - A new geometry table in a watched schema is discovered on the next poll.
//! - A dropped geometry table is removed from the TSM on the next poll.
//!
//! The tests require a running PostGIS database pointed to by `DATABASE_URL`.
//! All DDL is scoped to the `watcher_test` schema, which is created and torn
//! down by the tests, so they do not interfere with the fixture data used by
//! the other postgres integration tests.

#![cfg(feature = "test-pg")]

use std::env;
use std::time::Duration;

use martin::config::file::postgres::{
    PostgresCfgPublish, PostgresConfig, POOL_SIZE_DEFAULT,
};
use martin::config::primitives::IdResolver;
use martin::config::primitives::OptBoolObj;
use martin::config::primitives::OptOneMany;
use martin::reload::ReloadAdvisory;
use martin::reload::TileSourceManager;
use martin::reload::postgres::{PostgresPollSetup, PostgresPoller};
use martin::srv::RESERVED_KEYWORDS;
use martin_core::tiles::NO_TILE_CACHE;
use martin_core::tiles::postgres::PostgresPool;
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Schema used exclusively by these tests. Isolated from the fixture data.
const WATCHER_SCHEMA: &str = "watcher_test";

/// How long to wait for the poller to process a change.
///
/// The poll interval is 1 s; we wait 3 s to allow for at least two cycles
/// and any scheduling jitter.
const POLL_WAIT: Duration = Duration::from_millis(3_000);

/// Returns the test database connection string from `DATABASE_URL`.
fn db_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must be set for test-pg tests")
}

/// Creates a pool pointing at the test database.
async fn make_pool() -> PostgresPool {
    PostgresPool::new(&db_url(), None, None, None, POOL_SIZE_DEFAULT)
        .await
        .expect("PostgresPool::new")
}

/// Execute raw SQL via the given pool.
async fn exec(pool: &PostgresPool, sql: &str) {
    pool.get()
        .await
        .expect("pool connection")
        .batch_execute(sql)
        .await
        .expect("SQL execution failed");
}

/// Build a `PostgresConfig` scoped to [`WATCHER_SCHEMA`] with a 1 s poll interval.
fn watcher_config() -> PostgresConfig {
    PostgresConfig {
        connection_string: Some(db_url()),
        auto_publish: OptBoolObj::Object(PostgresCfgPublish {
            from_schemas: OptOneMany::One(WATCHER_SCHEMA.to_string()),
            ..Default::default()
        }),
        watch_interval_secs: Some(1),
        ..Default::default()
    }
}

/// Create an advisory channel wired to `tsm` and return the sender half.
fn make_advisory_tx(tsm: &TileSourceManager) -> mpsc::Sender<ReloadAdvisory> {
    let (tx, rx) = mpsc::channel(64);
    tsm.clone().run_advisory_loop(rx);
    tx
}

// ---------------------------------------------------------------------------
// Test: new table is discovered on the next poll
// ---------------------------------------------------------------------------

#[tokio::test]
async fn postgres_poller_discovers_new_table() {
    let pool = make_pool().await;

    // Ensure a clean slate.
    exec(&pool, &format!("DROP SCHEMA IF EXISTS {WATCHER_SCHEMA} CASCADE;")).await;
    exec(&pool, &format!("CREATE SCHEMA {WATCHER_SCHEMA};")).await;

    let config = watcher_config();
    let idr = IdResolver::new(RESERVED_KEYWORDS);
    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let tx = make_advisory_tx(&tsm);

    PostgresPoller::start(
        tx,
        PostgresPollSetup { config, idr, interval: Duration::from_secs(1) },
    );

    // Give the poller time to run its first (empty) cycle.
    sleep(POLL_WAIT).await;
    assert_eq!(tsm.source_ids().len(), 0, "no sources before table is created");

    // Create a geometry table in the watched schema.
    exec(
        &pool,
        &format!(
            "CREATE TABLE {WATCHER_SCHEMA}.discover_me \
             (id serial PRIMARY KEY, geom geometry(Point, 4326));"
        ),
    )
    .await;

    // Wait for the next poll cycle to pick it up.
    sleep(POLL_WAIT).await;

    let ids = tsm.source_ids();
    assert!(
        ids.iter().any(|id| id.contains("discover_me")),
        "poller should have discovered 'discover_me'; sources found: {ids:?}"
    );

    // Cleanup.
    exec(&pool, &format!("DROP SCHEMA IF EXISTS {WATCHER_SCHEMA} CASCADE;")).await;
}

// ---------------------------------------------------------------------------
// Test: dropped table is removed on the next poll
// ---------------------------------------------------------------------------

#[tokio::test]
async fn postgres_poller_removes_dropped_table() {
    let pool = make_pool().await;

    exec(&pool, &format!("DROP SCHEMA IF EXISTS {WATCHER_SCHEMA} CASCADE;")).await;
    exec(&pool, &format!("CREATE SCHEMA {WATCHER_SCHEMA};")).await;

    // Create the table BEFORE the poller starts so it is present from the
    // very first poll cycle.
    exec(
        &pool,
        &format!(
            "CREATE TABLE {WATCHER_SCHEMA}.drop_me \
             (id serial PRIMARY KEY, geom geometry(Point, 4326));"
        ),
    )
    .await;

    let config = watcher_config();
    let idr = IdResolver::new(RESERVED_KEYWORDS);
    let tsm = TileSourceManager::new(NO_TILE_CACHE);
    let tx = make_advisory_tx(&tsm);

    PostgresPoller::start(
        tx,
        PostgresPollSetup { config, idr, interval: Duration::from_secs(1) },
    );

    // Wait for the poller to discover the initial table.
    sleep(POLL_WAIT).await;

    let ids_before = tsm.source_ids();
    assert!(
        ids_before.iter().any(|id| id.contains("drop_me")),
        "source 'drop_me' should be present before drop; sources: {ids_before:?}"
    );

    // Drop the table and wait for the next poll.
    exec(
        &pool,
        &format!("DROP TABLE {WATCHER_SCHEMA}.drop_me;"),
    )
    .await;
    sleep(POLL_WAIT).await;

    let ids_after = tsm.source_ids();
    assert!(
        !ids_after.iter().any(|id| id.contains("drop_me")),
        "source 'drop_me' should be gone after table drop; remaining: {ids_after:?}"
    );

    // Cleanup.
    exec(&pool, &format!("DROP SCHEMA IF EXISTS {WATCHER_SCHEMA} CASCADE;")).await;
}
