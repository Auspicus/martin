//! PostgreSQL polling reloader for the [`TileSourceManager`](super::TileSourceManager).
//!
//! [`PostgresPoller`] periodically re-discovers tables and functions from a
//! PostgreSQL connection and pushes [`ReloadAdvisory`] values into the shared
//! advisory channel so the tile catalog stays in sync with the database schema.
//!
//! ## What is detected
//!
//! | Event                              | Action               |
//! |------------------------------------|----------------------|
//! | New table / function created       | Source added to TSM  |
//! | Table / function dropped           | Source removed       |
//! | Geometry type or bounds changed    | Source refreshed, cached tiles invalidated |
//!
//! Data-only changes (row inserts / updates / deletes) are **not** detected
//! automatically.  Use a short [`watch_interval_secs`] or disable tile caching
//! for near-real-time data freshness.
//!
//! ## Pool lifecycle
//!
//! A fresh [`PostgresPool`] (and therefore fresh DB connections) is created on
//! each poll cycle.  Pool initialisation performs two lightweight queries to
//! check the Postgres and PostGIS versions; the actual table discovery follows.
//! For typical polling intervals (≥ 30 s) this overhead is negligible.
//!
//! [`watch_interval_secs`]: crate::config::file::postgres::PostgresConfig::watch_interval_secs

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use super::ReloadAdvisory;
use crate::config::file::postgres::PostgresConfig;
use crate::config::primitives::IdResolver;

/// Everything needed to start a Postgres polling watcher.
///
/// Constructed in [`Config::resolve()`](crate::config::file::Config::resolve)
/// for each [`PostgresConfig`] that has
/// [`watch_interval_secs`](PostgresConfig::watch_interval_secs) set, and
/// passed to [`PostgresPoller::start`].
pub struct PostgresPollSetup {
    /// A clone of the postgres config **before** resolution mutates it.
    ///
    /// Preserves `tables: None` / `functions: None` so that each poll
    /// re-runs full auto-discovery rather than re-using the initial snapshot.
    pub config: PostgresConfig,
    /// The same [`IdResolver`] that was used during initial resolution.
    ///
    /// Sharing the resolver ensures that re-polled sources receive the same
    /// stable IDs as they did at startup.
    pub idr: IdResolver,
    /// Time between successive polls.
    pub interval: Duration,
}

/// Runs a background polling loop for a single PostgreSQL connection.
///
/// Call [`start`](Self::start) once per [`PostgresConfig`] that has polling
/// enabled; the spawned task runs until the advisory channel is closed or the
/// process exits.
pub struct PostgresPoller;

impl PostgresPoller {
    /// Spawn a background task that polls the configured connection at the
    /// given interval and sends [`ReloadAdvisory`] values into `tx`.
    ///
    /// On each poll the config is cloned and fully re-resolved (new connection
    /// pool, fresh table/function discovery).  The resulting source set is
    /// diffed against the locally-tracked state:
    ///
    /// - **New** sources (not previously known) are sent as
    ///   [`added`](ReloadAdvisory::added).
    /// - **Existing** sources whose `TileJSON` has changed are sent as
    ///   [`changed`](ReloadAdvisory::changed), which invalidates their cached
    ///   tiles.
    /// - **Missing** sources (dropped from the DB) are sent as
    ///   [`removed`](ReloadAdvisory::removed).
    ///
    /// If nothing changed no advisory is sent and the TSM is left untouched.
    /// The task exits if `tx` is closed (i.e. the advisory consumer has stopped).
    pub fn start(tx: mpsc::Sender<ReloadAdvisory>, setup: PostgresPollSetup) {
        let PostgresPollSetup { config, idr, interval } = setup;

        tokio::spawn(async move {
            let conn_id = config
                .connection_string
                .as_deref()
                .unwrap_or("unknown")
                .to_string();

            info!(
                "Starting Postgres poll watcher for {conn_id} \
                 (interval: {}s)",
                interval.as_secs()
            );

            // IDs that belong to this Postgres connection.  Starts empty;
            // the first poll populates it, re-upserting sources that are
            // already in the TSM (harmless — just invalidates their caches).
            let mut known_ids: HashSet<String> = HashSet::new();

            // Last-seen serialized TileJSON per source ID.  Used to detect
            // schema changes without querying the TSM.
            let mut last_tilejson: HashMap<String, serde_json::Value> = HashMap::new();

            loop {
                tokio::time::sleep(interval).await;

                debug!("Polling Postgres sources for {conn_id}");

                let mut cfg_clone = config.clone();
                let new_sources = match cfg_clone.resolve(idr.clone()).await {
                    Ok((sources, _warnings)) => sources,
                    Err(e) => {
                        warn!("Postgres poll failed for {conn_id}: {e}");
                        continue;
                    }
                };

                let new_ids: HashSet<String> =
                    new_sources.iter().map(|s| s.get_id().to_string()).collect();

                // Sources that disappeared from the DB since the last poll.
                let removed: Vec<String> = known_ids
                    .iter()
                    .filter(|id| !new_ids.contains(*id))
                    .cloned()
                    .collect();

                let mut added = Vec::new();
                let mut changed = Vec::new();

                for source in new_sources {
                    let id = source.get_id().to_string();
                    let current_tj = serde_json::to_value(source.get_tilejson()).ok();
                    if known_ids.contains(&id) {
                        // Only re-register if the TileJSON metadata changed;
                        // this avoids unnecessary cache invalidation when the
                        // schema is stable.
                        let tilejson_changed = last_tilejson.get(&id) != current_tj.as_ref();
                        if tilejson_changed {
                            if let Some(tj) = current_tj {
                                last_tilejson.insert(id.clone(), tj);
                            }
                            changed.push(source);
                        }
                    } else {
                        info!("Postgres poller: new source discovered `{id}` on {conn_id}");
                        if let Some(tj) = current_tj {
                            last_tilejson.insert(id.clone(), tj);
                        }
                        added.push(source);
                    }
                }

                // Update local tracking state.
                for id in &removed {
                    info!("Postgres poller: source removed `{id}` on {conn_id}");
                    known_ids.remove(id);
                    last_tilejson.remove(id);
                }
                for source in &added {
                    known_ids.insert(source.get_id().to_string());
                }

                if added.is_empty() && changed.is_empty() && removed.is_empty() {
                    debug!("Postgres poll: no changes for {conn_id}");
                    continue;
                }

                if tx.send(ReloadAdvisory { added, changed, removed }).await.is_err() {
                    warn!("Advisory channel closed for {conn_id}; stopping Postgres poller");
                    break;
                }
            }
        });
    }
}
