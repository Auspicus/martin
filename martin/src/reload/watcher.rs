//! Generic filesystem watcher for file-based tile sources.
//!
//! [`TileFileWatcher::start`] spawns a background task that watches registered
//! tile files and directories for changes, forwarding them to the
//! [`TileSourceManager`].  Format-specific knowledge lives in the
//! [`TileSourceWatcher`](super::TileSourceWatcher) implementations passed to
//! `start` — the watcher itself is format-agnostic.
//!
//! ## Implementation note: directory watching
//!
//! We watch **parent directories** rather than individual files.  On
//! overlay/container filesystems (common in CI and Docker) inotify
//! `DELETE_SELF` for individual file watches is unreliable, whereas
//! `IN_DELETE` delivered on a directory watch works correctly on all
//! supported kernels.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use notify::event::{AccessKind, AccessMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{info, warn};

use super::{ReloadAdvisory, TileSourceManager, TileSourceWatcher};

/// Paths needed to start the tile filesystem watcher.
pub struct WatchPaths {
    /// Existing sources: maps source ID → original file path.
    pub id_to_path: HashMap<String, PathBuf>,
    /// Directories to watch for newly-created tile files.
    pub watched_dirs: Vec<PathBuf>,
}

/// Generic filesystem watcher for file-based tile sources.
pub struct TileFileWatcher;

impl TileFileWatcher {
    /// Spawn a background task that reacts to filesystem events.
    ///
    /// `loaders` is a list of format-specific handlers; the watcher consults
    /// them (via [`TileSourceWatcher::can_handle`]) to decide whether to act on
    /// a file and which loader to use.
    ///
    /// - **Modified file** → source is hot-reloaded in the TSM.
    /// - **Deleted file** → source is removed from the TSM.
    /// - **New file** in a watched directory → source is loaded if a loader
    ///   reports it can handle the file.
    pub async fn start(
        tsm: TileSourceManager,
        paths: WatchPaths,
        loaders: Vec<Arc<dyn TileSourceWatcher>>,
    ) {
        let WatchPaths {
            id_to_path,
            watched_dirs,
        } = paths;

        // Build a reverse map from canonical file path → source ID.
        let path_to_id: DashMap<PathBuf, String> = id_to_path
            .into_iter()
            .map(|(id, path)| {
                let canon = path.canonicalize().unwrap_or(path);
                (canon, id)
            })
            .collect();

        // Canonicalize watched directories.
        let watched_dirs: Vec<PathBuf> = watched_dirs
            .iter()
            .map(|d| d.canonicalize().unwrap_or_else(|_| d.clone()))
            .collect();

        let (tx, mut rx) = mpsc::channel::<Event>(256);

        let mut watcher = match RecommendedWatcher::new(
            move |result: notify::Result<Event>| {
                if let Ok(event) = result {
                    let _ = tx.blocking_send(event);
                }
            },
            Config::default(),
        ) {
            Ok(w) => w,
            Err(e) => {
                warn!("Failed to initialise filesystem watcher: {e}");
                return;
            }
        };

        // Collect unique directories to watch:
        //   - parent directories of each tracked file
        //   - explicitly configured watched directories
        let mut dirs_to_watch: HashSet<PathBuf> = HashSet::new();
        for entry in path_to_id.iter() {
            if let Some(parent) = entry.key().parent() {
                dirs_to_watch.insert(parent.to_path_buf());
            }
        }
        for dir in &watched_dirs {
            dirs_to_watch.insert(dir.clone());
        }

        for dir in &dirs_to_watch {
            if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
                warn!("Cannot watch directory {}: {e}", dir.display());
            }
        }

        tokio::spawn(async move {
            // Keep the watcher alive for the lifetime of this task.
            let _watcher = watcher;
            while let Some(event) = rx.recv().await {
                for path in &event.paths {
                    let canon = path.canonicalize().unwrap_or_else(|_| path.clone());
                    handle_event(
                        &event.kind,
                        path,
                        &canon,
                        &tsm,
                        &path_to_id,
                        &watched_dirs,
                        &loaders,
                    )
                    .await;
                }
            }
        });
    }
}

async fn handle_event(
    kind: &EventKind,
    path: &PathBuf,
    canon: &PathBuf,
    tsm: &TileSourceManager,
    path_to_id: &DashMap<PathBuf, String>,
    watched_dirs: &[PathBuf],
    loaders: &[Arc<dyn TileSourceWatcher>],
) {
    match kind {
        EventKind::Modify(_) => {
            if let Some(id) = path_to_id.get(canon).map(|r| r.clone()) {
                if let Some(loader) = loaders.iter().find(|l| l.can_handle(path)) {
                    info!("Reloading source `{id}` (file changed: {})", canon.display());
                    match loader.reload_source(tsm, &id, path.clone()).await {
                        Ok(advisory) => tsm.apply_advisory(advisory),
                        Err(e) => warn!("Reload of `{id}` failed: {e}"),
                    }
                }
            }
        }
        EventKind::Remove(_) => {
            if let Some((_, id)) = path_to_id.remove(canon) {
                info!(
                    "Removing source `{id}` (file deleted: {})",
                    canon.display()
                );
                tsm.apply_advisory(ReloadAdvisory {
                    added: vec![],
                    changed: vec![],
                    removed: vec![id],
                });
            }
        }
        EventKind::Create(_) => {
            // If the created path is a tracked file, reload it in place.
            if let Some(id) = path_to_id.get(canon).map(|r| r.clone()) {
                if let Some(loader) = loaders.iter().find(|l| l.can_handle(path)) {
                    info!(
                        "Reloading source `{id}` (file replaced: {})",
                        canon.display()
                    );
                    match loader.reload_source(tsm, &id, path.clone()).await {
                        Ok(advisory) => tsm.apply_advisory(advisory),
                        Err(e) => warn!("Reload of `{id}` failed: {e}"),
                    }
                }
            }
            // New files are loaded on Close(Write) once fully written.
        }
        // Load new files after write is complete and file descriptor closed.
        // This fires after Create(File) once all data has been flushed, avoiding
        // attempts to open a partially-written file.
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => {
            if path_to_id.contains_key(canon) {
                return;
            }
            let in_watched = watched_dirs
                .iter()
                .any(|dir| canon.parent().is_some_and(|p| p == dir));
            if in_watched {
                if let Some(loader) = loaders.iter().find(|l| l.can_handle(path)) {
                    info!("Loading new source from {}", path.display());
                    match loader.load_file(tsm, path.clone()).await {
                        Ok(advisory) => {
                            let added_ids = advisory.added_ids();
                            tsm.apply_advisory(advisory);
                            for id in added_ids {
                                path_to_id.insert(canon.clone(), id);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to load {}: {e}", path.display());
                        }
                    }
                }
            }
        }
        _ => {}
    }
}
