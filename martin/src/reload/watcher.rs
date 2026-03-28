//! Generic filesystem watcher for file-based tile sources.
//!
//! [`TileFileWatcher::start`] spawns a background task that watches registered
//! tile files and directories for changes, sending [`ReloadAdvisory`] values
//! into the shared advisory channel.  Format-specific knowledge lives in the
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

use super::{ReloadAdvisory, TileSourceWatcher};
use crate::config::primitives::IdResolver;

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
    /// `idr` is used by loaders to assign stable IDs to newly-discovered files.
    /// `tx` is the shared advisory channel; all reload events are sent here and
    /// consumed by [`TileSourceManager::run_advisory_loop`](super::TileSourceManager::run_advisory_loop).
    ///
    /// `loaders` is a list of format-specific handlers; the watcher consults
    /// them (via [`TileSourceWatcher::can_handle`]) to decide whether to act on
    /// a file and which loader to use.
    ///
    /// - **Modified file** → source is hot-reloaded via the advisory channel.
    /// - **Deleted file** → source is removed via the advisory channel.
    /// - **New file** in a watched directory → source is loaded if a loader
    ///   reports it can handle the file.
    pub async fn start(
        idr: IdResolver,
        tx: mpsc::Sender<ReloadAdvisory>,
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

        // fs_tx/fs_rx carry raw filesystem events from the notify callback into
        // the async event loop.  Named distinctly to avoid shadowing the
        // advisory `tx`.
        let (fs_tx, mut fs_rx) = mpsc::channel::<Event>(256);

        let mut watcher = match RecommendedWatcher::new(
            move |result: notify::Result<Event>| {
                if let Ok(event) = result {
                    let _ = fs_tx.blocking_send(event);
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
            while let Some(event) = fs_rx.recv().await {
                for path in &event.paths {
                    let canon = path.canonicalize().unwrap_or_else(|_| path.clone());
                    handle_event(
                        &event.kind,
                        path,
                        &canon,
                        &idr,
                        &tx,
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

/// Send an advisory into the channel, logging a warning if the channel is closed.
async fn send_advisory(tx: &mpsc::Sender<ReloadAdvisory>, advisory: ReloadAdvisory) {
    if tx.send(advisory).await.is_err() {
        warn!("Advisory channel closed; dropping reload advisory");
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_event(
    kind: &EventKind,
    path: &PathBuf,
    canon: &PathBuf,
    idr: &IdResolver,
    tx: &mpsc::Sender<ReloadAdvisory>,
    path_to_id: &DashMap<PathBuf, String>,
    watched_dirs: &[PathBuf],
    loaders: &[Arc<dyn TileSourceWatcher>],
) {
    match kind {
        EventKind::Modify(_) => {
            if let Some(id) = path_to_id.get(canon).map(|r| r.clone()) {
                if !path.exists() {
                    // File was deleted; some kernels/filesystems emit Modify
                    // before (or instead of) Remove — treat it as a removal.
                    path_to_id.remove(canon);
                    info!("Removing source `{id}` (file deleted: {})", path.display());
                    send_advisory(tx, ReloadAdvisory {
                        added: vec![],
                        changed: vec![],
                        removed: vec![id],
                    }).await;
                } else if let Some(loader) = loaders.iter().find(|l| l.can_handle(path)) {
                    info!("Reloading source `{id}` (file changed: {})", canon.display());
                    // Convert Result → Option before the next await so the
                    // non-Send MartinError is not held across the await point.
                    let advisory = match loader.reload_source(&id, path.clone()).await {
                        Ok(a) => Some(a),
                        Err(e) => { warn!("Reload of `{id}` failed: {e}"); None }
                    };
                    if let Some(advisory) = advisory {
                        send_advisory(tx, advisory).await;
                    }
                }
            }
        }
        EventKind::Remove(_) => {
            // When the file is already gone, canonicalize() falls back to the
            // raw path, which may not match the canonicalized key stored at
            // setup time.  Try both forms so the entry is always cleaned up.
            let removed = path_to_id
                .remove(canon)
                .or_else(|| if canon != path { path_to_id.remove(path) } else { None });
            if let Some((_, id)) = removed {
                info!(
                    "Removing source `{id}` (file deleted: {})",
                    path.display()
                );
                send_advisory(tx, ReloadAdvisory {
                    added: vec![],
                    changed: vec![],
                    removed: vec![id],
                }).await;
            }
        }
        EventKind::Create(_) => {
            // If the created path is a tracked file, reload it in place.
            if let (Some(id), Some(loader)) = (
                path_to_id.get(canon).map(|r| r.clone()),
                loaders.iter().find(|l| l.can_handle(path)),
            ) {
                info!(
                    "Reloading source `{id}` (file replaced: {})",
                    canon.display()
                );
                let advisory = match loader.reload_source(&id, path.clone()).await {
                    Ok(a) => Some(a),
                    Err(e) => { warn!("Reload of `{id}` failed: {e}"); None }
                };
                if let Some(advisory) = advisory {
                    send_advisory(tx, advisory).await;
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
            if let Some(loader) = in_watched.then(|| loaders.iter().find(|l| l.can_handle(path))).flatten() {
                info!("Loading new source from {}", path.display());
                let result = match loader.load_file(idr, path.clone()).await {
                    Ok(a) => Some(a),
                    Err(e) => { warn!("Failed to load {}: {e}", path.display()); None }
                };
                if let Some(advisory) = result {
                    let added_ids = advisory.added_ids();
                    send_advisory(tx, advisory).await;
                    for id in added_ids {
                        path_to_id.insert(canon.clone(), id);
                    }
                }
            }
        }
        _ => {}
    }
}
