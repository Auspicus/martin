//! Primitive filesystem watcher for tile source file changes.
//!
//! [`TileFileWatcher::start`] spawns a background task that watches registered
//! paths for filesystem events and emits raw [`FileChange`] values.
//! Format-specific logic (extension filtering, source loading, advisory
//! construction) belongs entirely to the callers — the per-format reloaders
//! that embed this watcher.
//!
//! ## Implementation note: directory watching
//!
//! We watch **parent directories** rather than individual files.  On
//! overlay/container filesystems (common in CI and Docker) inotify
//! `DELETE_SELF` for individual file watches is unreliable, whereas
//! `IN_DELETE` delivered on a directory watch works correctly on all
//! supported kernels.

use std::collections::HashSet;
use std::path::PathBuf;

use dashmap::DashSet;
use notify::event::{AccessKind, AccessMode};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::warn;

/// A raw filesystem change event emitted by [`TileFileWatcher`].
pub enum FileChange {
    /// A tracked file was modified and still exists on disk.
    Modified(PathBuf),
    /// A tracked file was deleted (or replaced by a deletion-then-create cycle).
    Deleted(PathBuf),
    /// A new file was fully written to a watched directory.
    New(PathBuf),
}

/// Primitive filesystem watcher that emits raw [`FileChange`] events.
///
/// This type knows nothing about tile-source formats, source IDs, advisory
/// channels, or ID resolution.  All of that logic lives in the per-format
/// reloaders that embed this watcher.
pub struct TileFileWatcher;

impl TileFileWatcher {
    /// Spawn a background task watching `tracked_files` and `watch_dirs`.
    ///
    /// - Modifications to a file in `tracked_files` → [`FileChange::Modified`].
    /// - Deletions of a file in `tracked_files` → [`FileChange::Deleted`].
    /// - New files fully written to a directory in `watch_dirs` →
    ///   [`FileChange::New`] (the path is also added to the tracked set so
    ///   subsequent modifications are also reported).
    ///
    /// All emitted paths are **canonical** (resolved via
    /// [`std::fs::canonicalize`] at event time, falling back to the raw path
    /// when the file no longer exists).
    ///
    /// Events are delivered to `tx`.
    pub async fn start(
        tracked_files: Vec<PathBuf>,
        watch_dirs: Vec<PathBuf>,
        tx: mpsc::Sender<FileChange>,
    ) {
        // Canonicalize tracked files and store in a concurrent set.
        let tracked_paths: DashSet<PathBuf> = tracked_files
            .into_iter()
            .map(|p| p.canonicalize().unwrap_or(p))
            .collect();

        // Canonicalize watched directories.
        let watch_dirs: Vec<PathBuf> = watch_dirs
            .iter()
            .map(|d| d.canonicalize().unwrap_or_else(|_| d.clone()))
            .collect();

        // fs_tx/fs_rx carry raw notify events into the async event loop.
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

        // Watch parent dirs of each tracked file plus the configured watch dirs.
        let mut dirs_to_watch: HashSet<PathBuf> = HashSet::new();
        for path in tracked_paths.iter() {
            if let Some(parent) = path.parent() {
                dirs_to_watch.insert(parent.to_path_buf());
            }
        }
        for dir in &watch_dirs {
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
                    handle_event(&event.kind, path, &canon, &tx, &tracked_paths, &watch_dirs)
                        .await;
                }
            }
        });
    }
}

async fn send_change(tx: &mpsc::Sender<FileChange>, change: FileChange) {
    if tx.send(change).await.is_err() {
        warn!("FileChange channel closed; dropping filesystem event");
    }
}

async fn handle_event(
    kind: &EventKind,
    path: &PathBuf,
    canon: &PathBuf,
    tx: &mpsc::Sender<FileChange>,
    tracked_paths: &DashSet<PathBuf>,
    watch_dirs: &[PathBuf],
) {
    match kind {
        EventKind::Modify(_) => {
            if tracked_paths.contains(canon) {
                if !path.exists() {
                    // Some kernels/filesystems emit Modify instead of Remove
                    // when a file is deleted — treat it as a deletion.
                    tracked_paths.remove(canon);
                    send_change(tx, FileChange::Deleted(canon.clone())).await;
                } else {
                    send_change(tx, FileChange::Modified(canon.clone())).await;
                }
            }
        }
        EventKind::Remove(_) => {
            // When the file is already gone, canonicalize() falls back to the
            // raw path, which may differ from the canonical key stored at
            // setup time.  Try both forms so the entry is always cleaned up.
            let found = if tracked_paths.remove(canon).is_some() {
                Some(canon.clone())
            } else if canon != path && tracked_paths.remove(path).is_some() {
                Some(path.clone())
            } else {
                None
            };
            if let Some(removed) = found {
                send_change(tx, FileChange::Deleted(removed)).await;
            }
        }
        EventKind::Create(_) => {
            // If a tracked path was re-created (e.g. atomically replaced),
            // treat it as a modification so the reloader refreshes the source.
            if tracked_paths.contains(canon) {
                send_change(tx, FileChange::Modified(canon.clone())).await;
            }
            // Genuinely new files are loaded after Close(Write) below.
        }
        // Load new files after the write is complete and the file descriptor
        // closed, avoiding attempts to open a partially-written file.
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => {
            if tracked_paths.contains(canon) {
                return;
            }
            let in_watched = watch_dirs
                .iter()
                .any(|dir| canon.parent().is_some_and(|p| p == dir));
            if in_watched {
                // Track the new path so future Modify events are reported.
                tracked_paths.insert(canon.clone());
                send_change(tx, FileChange::New(canon.clone())).await;
            }
        }
        _ => {}
    }
}
