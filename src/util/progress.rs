//! Progress-bar reporting — thin facade over [`indicatif`].
//!
//! Provides a [`ProgressReporter`] trait that abstracts progress reporting so
//! that callers (block-image engine, apply-patch, etc.) are decoupled from
//! terminal rendering details.
//!
//! Two concrete implementations are offered:
//!
//! | Type                | Behaviour                                      |
//! |---------------------|-------------------------------------------------|
//! | [`ConsoleProgress`] | Renders a live progress bar to stderr.          |
//! | [`SilentProgress`]  | Discards all progress events (for batch / CI).  |
//!
//! The factory function [`new_progress`] selects the appropriate
//! implementation based on a `verbose` flag.

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// An observer that receives progress updates from a long-running operation.
///
/// All methods take `&mut self` to allow implementations that track internal
/// counters without interior mutability. The `Send` bound is required so that
/// the reporter can be passed to worker threads (e.g. via `rayon`).
pub trait ProgressReporter: Send {
    /// Set the total number of work units (e.g. blocks to process).
    ///
    /// May be called more than once if the total is revised (e.g. after
    /// parsing a transfer list).
    fn set_total(&mut self, total: u64);

    /// Record that `delta` additional work units have been completed.
    fn advance(&mut self, delta: u64);

    /// Update the human-readable stage label (e.g. `"move"`, `"new"`).
    fn set_stage(&mut self, msg: &str);

    /// Mark the entire operation as finished and clean up the display.
    fn finish(&mut self);
}

// ---------------------------------------------------------------------------
// ConsoleProgress
// ---------------------------------------------------------------------------

/// Progress bar template used for block-based operations.
///
/// Layout:  `stage [████████░░░░] 1234/5678 blocks (23%) 456.7 blocks/s ETA 00:12`
const BAR_TEMPLATE: &str = "{prefix:>12.cyan.bold} [{bar:40.green/dark_gray}] \
     {pos}/{len} blocks ({percent}%) {per_sec} ETA {eta}";

/// Progress bar characters: filled, current, empty.
const BAR_CHARS: &str = "█▓░";

/// A progress reporter that renders a live progress bar to stderr using
/// [`indicatif`].
///
/// Displays: stage prefix, animated bar, position / total, percentage,
/// throughput (blocks/s), and estimated time remaining.
pub struct ConsoleProgress {
    bar: ProgressBar,
}

impl ConsoleProgress {
    /// Create a new console progress reporter.
    ///
    /// The bar is drawn to stderr and initially has zero length; call
    /// [`ProgressReporter::set_total`] to set the actual total before
    /// advancing.
    pub fn new() -> Result<Self> {
        Self::with_bar(ProgressBar::new(0))
    }

    /// Internal constructor accepting an arbitrary [`ProgressBar`] instance
    /// (used by tests to inject a hidden bar).
    fn with_bar(bar: ProgressBar) -> Result<Self> {
        let style = ProgressStyle::with_template(BAR_TEMPLATE)
            .context("invalid progress bar template")?
            .progress_chars(BAR_CHARS);
        bar.set_style(style);
        Ok(Self { bar })
    }
}

impl ProgressReporter for ConsoleProgress {
    fn set_total(&mut self, total: u64) {
        self.bar.set_length(total);
    }

    fn advance(&mut self, delta: u64) {
        self.bar.inc(delta);
    }

    fn set_stage(&mut self, msg: &str) {
        self.bar.set_prefix(msg.to_string());
    }

    fn finish(&mut self) {
        self.bar.finish_and_clear();
    }
}

// ---------------------------------------------------------------------------
// SilentProgress
// ---------------------------------------------------------------------------

/// A no-op progress reporter that discards all events.
///
/// Used when progress output is not desired (e.g. piped output, CI
/// environments, or when the user did not request verbose mode).
pub struct SilentProgress {
    total: u64,
    position: u64,
}

impl SilentProgress {
    /// Create a new silent progress reporter.
    pub fn new() -> Self {
        Self {
            total: 0,
            position: 0,
        }
    }

    /// Current position (for testing / introspection).
    #[cfg(test)]
    fn position(&self) -> u64 {
        self.position
    }

    /// Current total (for testing / introspection).
    #[cfg(test)]
    fn total(&self) -> u64 {
        self.total
    }
}

impl Default for SilentProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressReporter for SilentProgress {
    fn set_total(&mut self, total: u64) {
        self.total = total;
    }

    fn advance(&mut self, delta: u64) {
        self.position = self.position.saturating_add(delta);
    }

    fn set_stage(&mut self, _msg: &str) {
        // intentionally empty
    }

    fn finish(&mut self) {
        self.position = self.total;
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Create a boxed [`ProgressReporter`] appropriate for the current context.
///
/// * `verbose = true`  → [`ConsoleProgress`] (live terminal bar).
/// * `verbose = false` → [`SilentProgress`] (no output).
///
/// If creating the console reporter fails (e.g. template error), falls back
/// to [`SilentProgress`] and logs a warning.
pub fn new_progress(verbose: bool) -> Box<dyn ProgressReporter> {
    if verbose {
        match ConsoleProgress::new() {
            Ok(cp) => Box::new(cp),
            Err(e) => {
                log::warn!("failed to create console progress bar: {e:#}; falling back to silent");
                Box::new(SilentProgress::new())
            }
        }
    } else {
        Box::new(SilentProgress::new())
    }
}
