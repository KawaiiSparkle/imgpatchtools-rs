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
const BAR_TEMPLATE: &str =
    "{prefix:>12.cyan.bold} [{bar:40.green/dark_gray}] \
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

    /// Create a console progress reporter backed by a hidden (non-drawing)
    /// bar. Useful for unit tests that exercise the trait without producing
    /// terminal output.
    #[cfg(test)]
    fn hidden() -> Result<Self> {
        Self::with_bar(ProgressBar::hidden())
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

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SilentProgress ---------------------------------------------------

    #[test]
    fn silent_default_state() {
        let sp = SilentProgress::new();
        assert_eq!(sp.total(), 0);
        assert_eq!(sp.position(), 0);
    }

    #[test]
    fn silent_set_total() {
        let mut sp = SilentProgress::new();
        sp.set_total(100);
        assert_eq!(sp.total(), 100);
        assert_eq!(sp.position(), 0);
    }

    #[test]
    fn silent_advance() {
        let mut sp = SilentProgress::new();
        sp.set_total(100);
        sp.advance(30);
        assert_eq!(sp.position(), 30);
        sp.advance(20);
        assert_eq!(sp.position(), 50);
    }

    #[test]
    fn silent_advance_saturates() {
        let mut sp = SilentProgress::new();
        sp.set_total(10);
        sp.advance(u64::MAX);
        // saturating_add prevents overflow
        assert_eq!(sp.position(), u64::MAX);
    }

    #[test]
    fn silent_set_stage_is_noop() {
        let mut sp = SilentProgress::new();
        // Should not panic or produce any side effect.
        sp.set_stage("move");
        sp.set_stage("new");
        sp.set_stage("");
        assert_eq!(sp.position(), 0);
    }

    #[test]
    fn silent_finish() {
        let mut sp = SilentProgress::new();
        sp.set_total(100);
        sp.advance(50);
        sp.finish();
        assert_eq!(sp.position(), sp.total());
    }

    #[test]
    fn silent_default_trait() {
        let sp = SilentProgress::default();
        assert_eq!(sp.total(), 0);
    }

    // ---- ConsoleProgress (hidden bar) ------------------------------------

    #[test]
    fn console_hidden_new() {
        let cp = ConsoleProgress::hidden();
        assert!(cp.is_ok());
    }

    #[test]
    fn console_hidden_set_total() {
        let mut cp = ConsoleProgress::hidden().unwrap();
        cp.set_total(500);
        // No panic, no visible output.
    }

    #[test]
    fn console_hidden_advance() {
        let mut cp = ConsoleProgress::hidden().unwrap();
        cp.set_total(100);
        cp.advance(10);
        cp.advance(20);
        // The hidden bar tracks position internally.
    }

    #[test]
    fn console_hidden_set_stage() {
        let mut cp = ConsoleProgress::hidden().unwrap();
        cp.set_stage("zero");
        cp.set_stage("move");
        cp.set_stage("bsdiff");
    }

    #[test]
    fn console_hidden_finish() {
        let mut cp = ConsoleProgress::hidden().unwrap();
        cp.set_total(100);
        cp.advance(100);
        cp.finish();
    }

    #[test]
    fn console_hidden_full_lifecycle() {
        let mut cp = ConsoleProgress::hidden().unwrap();
        cp.set_total(1000);
        cp.set_stage("new");
        for _ in 0..10 {
            cp.advance(100);
        }
        cp.set_stage("done");
        cp.finish();
    }

    // ---- Factory ----------------------------------------------------------

    #[test]
    fn factory_verbose_returns_console() {
        let mut p = new_progress(true);
        // Should be a ConsoleProgress (or SilentProgress fallback).
        // Either way, the trait methods must not panic.
        p.set_total(50);
        p.set_stage("test");
        p.advance(10);
        p.finish();
    }

    #[test]
    fn factory_quiet_returns_silent() {
        let mut p = new_progress(false);
        p.set_total(50);
        p.set_stage("test");
        p.advance(50);
        p.finish();
    }

    // ---- Trait object usage -----------------------------------------------

    #[test]
    fn trait_object_dispatch() {
        let reporters: Vec<Box<dyn ProgressReporter>> = vec![
            new_progress(true),
            new_progress(false),
        ];
        for mut r in reporters {
            r.set_total(100);
            r.set_stage("work");
            r.advance(50);
            r.advance(50);
            r.finish();
        }
    }

    // ---- Send bound -------------------------------------------------------

    #[test]
    fn reporter_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<ConsoleProgress>();
        assert_send::<SilentProgress>();
        assert_send::<Box<dyn ProgressReporter>>();
    }
}