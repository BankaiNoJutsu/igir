use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io::{stderr, IsTerminal};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::config::Config;
use crate::types::Action;

const ACTION_BAR_TEMPLATE: &str = "{prefix} [{bar:40}] {pos:>5}/{len:<5} | {percent:>3}% | {elapsed_precise}<{eta_precise} | {msg}";
const SPINNER_TEMPLATE: &str = "{prefix} {spinner} {elapsed_precise} | {msg}";
const DETAIL_BAR_TEMPLATE: &str = "{prefix} {spinner} {elapsed_precise}\n{msg}";

fn ellipsize(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut shortened = String::new();
    for ch in input.chars().take(keep) {
        shortened.push(ch);
    }
    shortened.push_str("...");
    shortened
}

fn file_hint(path: Option<&Path>) -> Option<String> {
    path.and_then(|p| p.file_name())
        .and_then(|os| os.to_str())
        .map(|name| ellipsize(name, 40))
}

fn action_label(action: &Action) -> String {
    match action {
        Action::Copy => "COPY",
        Action::Move => "MOVE",
        Action::Link => "LINK",
        Action::Extract => "EXTRACT",
        Action::Zip => "ZIP",
        Action::Playlist => "PLAYLIST",
        Action::Test => "TEST",
        Action::Dir2dat => "DIR2DAT",
        Action::Fixdat => "FIXDAT",
        Action::Clean => "CLEAN",
        Action::Report => "REPORT",
    }
    .to_string()
}

fn format_byte_progress(done: u64, total: Option<u64>) -> String {
    match total {
        Some(limit) if limit > 0 => {
            format!("{} / {}", HumanBytes(done), HumanBytes(limit))
        }
        _ => HumanBytes(done).to_string(),
    }
}

fn format_rate(bytes_per_second: f64) -> String {
    const UNITS: [(&str, f64); 5] = [
        ("B/s", 1.0),
        ("KiB/s", 1024.0),
        ("MiB/s", 1024.0 * 1024.0),
        ("GiB/s", 1024.0 * 1024.0 * 1024.0),
        ("TiB/s", 1024.0 * 1024.0 * 1024.0 * 1024.0),
    ];
    let mut value = bytes_per_second;
    let mut unit = "B/s";
    for (label, threshold) in UNITS.iter() {
        if bytes_per_second >= *threshold {
            value = bytes_per_second / *threshold;
            unit = label;
        } else {
            break;
        }
    }
    format!("{value:.2} {unit}")
}

fn format_speed(bytes: u64, elapsed: Duration) -> Option<String> {
    let seconds = elapsed.as_secs_f64();
    if seconds <= 0.0 || bytes == 0 {
        return None;
    }
    let per_second = bytes as f64 / seconds;
    Some(format_rate(per_second))
}

fn format_duration_short(d: Duration) -> String {
    if d.as_secs_f64() >= 1.0 {
        return format!("{:.2}s", d.as_secs_f64());
    }
    if d.as_millis() > 0 {
        return format!("{} ms", d.as_millis());
    }
    format!("{} µs", d.as_micros())
}

#[cfg(test)]
thread_local! {
    static FORCE_PROGRESS_TTY: Cell<Option<bool>> = Cell::new(None);
}

fn stderr_supports_progress() -> bool {
    #[cfg(test)]
    {
        if let Some(flag) = FORCE_PROGRESS_TTY.with(|cell| cell.get()) {
            return flag;
        }
    }
    stderr().is_terminal()
}

#[derive(Copy, Clone)]
enum DetailSection {
    Scan,
    Dat,
    Action,
}

#[derive(Default)]
struct DetailPanelState {
    scan: Option<String>,
    dat: Option<String>,
    action: Option<String>,
}

impl DetailPanelState {
    fn set(&mut self, section: DetailSection, value: Option<String>) {
        match section {
            DetailSection::Scan => self.scan = value,
            DetailSection::Dat => self.dat = value,
            DetailSection::Action => self.action = value,
        }
    }

    fn render(&self) -> String {
        let mut lines = Vec::new();
        if let Some(ref scan) = self.scan {
            lines.push(format!("Scan     | {scan}"));
        }
        if let Some(ref dat) = self.dat {
            lines.push(format!("DAT      | {dat}"));
        }
        if let Some(ref action) = self.action {
            lines.push(format!("Action   | {action}"));
        }
        if lines.is_empty() {
            lines.push("Idle".to_string());
        }
        lines.join("\n")
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub enum BackgroundTask {
    Checksums,
    Cache,
    NetLookup,
    Diag,
}

impl BackgroundTask {
    fn prefix(&self) -> &'static str {
        match self {
            BackgroundTask::Checksums => "[HASH]",
            BackgroundTask::Cache => "[CACHE]",
            BackgroundTask::NetLookup => "[NET ]",
            BackgroundTask::Diag => "[DIAG]",
        }
    }

    fn noun(&self) -> &'static str {
        match self {
            BackgroundTask::Checksums => "files",
            BackgroundTask::Cache => "entries",
            BackgroundTask::NetLookup => "requests",
            BackgroundTask::Diag => "queued",
        }
    }

    fn is_metered(&self) -> bool {
        matches!(self, BackgroundTask::Checksums)
    }
}

#[derive(Clone, Debug)]
pub struct ProgressEvent {
    path: PathBuf,
    bytes_done: u64,
    total_bytes: Option<u64>,
}

impl ProgressEvent {
    pub fn hashing(path: PathBuf, bytes_done: u64, total_bytes: Option<u64>) -> Self {
        Self {
            path,
            bytes_done,
            total_bytes,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn bytes_done(&self) -> u64 {
        self.bytes_done
    }

    pub fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }
}

#[derive(Default)]
struct ItemBytesState {
    last_reported: u64,
    total: Option<u64>,
}

struct BackgroundTaskState {
    task: BackgroundTask,
    bar: ProgressBar,
    count: usize,
    total: Option<usize>,
    bytes_done: u64,
    bytes_total: Option<u64>,
    last_hint: Option<String>,
    last_latency: Option<Duration>,
    avg_latency_secs: Option<f64>,
}

impl BackgroundTaskState {
    fn update_message(&self) -> String {
        let mut base = if let Some(total) = self.total {
            let capped = self.count.min(total);
            format!("{capped}/{total} {}", self.task.noun())
        } else {
            format!("{} {}", self.count, self.task.noun())
        };
        if self.bytes_done > 0 || self.bytes_total.is_some() {
            base.push_str(" | ");
            base.push_str(&format_byte_progress(self.bytes_done, self.bytes_total));
        }
        if let Some(hint) = &self.last_hint {
            base.push_str(&format!(" | last: {hint}"));
        }
        if let Some(latency) = self.last_latency {
            base.push_str(&format!(" | req {}", format_duration_short(latency)));
            if let Some(avg) = self.avg_latency_secs {
                let avg_duration = Duration::from_secs_f64(avg.max(0.0));
                base.push_str(&format!(" avg {}", format_duration_short(avg_duration)));
            }
        }
        base
    }
}

pub struct ProgressReporter {
    enabled: bool,
    multi: MultiProgress,
    scanning_bar: ProgressBar,
    detail_bar: ProgressBar,
    detail_panel: RefCell<DetailPanelState>,
    background_tasks: RefCell<HashMap<BackgroundTask, BackgroundTaskState>>,
    action_bar: RefCell<Option<ProgressBar>>,
    scan_finished: Cell<bool>,
    scan_total: Cell<Option<usize>>,
    scan_total_bytes: Cell<Option<u64>>,
    scan_started_at: RefCell<Option<Instant>>,
    scan_last_bytes: Cell<u64>,
    dat_bar: RefCell<Option<ProgressBar>>,
    dat_total: Cell<Option<usize>>,
    current_action_label: RefCell<Option<String>>,
    action_total: Cell<Option<usize>>,
    verbosity: u8,
    finalized: Cell<bool>,
    diag_phase_bars: RefCell<HashMap<String, ProgressBar>>,
    item_bytes: RefCell<HashMap<BackgroundTask, HashMap<PathBuf, ItemBytesState>>>,
    action_item_bytes: RefCell<HashMap<PathBuf, ItemBytesState>>,
}

impl ProgressReporter {
    fn format_message<F>(&self, base: &str, detail: F) -> String
    where
        F: FnOnce() -> String,
    {
        if self.verbosity == 0 {
            base.to_string()
        } else {
            detail()
        }
    }

    fn format_hint(&self, path: Option<&Path>) -> Option<String> {
        match self.verbosity {
            0 => None,
            1 => file_hint(path),
            2 => path.map(|p| {
                let display = p.to_string_lossy();
                ellipsize(&display, 80)
            }),
            _ => path.map(|p| p.to_string_lossy().to_string()),
        }
    }

    fn refresh_panel(&self) {
        if !self.enabled {
            return;
        }
        let panel = self.detail_panel.borrow();
        self.detail_bar.set_message(panel.render());
    }

    fn create_background_task_state(
        &self,
        task: BackgroundTask,
        total_hint: Option<usize>,
    ) -> BackgroundTaskState {
        let bar = if task.is_metered() && total_hint.unwrap_or(0) > 0 {
            let bar = self.multi.insert_before(
                &self.detail_bar,
                ProgressBar::new(total_hint.unwrap() as u64),
            );
            bar.set_style(
                ProgressStyle::with_template(ACTION_BAR_TEMPLATE)
                    .unwrap()
                    .progress_chars("=>-"),
            );
            bar
        } else {
            let spinner = self
                .multi
                .insert_before(&self.detail_bar, ProgressBar::new_spinner());
            spinner.set_style(
                ProgressStyle::with_template(SPINNER_TEMPLATE)
                    .unwrap()
                    .tick_strings(&["-", "\\", "|", "/"]),
            );
            spinner.enable_steady_tick(Duration::from_millis(140));
            spinner
        };
        bar.set_prefix(task.prefix().to_string());
        let state = BackgroundTaskState {
            task,
            bar,
            count: 0,
            total: total_hint.filter(|t| *t > 0),
            bytes_done: 0,
            bytes_total: None,
            last_hint: None,
            last_latency: None,
            avg_latency_secs: None,
        };
        state.bar.set_message(state.update_message());
        state
    }

    fn with_background_task<F>(&self, task: BackgroundTask, total_hint: Option<usize>, mut f: F)
    where
        F: FnMut(&mut BackgroundTaskState),
    {
        if !self.enabled {
            return;
        }
        let mut tasks = self.background_tasks.borrow_mut();
        let state = tasks
            .entry(task)
            .or_insert_with(|| self.create_background_task_state(task, total_hint));
        if let Some(total) = total_hint.filter(|t| *t > 0) {
            state.total = Some(total);
            state.bar.set_length(total as u64);
        }
        f(state);
    }

    pub fn hint_background_task_bytes(&self, task: BackgroundTask, total_bytes: Option<u64>) {
        if !self.enabled {
            return;
        }
        self.with_background_task(task, None, |state| {
            state.bytes_total = total_bytes;
            if let Some(limit) = total_bytes {
                if state.bytes_done > limit {
                    state.bytes_done = limit;
                }
            }
            state.bar.set_message(state.update_message());
        });
    }

    fn add_background_task_bytes(&self, task: BackgroundTask, delta: u64) {
        if !self.enabled || delta == 0 {
            return;
        }
        self.with_background_task(task, None, |state| {
            state.bytes_done = state.bytes_done.saturating_add(delta);
            if let Some(limit) = state.bytes_total {
                if state.bytes_done > limit {
                    state.bytes_done = limit;
                }
            }
            state.bar.set_message(state.update_message());
        });
    }

    fn note_item_bytes(
        &self,
        task: BackgroundTask,
        path: &Path,
        bytes_done: u64,
        total: Option<u64>,
    ) {
        if !self.enabled {
            return;
        }
        let (delta, total_hint, should_finish) = {
            let mut per_task = self.item_bytes.borrow_mut();
            let task_map = per_task.entry(task).or_default();
            let entry = task_map
                .entry(path.to_path_buf())
                .or_insert_with(|| ItemBytesState {
                    last_reported: 0,
                    total,
                });
            if entry.total.is_none() {
                entry.total = total;
            }
            let delta = bytes_done.saturating_sub(entry.last_reported);
            entry.last_reported = bytes_done;
            let total_hint = entry.total;
            let should_finish = total_hint.map(|limit| bytes_done >= limit).unwrap_or(false);
            (delta, total_hint, should_finish)
        };
        let _ = total_hint;
        if delta > 0 {
            self.add_background_task_bytes(task, delta);
        }
        if should_finish {
            self.finish_background_item(task, path);
        }
    }

    pub fn finish_background_item(&self, task: BackgroundTask, path: &Path) {
        if !self.enabled {
            return;
        }
        let mut per_task = self.item_bytes.borrow_mut();
        if let Some(task_map) = per_task.get_mut(&task) {
            task_map.remove(path);
            if task_map.is_empty() {
                per_task.remove(&task);
            }
        }
    }

    pub fn handle_event(&self, event: ProgressEvent) {
        if !self.enabled {
            return;
        }
        self.update_checksums_progress(&event.path, event.bytes_done, event.total_bytes);
    }

    pub fn hint_background_task_total(&self, task: BackgroundTask, total: Option<usize>) {
        if !self.enabled {
            return;
        }
        if total.is_none() {
            return;
        }
        let total_value = total.unwrap();
        self.with_background_task(task, total, |state| {
            state.count = state.count.min(total_value);
            state.bar.set_position(state.count as u64);
            state.bar.set_message(state.update_message());
        });
    }

    pub fn tick_background_task(&self, task: BackgroundTask, amount: usize, hint: Option<&Path>) {
        if !self.enabled {
            return;
        }
        let hint_text = hint.and_then(|h| self.format_hint(Some(h)));
        self.with_background_task(task, None, |state| {
            state.count = state.count.saturating_add(amount);
            if let Some(total) = state.total {
                let capped = state.count.min(total);
                state.bar.set_position(capped as u64);
            }
            if let Some(ref text) = hint_text {
                state.last_hint = Some(text.clone());
            }
            state.bar.set_message(state.update_message());
        });
    }

    /// Update the Checksums background task with byte-level progress for the
    /// currently-processing file. This updates the task's `last_hint` message
    /// to include human-readable byte progress (e.g., "game.bin 4 MiB / 32 MiB").
    pub fn update_checksums_progress(&self, path: &Path, bytes_done: u64, total: Option<u64>) {
        if !self.enabled {
            return;
        }
        self.note_item_bytes(BackgroundTask::Checksums, path, bytes_done, total);
        let hint_text = match self.verbosity {
            0 => None,
            1 => file_hint(Some(path)),
            2 => Some(format!(
                "{} | {}",
                ellipsize(&path.to_string_lossy(), 80),
                format_byte_progress(bytes_done, total)
            )),
            _ => Some(format!(
                "{} | {}",
                path.to_string_lossy(),
                format_byte_progress(bytes_done, total)
            )),
        };

        self.with_background_task(BackgroundTask::Checksums, None, |state| {
            if let Some(ref text) = hint_text {
                state.last_hint = Some(text.clone());
            } else {
                state.last_hint = None;
            }
            state.bar.set_message(state.update_message());
        });
    }

    pub fn update_action_item_bytes(&self, path: &Path, bytes_done: u64, total: Option<u64>) {
        if !self.enabled {
            return;
        }
        let mut items = self.action_item_bytes.borrow_mut();
        let entry = items
            .entry(path.to_path_buf())
            .or_insert_with(|| ItemBytesState {
                last_reported: 0,
                total,
            });
        entry.last_reported = bytes_done;
        if entry.total.is_none() {
            entry.total = total;
        }
        let total_hint = entry.total.or(total);
        drop(items);
        let bytes_fragment = format_byte_progress(bytes_done, total_hint);
        let detail = if let Some(hint) = self.format_hint(Some(path)) {
            format!("{hint} | {bytes_fragment}")
        } else {
            bytes_fragment
        };
        if let Some(bar) = self.action_bar.borrow().as_ref() {
            bar.set_message(self.format_message("Working...", || detail.clone()));
        }
        self.set_panel_section(DetailSection::Action, detail, false);
    }

    pub fn finish_action_item(&self, path: &Path) {
        if !self.enabled {
            return;
        }
        self.action_item_bytes.borrow_mut().remove(path);
    }

    /// Update the diagnostics background task with a small, human-friendly
    /// throughput message. This is intended to be called periodically (e.g.,
    /// every 2s) from the main thread so it can safely call into the
    /// `ProgressReporter` API.
    pub fn update_diag(
        &self,
        queued: usize,
        in_flight: usize,
        files_per_sec: f64,
        mib_per_sec: f64,
    ) {
        if !self.enabled {
            return;
        }
        let msg = format!(
            "queued={} in_flight={} files/s={:.1} MiB/s={:.2}",
            queued, in_flight, files_per_sec, mib_per_sec
        );
        self.with_background_task(BackgroundTask::Diag, None, |state| {
            state.count = queued;
            state.last_hint = Some(msg.clone());
            state.bar.set_message(state.update_message());
        });
    }

    pub fn finish_background_task(&self, task: BackgroundTask) {
        if !self.enabled {
            return;
        }
        if let Some(state) = self.background_tasks.borrow_mut().remove(&task) {
            state.bar.finish_and_clear();
            self.multi.remove(&state.bar);
        }
        self.item_bytes.borrow_mut().remove(&task);
    }

    pub fn record_background_task_latency(&self, task: BackgroundTask, duration: Duration) {
        if !self.enabled {
            return;
        }
        self.with_background_task(task, None, |state| {
            state.last_latency = Some(duration);
            let seconds = duration.as_secs_f64();
            state.avg_latency_secs = Some(match state.avg_latency_secs {
                Some(avg) => avg * 0.7 + seconds * 0.3,
                None => seconds,
            });
            state.bar.set_message(state.update_message());
        });
    }

    fn set_panel_section(
        &self,
        section: DetailSection,
        message: impl Into<String>,
        reset_elapsed: bool,
    ) {
        if !self.enabled {
            return;
        }
        if reset_elapsed {
            self.detail_bar.reset_elapsed();
        }
        self.detail_panel
            .borrow_mut()
            .set(section, Some(message.into()));
        self.refresh_panel();
    }

    fn update_action_panel(&self, message: impl Into<String>, reset_elapsed: bool) {
        let label = {
            let borrowed = self.current_action_label.borrow();
            borrowed.clone().unwrap_or_else(|| "ACTION".to_string())
        };
        let text = format!("{label} - {}", message.into());
        self.set_panel_section(DetailSection::Action, text, reset_elapsed);
    }

    fn log_summary(&self, message: String) {
        if !self.enabled {
            return;
        }
        let _ = self.multi.println(message);
    }

    pub fn log_diag(&self, message: impl Into<String>) {
        if !self.enabled {
            return;
        }
        let message = message.into();
        self.with_background_task(BackgroundTask::Diag, None, |state| {
            state.last_hint = Some(message.clone());
            state.bar.set_message(state.update_message());
        });
    }

    #[cfg(test)]
    pub(crate) fn diag_last_hint_for_tests(&self) -> Option<String> {
        self.background_tasks
            .borrow()
            .get(&BackgroundTask::Diag)
            .and_then(|state| state.last_hint.clone())
    }

    pub fn maybe_new(config: &Config) -> Option<Self> {
        if config.quiet > 0 {
            return None;
        }
        if !stderr_supports_progress() {
            return None;
        }

        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(15));

        let scanning_bar = multi.add(ProgressBar::new_spinner());
        scanning_bar.set_style(
            ProgressStyle::with_template(SPINNER_TEMPLATE)
                .unwrap()
                .tick_strings(&["-", "\\", "|", "/"]),
        );
        scanning_bar.set_prefix("[SCAN]");
        scanning_bar.set_message("Waiting to start...");
        scanning_bar.enable_steady_tick(Duration::from_millis(100));

        let detail_bar = multi.add(ProgressBar::new_spinner());
        detail_bar.set_style(
            ProgressStyle::with_template(DETAIL_BAR_TEMPLATE)
                .unwrap()
                .tick_strings(&["-", "\\", "|", "/"]),
        );
        detail_bar.set_prefix("[STATUS]");
        let detail_panel = DetailPanelState::default();
        let initial_panel = detail_panel.render();
        detail_bar.set_message(initial_panel);
        detail_bar.enable_steady_tick(Duration::from_millis(120));

        Some(Self {
            enabled: true,
            multi,
            scanning_bar,
            detail_bar,
            detail_panel: RefCell::new(detail_panel),
            background_tasks: RefCell::new(HashMap::new()),
            action_bar: RefCell::new(None),
            diag_phase_bars: RefCell::new(HashMap::new()),
            scan_finished: Cell::new(false),
            scan_total: Cell::new(None),
            scan_total_bytes: Cell::new(None),
            scan_started_at: RefCell::new(None),
            scan_last_bytes: Cell::new(0),
            dat_bar: RefCell::new(None),
            dat_total: Cell::new(None),
            current_action_label: RefCell::new(None),
            action_total: Cell::new(None),
            verbosity: config.verbose,
            finalized: Cell::new(false),
            item_bytes: RefCell::new(HashMap::new()),
            action_item_bytes: RefCell::new(HashMap::new()),
        })
    }

    pub fn begin_scanning(
        &self,
        inputs: usize,
        total_files: Option<usize>,
        total_bytes: Option<u64>,
    ) {
        if !self.enabled {
            return;
        }
        self.scan_total.set(total_files);
        self.scan_total_bytes.set(total_bytes);
        self.scan_started_at.replace(Some(Instant::now()));
        self.scan_last_bytes.set(0);

        if let Some(total) = total_files {
            self.scanning_bar.disable_steady_tick();
            self.scanning_bar.set_style(
                ProgressStyle::with_template(ACTION_BAR_TEMPLATE)
                    .unwrap()
                    .progress_chars("=>-"),
            );
            self.scanning_bar.set_length(total as u64);
            self.scanning_bar.set_position(0);
            let input_noun = if inputs == 1 { "input" } else { "inputs" };
            let file_noun = if total == 1 { "file" } else { "files" };
            let bytes_fragment = format_byte_progress(0, total_bytes);
            self.scanning_bar
                .set_message(self.format_message("Scanning inputs", || {
                    format!(
                        "Scanning {inputs} {input_noun} - 0/{total} {file_noun} | {bytes_fragment}"
                    )
                }));
            let detail_message = self.format_message("Scanning inputs", || {
                format!("{inputs} {input_noun} | 0/{total} {file_noun} | {bytes_fragment}")
            });
            self.set_panel_section(DetailSection::Scan, detail_message, true);
        } else {
            let initial_bytes = format_byte_progress(0, total_bytes);
            self.scanning_bar
                .set_message(self.format_message("Scanning inputs", || {
                    if inputs == 0 {
                        format!("Scanning inputs - waiting for files | {initial_bytes}")
                    } else {
                        let noun = if inputs == 1 { "input" } else { "inputs" };
                        format!("Scanning {inputs} {noun}... | {initial_bytes}")
                    }
                }));
            let detail_message = self.format_message("Scanning inputs", || {
                if inputs == 0 {
                    format!("Waiting for files | {initial_bytes}")
                } else {
                    let noun = if inputs == 1 { "input" } else { "inputs" };
                    format!("{inputs} {noun} | {initial_bytes}")
                }
            });
            self.set_panel_section(DetailSection::Scan, detail_message, true);
        }
    }

    pub fn scanning_tick(&self, total_indexed: usize, bytes_indexed: u64, hint: Option<&Path>) {
        if !self.enabled {
            return;
        }
        self.scan_last_bytes.set(bytes_indexed);
        let hint_text = self.format_hint(hint);
        let total_bytes = self.scan_total_bytes.get();
        if let Some(total) = self.scan_total.get() {
            let completed = total_indexed.min(total);
            self.scanning_bar.set_position(completed as u64);
            let bytes_fragment = format_byte_progress(bytes_indexed, total_bytes);
            self.scanning_bar
                .set_message(self.format_message("Scanning inputs", || {
                    let mut message = format!(
                        "Scanning inputs - {completed}/{total} file{} | {bytes_fragment}",
                        if total == 1 { "" } else { "s" }
                    );
                    if let Some(name) = hint_text.as_ref() {
                        message.push_str(&format!(" (latest: {name})"));
                    }
                    message
                }));
            let detail_message = match hint_text.as_ref() {
                Some(name) => format!("Latest file: {name} | {bytes_fragment}"),
                None => format!("Indexed {completed}/{total} files | {bytes_fragment}"),
            };
            self.set_panel_section(DetailSection::Scan, detail_message, false);
        } else {
            let bytes_fragment = format_byte_progress(bytes_indexed, total_bytes);
            self.scanning_bar
                .set_message(self.format_message("Scanning inputs", || {
                    let mut message = format!(
                        "Scanning inputs - {} file{} indexed | {bytes_fragment}",
                        total_indexed,
                        if total_indexed == 1 { "" } else { "s" }
                    );
                    if let Some(name) = hint_text.as_ref() {
                        message.push_str(&format!(" (latest: {name})"));
                    }
                    message
                }));
            let detail_message = match hint_text.as_ref() {
                Some(name) => format!("Latest file: {name} | {bytes_fragment}"),
                None => format!("Indexed {total_indexed} files | {bytes_fragment}"),
            };
            self.set_panel_section(DetailSection::Scan, detail_message, false);
        }
    }

    pub fn finish_scanning(&self, total_indexed: usize) {
        if !self.enabled || self.scan_finished.get() {
            return;
        }
        self.scan_finished.set(true);
        let mut summary = if let Some(total) = self.scan_total.get() {
            let completed = total_indexed.min(total);
            self.scanning_bar.set_position(completed as u64);
            format!(
                "Scanned {completed}/{total} file{}",
                if total == 1 { "" } else { "s" }
            )
        } else {
            format!(
                "Scanned {total_indexed} file{}",
                if total_indexed == 1 { "" } else { "s" }
            )
        };
        if let Some(start) = self.scan_started_at.borrow_mut().take() {
            if let Some(speed) = format_speed(self.scan_last_bytes.get(), start.elapsed()) {
                summary.push_str(&format!(" | {speed}"));
            }
        }
        self.scan_total.set(None);
        self.log_summary(summary.clone());
        self.scanning_bar.finish_and_clear();
        self.multi.remove(&self.scanning_bar);
        self.set_panel_section(DetailSection::Scan, summary, true);
        self.finish_background_task(BackgroundTask::Checksums);
    }

    pub fn begin_dat_loading(&self, total: usize) {
        if !self.enabled || total == 0 {
            return;
        }
        if let Some(existing) = self.dat_bar.borrow_mut().take() {
            existing.finish_and_clear();
            self.multi.remove(&existing);
        }
        let bar = self
            .multi
            .insert_before(&self.detail_bar, ProgressBar::new(total as u64));
        bar.set_style(
            ProgressStyle::with_template(ACTION_BAR_TEMPLATE)
                .unwrap()
                .progress_chars("=>-"),
        );
        bar.set_prefix("[DAT ]");
        bar.set_message(
            self.format_message("Loading DATs", || format!("Loading DATs - 0/{total}")),
        );
        self.dat_total.set(Some(total));
        self.dat_bar.replace(Some(bar));
        let detail_message = self.format_message("Loading DATs", || format!("0/{total} processed"));
        self.set_panel_section(DetailSection::Dat, detail_message, true);
    }

    pub fn advance_dat_loading(&self, completed: usize, current: Option<&Path>) {
        if !self.enabled {
            return;
        }
        let Some(total) = self.dat_total.get() else {
            return;
        };
        if let Some(bar) = self.dat_bar.borrow().as_ref() {
            let capped = completed.min(total);
            bar.set_position(capped as u64);
            let hint_text = self.format_hint(current);
            bar.set_message(self.format_message("Loading DATs", || {
                let mut message = format!("Loading DATs - {capped}/{total}");
                if let Some(name) = hint_text.as_ref() {
                    message.push_str(&format!(" ({name})"));
                }
                message
            }));
            let detail_text = match hint_text.as_ref() {
                Some(name) => format!("{capped}/{total} | latest: {name}"),
                None => format!("{capped}/{total} processed"),
            };
            self.set_panel_section(DetailSection::Dat, detail_text, false);
        }
    }

    pub fn finish_dat_loading(&self, completed: usize) {
        if !self.enabled {
            return;
        }
        let total = self.dat_total.get().unwrap_or(completed);
        if let Some(bar) = self.dat_bar.borrow_mut().take() {
            let capped = completed.min(total);
            let summary = format!(
                "Loaded {capped}/{total} DAT{}",
                if total == 1 { "" } else { "s" }
            );
            bar.finish_and_clear();
            self.multi.remove(&bar);
            self.log_summary(summary.clone());
            self.set_panel_section(DetailSection::Dat, summary, true);
        }
        self.dat_total.set(None);
    }

    pub fn begin_action(&self, action: &Action, total: usize) {
        if !self.enabled {
            return;
        }
        if let Some(existing) = self.action_bar.borrow_mut().take() {
            existing.finish_and_clear();
            self.multi.remove(&existing);
        }
        let length = if total == 0 { 1 } else { total as u64 };
        let action_name = action_label(action);
        let action_prefix = format!("[{action_name}]");
        let bar = self
            .multi
            .insert_before(&self.detail_bar, ProgressBar::new(length));
        bar.set_style(
            ProgressStyle::with_template(ACTION_BAR_TEMPLATE)
                .unwrap()
                .progress_chars("=>-"),
        );
        bar.set_prefix(action_prefix.clone());
        bar.set_message(self.format_message("Working...", || "Preparing...".to_string()));
        self.action_bar.replace(Some(bar));
        self.current_action_label.replace(Some(action_name));
        self.action_total
            .set(if total == 0 { None } else { Some(total) });
        let message = self.format_message("Working...", || "Preparing...".to_string());
        self.update_action_panel(message, true);
    }

    pub fn advance_action(&self, completed: usize, hint: Option<&Path>) {
        if !self.enabled {
            return;
        }
        if let Some(bar) = self.action_bar.borrow().as_ref() {
            let total_opt = self.action_total.get();
            let capped = total_opt
                .map(|limit| completed.min(limit))
                .unwrap_or(completed);
            bar.set_position(capped as u64);
            let hint_text = self.format_hint(hint);
            let progress_fragment = if let Some(total) = total_opt {
                format!("{capped}/{total} done")
            } else {
                format!("{capped} done")
            };
            let panel_detail = match hint_text.as_ref() {
                Some(name) => format!("{progress_fragment} | latest: {name}"),
                None => progress_fragment,
            };
            bar.set_message(self.format_message("Working...", || panel_detail.clone()));
            self.update_action_panel(panel_detail, false);
        }
    }

    pub fn finish_action(&self, action: &Action) {
        if !self.enabled {
            return;
        }
        if let Some(bar) = self.action_bar.borrow_mut().take() {
            let action_name = {
                let borrowed = self.current_action_label.borrow();
                borrowed.clone().unwrap_or_else(|| action_label(action))
            };
            let summary = format!("{action_name} complete");
            bar.finish_and_clear();
            self.multi.remove(&bar);
            self.log_summary(summary.clone());
            self.update_action_panel(summary, true);
            self.current_action_label.replace(None);
            self.action_total.set(None);
        }
        self.action_item_bytes.borrow_mut().clear();
    }

    pub fn begin_diag_phase(&self, name: &str) {
        if !self.enabled {
            return;
        }
        let mut phases = self.diag_phase_bars.borrow_mut();
        if phases.contains_key(name) {
            return;
        }
        let bar = self
            .multi
            .insert_before(&self.detail_bar, ProgressBar::new_spinner());
        bar.set_style(
            ProgressStyle::with_template(SPINNER_TEMPLATE)
                .unwrap()
                .tick_strings(&["-", "\\", "|", "/"]),
        );
        bar.set_prefix("[PHASE]".to_string());
        bar.set_message(format!("{name}..."));
        bar.enable_steady_tick(Duration::from_millis(120));
        phases.insert(name.to_string(), bar);
    }

    pub fn finish_diag_phase(&self, name: &str, summary: Option<String>) {
        if !self.enabled {
            return;
        }
        if let Some(bar) = self.diag_phase_bars.borrow_mut().remove(name) {
            if let Some(msg) = summary {
                self.log_summary(format!("{name} {msg}"));
            }
            bar.finish_and_clear();
            self.multi.remove(&bar);
        }
    }

    pub fn finalize(&self) {
        if !self.enabled || self.finalized.replace(true) {
            return;
        }
        if !self.scan_finished.get() {
            self.scanning_bar.finish_and_clear();
            self.multi.remove(&self.scanning_bar);
            self.scan_finished.set(true);
        }
        if let Some(bar) = self.dat_bar.borrow_mut().take() {
            bar.finish_and_clear();
            self.multi.remove(&bar);
        }
        if let Some(bar) = self.action_bar.borrow_mut().take() {
            bar.finish_and_clear();
            self.multi.remove(&bar);
        }
        for (_, state) in self.background_tasks.borrow_mut().drain() {
            state.bar.finish_and_clear();
            self.multi.remove(&state.bar);
        }
        self.item_bytes.borrow_mut().clear();
        self.action_item_bytes.borrow_mut().clear();
        for (_, bar) in self.diag_phase_bars.borrow_mut().drain() {
            bar.finish_and_clear();
            self.multi.remove(&bar);
        }
        self.detail_bar.finish_and_clear();
        self.multi.remove(&self.detail_bar);
    }
}

#[cfg(test)]
pub(crate) fn force_progress_tty_for_tests(flag: Option<bool>) {
    FORCE_PROGRESS_TTY.with(|cell| cell.set(flag));
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        if self.enabled {
            self.finalize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn quiet_mode_disables_progress_even_when_tty_exists() {
        force_progress_tty_for_tests(Some(true));
        let mut cfg = Config::default();
        cfg.quiet = 1;
        assert!(ProgressReporter::maybe_new(&cfg).is_none());
        force_progress_tty_for_tests(None);
    }

    #[test]
    fn progress_initializes_when_tty_is_forced() {
        force_progress_tty_for_tests(Some(true));
        let cfg = Config::default();
        assert!(ProgressReporter::maybe_new(&cfg).is_some());
        force_progress_tty_for_tests(None);
    }

    #[test]
    fn log_diag_records_last_hint() {
        force_progress_tty_for_tests(Some(true));
        let cfg = Config::default();
        let progress = ProgressReporter::maybe_new(&cfg).expect("should create reporter");
        progress.log_diag("diag message");
        assert_eq!(
            progress.diag_last_hint_for_tests().as_deref(),
            Some("diag message")
        );
        drop(progress);
        force_progress_tty_for_tests(None);
    }
}
