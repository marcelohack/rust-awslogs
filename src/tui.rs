//! Ratatui terminal UI for browsing AWS logs.
//!
//! The TUI is a thin front-end over the same engines the CLI uses: it drives
//! [`AwsLogs`] and [`KinesisSearch`] and captures their line-oriented output
//! into an in-memory buffer via [`ChannelWriter`], so every view corresponds to
//! a real CLI command (`groups`, `streams`, `get`, `kinesis shards`,
//! `kinesis search`) rather than a re-implementation.
//!
//! ## Threading
//!
//! The engines compile an optional JMESPath [`Expression`](jmespath::Expression),
//! which is `Rc`-backed and therefore not `Send` — their futures cannot be moved
//! onto the multi-threaded runtime with `tokio::spawn`. So all AWS work runs on a
//! dedicated OS thread with its own current-thread runtime + `LocalSet`. The UI
//! loop (on the main runtime) sends [`EngineCmd`]s to that thread and receives
//! [`Msg`]s back; nothing AWS-related is shared across the runtime boundary.
//!
//! ## Navigation
//!
//! - Two categories, selectable with `Tab`: CloudWatch (default) and Kinesis.
//! - CloudWatch drills groups → streams → logs.
//! - Kinesis drills stream name → shards → records.
//! - A bottom status bar always shows the connected AWS account id.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ratatui::Frame;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
use ratatui::crossterm::execute;
use ratatui::crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::{Constraint, Direction, Layout, Position};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs, Wrap};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::AbortHandle;

use crate::client::{
    AwsCredentialOptions, AwsKinesisClient, AwsLogsClient, AwsStsClient, IdentityClient,
    KinesisClient, LogsClient,
};
use crate::core::{AwsLogs, AwsLogsConfig, ColorPreference};
use crate::exceptions::AwsLogsError;
use crate::kinesis::{KinesisSearch, KinesisSearchConfig};

/// Synthetic list entry meaning "every stream in this group".
const ALL_STREAMS: &str = "[ALL streams]";
/// Synthetic list entry meaning "every shard in this stream".
const ALL_SHARDS: &str = "[ALL shards]";
/// Default look-back window for log/record views (minutes).
const DEFAULT_LOOKBACK_MIN: i64 = 30;
/// Cap on retained output lines, so an unbounded `--watch` view can't grow forever.
const OUTPUT_CAP: usize = 100_000;
/// `req` id used for engine-global messages (identity, startup errors) that
/// should always be applied regardless of the active command. Real command ids
/// start at 1.
const GLOBAL_REQ: u64 = 0;

/// A command sent from the UI loop to the engine thread.
enum EngineCmd {
    Groups {
        req: u64,
    },
    Streams {
        req: u64,
        group: String,
    },
    Logs {
        req: u64,
        group: String,
        stream: Option<String>, // None == ALL
        filter: Option<String>,
        watch: bool,
    },
    Shards {
        req: u64,
        stream: String,
    },
    Search {
        req: u64,
        stream: String,
        shard: Option<String>, // None == all shards
        filter: Option<String>,
        watch: bool,
    },
}

impl EngineCmd {
    fn req(&self) -> u64 {
        match self {
            EngineCmd::Groups { req }
            | EngineCmd::Streams { req, .. }
            | EngineCmd::Logs { req, .. }
            | EngineCmd::Shards { req, .. }
            | EngineCmd::Search { req, .. } => *req,
        }
    }
}

/// Messages sent from the engine thread back to the UI loop. Each carries the
/// `req` id of the command that produced it, so stale results from a superseded
/// command are discarded.
enum Msg {
    Identity { account_id: String },
    Started { req: u64, abort: AbortHandle },
    Items { req: u64, items: Vec<String> },
    Line { req: u64, line: String },
    Error { req: u64, text: String },
    Done { req: u64 },
}

#[derive(Clone, Copy, PartialEq)]
enum Category {
    CloudWatch,
    Kinesis,
}

#[derive(Clone, Copy, PartialEq)]
enum Pane {
    List,
    Output,
    Input,
}

#[derive(Clone, Copy, PartialEq)]
enum InputKind {
    KinesisStream,
    CwFilter,
    KinFilter,
}

/// A `Write` sink that forwards each completed line to the UI over a channel.
/// The engines write and flush line-by-line, so a newline is the natural unit.
struct ChannelWriter {
    tx: UnboundedSender<Msg>,
    req: u64,
    buf: Vec<u8>,
}

impl ChannelWriter {
    fn new(tx: UnboundedSender<Msg>, req: u64) -> Self {
        Self {
            tx,
            req,
            buf: Vec::new(),
        }
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=pos).collect();
            let text = String::from_utf8_lossy(&line[..line.len() - 1]).to_string();
            let _ = self.tx.send(Msg::Line {
                req: self.req,
                line: text,
            });
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn now_minus_minutes(minutes: i64) -> i64 {
    chrono::Utc::now().timestamp_millis() - minutes * 60_000
}

fn send_items(tx: &UnboundedSender<Msg>, req: u64, items: Vec<String>) {
    let _ = tx.send(Msg::Items { req, items });
}

fn send_error(tx: &UnboundedSender<Msg>, req: u64, err: AwsLogsError) {
    let _ = tx.send(Msg::Error {
        req,
        text: err.hint(),
    });
}

fn send_error_text(tx: &UnboundedSender<Msg>, req: u64, text: impl Into<String>) {
    let _ = tx.send(Msg::Error {
        req,
        text: text.into(),
    });
}

// ── engine thread ───────────────────────────────────────────────────────────

/// Owns the AWS clients and runs every command on a current-thread runtime with
/// a `LocalSet`, so the non-`Send` engine futures are legal. Runs until the
/// command channel closes (i.e. the UI shuts down).
fn engine_thread(
    opts: AwsCredentialOptions,
    mut cmd_rx: UnboundedReceiver<EngineCmd>,
    tx: UnboundedSender<Msg>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            send_error_text(&tx, GLOBAL_REQ, format!("failed to start runtime: {err}"));
            return;
        }
    };

    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
        // Resolve the caller identity for the status bar (best effort).
        match AwsStsClient::new(&opts).await {
            Ok(sts) => match sts.caller_identity().await {
                Ok(id) => {
                    let _ = tx.send(Msg::Identity {
                        account_id: id.account_id,
                    });
                }
                Err(err) => {
                    let _ = tx.send(Msg::Identity {
                        account_id: "unavailable".to_string(),
                    });
                    send_error_text(&tx, GLOBAL_REQ, format!("identity lookup failed: {err}"));
                }
            },
            Err(err) => {
                let _ = tx.send(Msg::Identity {
                    account_id: "unavailable".to_string(),
                });
                send_error_text(&tx, GLOBAL_REQ, format!("STS client: {err}"));
            }
        }

        let logs: Option<Arc<dyn LogsClient>> = match AwsLogsClient::new(&opts).await {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                send_error_text(&tx, GLOBAL_REQ, format!("CloudWatch client: {err}"));
                None
            }
        };
        let kinesis: Option<Arc<dyn KinesisClient>> = match AwsKinesisClient::new(&opts).await {
            Ok(client) => Some(Arc::new(client)),
            Err(err) => {
                send_error_text(&tx, GLOBAL_REQ, format!("Kinesis client: {err}"));
                None
            }
        };

        while let Some(cmd) = cmd_rx.recv().await {
            let req = cmd.req();
            let fut = build_future(cmd, logs.clone(), kinesis.clone(), tx.clone());
            let handle = tokio::task::spawn_local(fut);
            let _ = tx.send(Msg::Started {
                req,
                abort: handle.abort_handle(),
            });
        }
    });
}

/// Build the (non-`Send`) future that services a single command.
fn build_future(
    cmd: EngineCmd,
    logs: Option<Arc<dyn LogsClient>>,
    kinesis: Option<Arc<dyn KinesisClient>>,
    tx: UnboundedSender<Msg>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
    match cmd {
        EngineCmd::Groups { req } => Box::pin(async move {
            match logs {
                Some(client) => match AwsLogs::new(AwsLogsConfig::default(), client) {
                    Ok(engine) => match engine.get_groups().await {
                        Ok(items) => send_items(&tx, req, items),
                        Err(err) => send_error(&tx, req, err),
                    },
                    Err(err) => send_error(&tx, req, err),
                },
                None => send_error_text(&tx, req, "CloudWatch client unavailable"),
            }
            let _ = tx.send(Msg::Done { req });
        }),
        EngineCmd::Streams { req, group } => Box::pin(async move {
            let cfg = AwsLogsConfig {
                log_group_name: Some(group),
                ..Default::default()
            };
            match logs {
                Some(client) => match AwsLogs::new(cfg, client) {
                    Ok(engine) => match engine.get_streams(None).await {
                        Ok(mut items) => {
                            items.insert(0, ALL_STREAMS.to_string());
                            send_items(&tx, req, items);
                        }
                        Err(err) => send_error(&tx, req, err),
                    },
                    Err(err) => send_error(&tx, req, err),
                },
                None => send_error_text(&tx, req, "CloudWatch client unavailable"),
            }
            let _ = tx.send(Msg::Done { req });
        }),
        EngineCmd::Logs {
            req,
            group,
            stream,
            filter,
            watch,
        } => Box::pin(async move {
            // A specific stream is queried by exact name (no pattern/window
            // filtering) and with no lower time bound, so its full history is
            // shown regardless of how long it has been idle. When watching, a
            // lookback bound keeps each poll cheap. "ALL streams" keeps the
            // lookback window to avoid flooding the whole group.
            let explicit_streams = stream.as_ref().map(|s| vec![s.clone()]);
            let start = match (&stream, watch) {
                (Some(_), false) => None,
                _ => Some(now_minus_minutes(DEFAULT_LOOKBACK_MIN)),
            };
            let cfg = AwsLogsConfig {
                log_group_name: Some(group),
                log_stream_name: Some(stream.unwrap_or_else(|| "ALL".to_string())),
                explicit_streams,
                filter_pattern: filter,
                start,
                watch,
                color: ColorPreference::Never,
                output_group_enabled: false,
                output_stream_enabled: true,
                output_timestamp_enabled: true,
                ..Default::default()
            };
            match logs {
                Some(client) => match AwsLogs::new(cfg, client) {
                    Ok(engine) => {
                        let mut writer = ChannelWriter::new(tx.clone(), req);
                        if let Err(err) = engine.list_logs_into(&mut writer).await {
                            send_error(&tx, req, err);
                        }
                    }
                    Err(err) => send_error(&tx, req, err),
                },
                None => send_error_text(&tx, req, "CloudWatch client unavailable"),
            }
            let _ = tx.send(Msg::Done { req });
        }),
        EngineCmd::Shards { req, stream } => Box::pin(async move {
            match kinesis {
                Some(client) => match client.list_shards(&stream).await {
                    Ok(shards) => {
                        let mut items: Vec<String> =
                            shards.into_iter().map(|s| s.shard_id).collect();
                        items.insert(0, ALL_SHARDS.to_string());
                        send_items(&tx, req, items);
                    }
                    Err(err) => send_error_text(&tx, req, err.to_string()),
                },
                None => send_error_text(&tx, req, "Kinesis client unavailable"),
            }
            let _ = tx.send(Msg::Done { req });
        }),
        EngineCmd::Search {
            req,
            stream,
            shard,
            filter,
            watch,
        } => Box::pin(async move {
            let cfg = KinesisSearchConfig {
                stream_name: stream,
                filter_pattern: filter,
                shard_ids: shard.map(|s| vec![s]).unwrap_or_default(),
                start: Some(now_minus_minutes(DEFAULT_LOOKBACK_MIN)),
                watch,
                color: ColorPreference::Never,
                output_shard_enabled: true,
                output_timestamp_enabled: true,
                ..Default::default()
            };
            match kinesis {
                Some(client) => match KinesisSearch::new(cfg, client) {
                    Ok(engine) => {
                        let mut writer = ChannelWriter::new(tx.clone(), req);
                        if let Err(err) = engine.search_into(&mut writer).await {
                            send_error(&tx, req, err);
                        }
                    }
                    Err(err) => send_error(&tx, req, err),
                },
                None => send_error_text(&tx, req, "Kinesis client unavailable"),
            }
            let _ = tx.send(Msg::Done { req });
        }),
    }
}

// ── application state ─────────────────────────────────────────────────────────

struct App {
    account_id: String,

    category: Category,
    pane: Pane,

    // Navigation context.
    cw_group: Option<String>,
    cw_stream_sel: Option<String>, // None == ALL streams
    cw_filter: Option<String>,
    kin_stream: Option<String>,
    kin_shard_sel: Option<String>, // None == all shards
    kin_filter: Option<String>,

    // List pane.
    items: Vec<String>,
    list_state: ListState,
    list_title: String,

    // Output pane.
    output: Vec<String>,
    output_title: String,
    follow: bool,
    scroll: usize,
    viewport_height: usize,

    // Input pane.
    input: String,
    input_prompt: String,
    input_kind: InputKind,

    status: String,

    // Async plumbing.
    cmd_tx: UnboundedSender<EngineCmd>,
    req: u64,
    abort: Option<AbortHandle>,
    loading: bool,
    watching: bool,

    should_quit: bool,
}

impl App {
    fn new(cmd_tx: UnboundedSender<EngineCmd>) -> Self {
        Self {
            account_id: "resolving…".to_string(),
            category: Category::CloudWatch,
            pane: Pane::List,
            cw_group: None,
            cw_stream_sel: None,
            cw_filter: None,
            kin_stream: None,
            kin_shard_sel: None,
            kin_filter: None,
            items: Vec::new(),
            list_state: ListState::default(),
            list_title: String::new(),
            output: Vec::new(),
            output_title: String::new(),
            follow: true,
            scroll: 0,
            viewport_height: 1,
            input: String::new(),
            input_prompt: String::new(),
            input_kind: InputKind::KinesisStream,
            status: String::new(),
            cmd_tx,
            req: GLOBAL_REQ,
            abort: None,
            loading: false,
            watching: false,
            should_quit: false,
        }
    }

    /// Cancel any in-flight command and allocate a fresh request id.
    fn start(&mut self) -> u64 {
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
        self.req += 1;
        self.req
    }

    fn send(&self, cmd: EngineCmd) {
        let _ = self.cmd_tx.send(cmd);
    }

    // ── loaders ────────────────────────────────────────────────────────────

    fn load_groups(&mut self) {
        let req = self.start();
        self.category = Category::CloudWatch;
        self.pane = Pane::List;
        self.cw_group = None;
        self.items.clear();
        self.list_state.select(None);
        self.list_title = "CloudWatch · Groups".to_string();
        self.loading = true;
        self.status.clear();
        self.send(EngineCmd::Groups { req });
    }

    fn load_streams(&mut self, group: String) {
        let req = self.start();
        self.category = Category::CloudWatch;
        self.pane = Pane::List;
        self.cw_group = Some(group.clone());
        self.items.clear();
        self.list_state.select(None);
        self.list_title = format!("CloudWatch · {group} · Streams");
        self.loading = true;
        self.status.clear();
        self.send(EngineCmd::Streams { req, group });
    }

    /// `stream == None` means the group's whole stream set (`ALL`).
    fn load_logs(&mut self, group: String, stream: Option<String>) {
        let req = self.start();
        self.category = Category::CloudWatch;
        self.pane = Pane::Output;
        self.output.clear();
        self.follow = true;
        self.scroll = 0;
        let label = stream.clone().unwrap_or_else(|| "ALL".to_string());
        self.output_title = format!("CloudWatch · {group} · {label}");
        self.loading = true;
        self.status.clear();
        self.send(EngineCmd::Logs {
            req,
            group,
            stream,
            filter: self.cw_filter.clone(),
            watch: self.watching,
        });
    }

    fn load_shards(&mut self, stream: String) {
        let req = self.start();
        self.category = Category::Kinesis;
        self.pane = Pane::List;
        self.kin_stream = Some(stream.clone());
        self.items.clear();
        self.list_state.select(None);
        self.list_title = format!("Kinesis · {stream} · Shards");
        self.loading = true;
        self.status.clear();
        self.send(EngineCmd::Shards { req, stream });
    }

    /// `shard == None` means search across every shard.
    fn load_search(&mut self, stream: String, shard: Option<String>) {
        let req = self.start();
        self.category = Category::Kinesis;
        self.pane = Pane::Output;
        self.output.clear();
        self.follow = true;
        self.scroll = 0;
        let label = shard.clone().unwrap_or_else(|| "all shards".to_string());
        self.output_title = format!("Kinesis · {stream} · {label}");
        self.loading = true;
        self.status.clear();
        self.send(EngineCmd::Search {
            req,
            stream,
            shard,
            filter: self.kin_filter.clone(),
            watch: self.watching,
        });
    }

    // ── message handling ─────────────────────────────────────────────────────

    fn on_msg(&mut self, msg: Msg) {
        match msg {
            Msg::Identity { account_id } => {
                self.account_id = account_id;
            }
            Msg::Started { req, abort } => {
                if req == self.req {
                    self.abort = Some(abort);
                } else {
                    // The command was already superseded before it even started.
                    abort.abort();
                }
            }
            Msg::Items { req, items } if req == self.req => {
                self.items = items;
                self.list_state
                    .select(if self.items.is_empty() { None } else { Some(0) });
                self.loading = false;
            }
            Msg::Line { req, line } if req == self.req => {
                self.output.push(line);
                if self.output.len() > OUTPUT_CAP {
                    let excess = self.output.len() - OUTPUT_CAP;
                    self.output.drain(0..excess);
                    if !self.follow {
                        self.scroll = self.scroll.saturating_sub(excess);
                    }
                }
            }
            Msg::Error { req, text } if req == self.req || req == GLOBAL_REQ => {
                self.status = format!("Error: {text}");
                self.loading = false;
            }
            Msg::Done { req } if req == self.req => {
                self.loading = false;
            }
            _ => {}
        }
    }

    // ── input handling ────────────────────────────────────────────────────────

    fn on_key(&mut self, code: KeyCode, mods: KeyModifiers) {
        if mods.contains(KeyModifiers::CONTROL) && code == KeyCode::Char('c') {
            self.should_quit = true;
            return;
        }
        match self.pane {
            Pane::Input => self.on_key_input(code),
            Pane::List => self.on_key_list(code),
            Pane::Output => self.on_key_output(code),
        }
    }

    fn on_key_input(&mut self, code: KeyCode) {
        match code {
            KeyCode::Enter => self.submit_input(),
            KeyCode::Esc => self.cancel_input(),
            KeyCode::Backspace => {
                self.input.pop();
            }
            KeyCode::Char(c) => self.input.push(c),
            _ => {}
        }
    }

    fn on_key_list(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => self.switch_category(),
            KeyCode::Up | KeyCode::Char('k') => self.list_prev(),
            KeyCode::Down | KeyCode::Char('j') => self.list_next(),
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => self.list_enter(),
            KeyCode::Esc | KeyCode::Backspace | KeyCode::Left | KeyCode::Char('h') => {
                self.list_back()
            }
            KeyCode::Char('r') => self.list_reload(),
            _ => {}
        }
    }

    fn on_key_output(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => self.switch_category(),
            KeyCode::Up | KeyCode::Char('k') => {
                self.follow = false;
                self.scroll = self.scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.follow = false;
                self.scroll_down(1);
            }
            KeyCode::PageUp => {
                self.follow = false;
                self.scroll = self.scroll.saturating_sub(10);
            }
            KeyCode::PageDown => {
                self.follow = false;
                self.scroll_down(10);
            }
            KeyCode::Char('g') => {
                self.follow = false;
                self.scroll = 0;
            }
            KeyCode::Char('G') => self.follow = true,
            KeyCode::Char('w') => self.toggle_watch(),
            KeyCode::Char('/') => self.open_filter_input(),
            KeyCode::Esc | KeyCode::Backspace | KeyCode::Left => self.output_back(),
            _ => {}
        }
    }

    fn scroll_down(&mut self, n: usize) {
        let max_start = self.output.len().saturating_sub(self.viewport_height);
        self.scroll = (self.scroll + n).min(max_start);
    }

    fn list_prev(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let i = self
            .list_state
            .selected()
            .map_or(0, |i| i.saturating_sub(1));
        self.list_state.select(Some(i));
    }

    fn list_next(&mut self) {
        if self.items.is_empty() {
            return;
        }
        let i = match self.list_state.selected() {
            Some(i) if i + 1 < self.items.len() => i + 1,
            Some(i) => i,
            None => 0,
        };
        self.list_state.select(Some(i));
    }

    fn list_enter(&mut self) {
        let Some(item) = self
            .list_state
            .selected()
            .and_then(|i| self.items.get(i).cloned())
        else {
            return;
        };
        match self.category {
            Category::CloudWatch => match self.cw_group.clone() {
                None => self.load_streams(item),
                Some(group) => {
                    if item == ALL_STREAMS {
                        self.cw_stream_sel = None;
                        self.load_logs(group, None);
                    } else {
                        self.cw_stream_sel = Some(item.clone());
                        self.load_logs(group, Some(item));
                    }
                }
            },
            Category::Kinesis => {
                let Some(stream) = self.kin_stream.clone() else {
                    return;
                };
                if item == ALL_SHARDS {
                    self.kin_shard_sel = None;
                    self.load_search(stream, None);
                } else {
                    self.kin_shard_sel = Some(item.clone());
                    self.load_search(stream, Some(item));
                }
            }
        }
    }

    fn list_back(&mut self) {
        match self.category {
            Category::CloudWatch => {
                if self.cw_group.is_some() {
                    self.load_groups();
                }
            }
            Category::Kinesis => self.open_stream_input(),
        }
    }

    fn list_reload(&mut self) {
        match self.category {
            Category::CloudWatch => match self.cw_group.clone() {
                Some(group) => self.load_streams(group),
                None => self.load_groups(),
            },
            Category::Kinesis => {
                if let Some(stream) = self.kin_stream.clone() {
                    self.load_shards(stream);
                }
            }
        }
    }

    /// Abort the in-flight command (if any) and invalidate its late messages.
    /// Bumping `req` means any `Line`/`Done`/`Error` still in the channel from
    /// the aborted task is ignored by `on_msg`.
    fn cancel_current(&mut self) {
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
        self.req += 1;
        self.loading = false;
    }

    fn output_back(&mut self) {
        self.cancel_current();
        self.watching = false;
        self.pane = Pane::List;
    }

    fn switch_category(&mut self) {
        match self.category {
            Category::CloudWatch => match self.kin_stream.clone() {
                Some(stream) => self.load_shards(stream),
                None => self.open_stream_input(),
            },
            Category::Kinesis => self.load_groups(),
        }
    }

    fn toggle_watch(&mut self) {
        self.watching = !self.watching;
        match self.category {
            Category::CloudWatch => {
                if let Some(group) = self.cw_group.clone() {
                    let sel = self.cw_stream_sel.clone();
                    self.load_logs(group, sel);
                }
            }
            Category::Kinesis => {
                if let Some(stream) = self.kin_stream.clone() {
                    let shard = self.kin_shard_sel.clone();
                    self.load_search(stream, shard);
                }
            }
        }
    }

    fn open_filter_input(&mut self) {
        self.pane = Pane::Input;
        match self.category {
            Category::CloudWatch => {
                self.input_kind = InputKind::CwFilter;
                self.input = self.cw_filter.clone().unwrap_or_default();
                self.input_prompt = "CloudWatch filter pattern (empty = none)".to_string();
            }
            Category::Kinesis => {
                self.input_kind = InputKind::KinFilter;
                self.input = self.kin_filter.clone().unwrap_or_default();
                self.input_prompt = "Kinesis filter substring (empty = none)".to_string();
            }
        }
    }

    fn open_stream_input(&mut self) {
        // Leaving whatever CloudWatch view was active: stop its task so a live
        // `--watch` tail doesn't keep polling in the background.
        self.cancel_current();
        self.watching = false;
        self.category = Category::Kinesis;
        self.pane = Pane::Input;
        self.input_kind = InputKind::KinesisStream;
        self.input = self.kin_stream.clone().unwrap_or_default();
        self.input_prompt = "Kinesis stream name".to_string();
    }

    fn submit_input(&mut self) {
        let value = self.input.trim().to_string();
        match self.input_kind {
            InputKind::KinesisStream => {
                if value.is_empty() {
                    self.status = "Enter a stream name".to_string();
                    return;
                }
                self.kin_stream = Some(value.clone());
                self.load_shards(value);
            }
            InputKind::CwFilter => {
                self.cw_filter = if value.is_empty() { None } else { Some(value) };
                match self.cw_group.clone() {
                    Some(group) => {
                        let sel = self.cw_stream_sel.clone();
                        self.load_logs(group, sel);
                    }
                    None => self.pane = Pane::Output,
                }
            }
            InputKind::KinFilter => {
                self.kin_filter = if value.is_empty() { None } else { Some(value) };
                match self.kin_stream.clone() {
                    Some(stream) => {
                        let shard = self.kin_shard_sel.clone();
                        self.load_search(stream, shard);
                    }
                    None => self.pane = Pane::Output,
                }
            }
        }
    }

    fn cancel_input(&mut self) {
        match self.input_kind {
            InputKind::KinesisStream => match self.kin_stream.clone() {
                Some(stream) => self.load_shards(stream),
                None => self.load_groups(),
            },
            InputKind::CwFilter | InputKind::KinFilter => self.pane = Pane::Output,
        }
    }

    // ── rendering ──────────────────────────────────────────────────────────

    fn main_title(&self) -> String {
        match self.pane {
            Pane::List => self.list_title.clone(),
            Pane::Output => self.output_title.clone(),
            Pane::Input => self.input_prompt.clone(),
        }
    }

    fn key_hints(&self) -> &'static str {
        match self.pane {
            Pane::Input => " Enter submit · Esc cancel ",
            Pane::List => match self.category {
                Category::CloudWatch => {
                    if self.cw_group.is_some() {
                        " ↑↓ move · Enter logs · Esc groups · r reload · Tab switch · q quit "
                    } else {
                        " ↑↓ move · Enter streams · r reload · Tab switch · q quit "
                    }
                }
                Category::Kinesis => {
                    " ↑↓ move · Enter search · Esc stream · r reload · Tab switch · q quit "
                }
            },
            Pane::Output => {
                " ↑↓ scroll · G follow · w watch · / filter · Esc back · Tab switch · q quit "
            }
        }
    }

    /// Placeholder text shown when a list came back empty (not while loading).
    fn list_empty_hint(&self) -> &'static str {
        match self.category {
            Category::CloudWatch => {
                if self.cw_group.is_some() {
                    "  No streams in this log group."
                } else {
                    "  No log groups found."
                }
            }
            Category::Kinesis => "  No shards found for this stream.",
        }
    }

    /// Placeholder text shown when a log/record view produced no lines (not
    /// while loading) — distinguishes "nothing here" from an error.
    fn output_empty_hint(&self) -> String {
        let what = match self.category {
            Category::CloudWatch => {
                if self.cw_stream_sel.is_some() {
                    "No log events found for this stream."
                } else {
                    "No log events in the last 30 minutes for this group."
                }
            }
            Category::Kinesis => "No matching records in the selected window.",
        };
        format!(
            "  {what}\n\n  Press  w  to watch (live tail)   ·   /  to filter   ·   Esc  to go back."
        )
    }

    fn draw(&mut self, f: &mut Frame) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(f.area());

        // Tabs.
        let selected = match self.category {
            Category::CloudWatch => 0,
            Category::Kinesis => 1,
        };
        let tabs = Tabs::new(vec!["CloudWatch", "Kinesis"])
            .select(selected)
            .block(Block::default().borders(Borders::ALL).title(" awslogs "))
            .highlight_style(
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(tabs, chunks[0]);

        // Main content.
        let title = match self.pane {
            Pane::List if !self.items.is_empty() => {
                format!(" {} ({}) ", self.list_title, self.items.len())
            }
            _ => format!(" {} ", self.main_title()),
        };
        let block = Block::default()
            .borders(Borders::ALL)
            .title(title)
            .title_bottom(Line::from(self.key_hints()).right_aligned());
        let area = chunks[1];
        self.viewport_height = area.height.saturating_sub(2) as usize;
        let dim = Style::default().fg(Color::DarkGray);

        match self.pane {
            Pane::List if self.items.is_empty() => {
                let msg = if self.loading {
                    "  Loading…".to_string()
                } else {
                    self.list_empty_hint().to_string()
                };
                f.render_widget(Paragraph::new(msg).block(block).style(dim), area);
            }
            Pane::List => {
                let items: Vec<ListItem> = self
                    .items
                    .iter()
                    .map(|i| ListItem::new(i.clone()))
                    .collect();
                let list = List::new(items)
                    .block(block)
                    .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
                    .highlight_symbol("› ");
                f.render_stateful_widget(list, area, &mut self.list_state);
            }
            Pane::Output if self.output.is_empty() => {
                let msg = if self.loading {
                    "  Loading…".to_string()
                } else {
                    self.output_empty_hint()
                };
                f.render_widget(
                    Paragraph::new(msg)
                        .block(block)
                        .style(dim)
                        .wrap(Wrap { trim: false }),
                    area,
                );
            }
            Pane::Output => {
                let total = self.output.len();
                let h = self.viewport_height.max(1);
                let max_start = total.saturating_sub(h);
                let start = if self.follow {
                    max_start
                } else {
                    self.scroll.min(max_start)
                };
                let end = (start + h).min(total);
                let lines: Vec<Line> = self.output[start..end]
                    .iter()
                    .map(|l| Line::from(l.clone()))
                    .collect();
                let para = Paragraph::new(lines).block(block);
                f.render_widget(para, area);
            }
            Pane::Input => {
                let para = Paragraph::new(format!("> {}", self.input)).block(block);
                f.render_widget(para, area);
                let x = area.x + 3 + self.input.chars().count() as u16;
                let y = area.y + 1;
                f.set_cursor_position(Position { x, y });
            }
        }

        // Status bar.
        let mut spans = vec![Span::raw(format!(" Account {} ", self.account_id))];
        if self.loading {
            spans.push(Span::raw("· loading "));
        }
        if self.watching {
            spans.push(Span::styled(
                "· watching ",
                Style::default().fg(Color::Yellow),
            ));
        }
        if !self.status.is_empty() {
            let color = if self.status.starts_with("Error") {
                Color::Red
            } else {
                Color::White
            };
            spans.push(Span::styled(
                format!("· {} ", self.status),
                Style::default().fg(color),
            ));
        }
        let bar = Paragraph::new(Line::from(spans))
            .style(Style::default().bg(Color::DarkGray).fg(Color::White));
        f.render_widget(bar, chunks[2]);
    }
}

// ── entry point ───────────────────────────────────────────────────────────────

/// Enter the alternate screen, run the UI loop, and restore the terminal. All
/// AWS work happens on a dedicated engine thread; this function only drives the
/// UI on the caller's runtime.
pub async fn run(opts: AwsCredentialOptions) -> Result<(), AwsLogsError> {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<EngineCmd>();
    let (msg_tx, msg_rx) = mpsc::unbounded_channel::<Msg>();
    let engine = std::thread::spawn(move || engine_thread(opts, cmd_rx, msg_tx));

    enable_raw_mode().map_err(|e| AwsLogsError::Aws(e.into()))?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).map_err(|e| AwsLogsError::Aws(e.into()))?;

    // Restore the terminal even if a panic unwinds through the UI.
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).map_err(|e| AwsLogsError::Aws(e.into()))?;

    let mut app = App::new(cmd_tx);
    app.load_groups();

    let result = event_loop(&mut app, &mut terminal, msg_rx).await;

    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
    let _ = terminal.show_cursor();

    // Dropping the command sender closes the engine's channel, ending its loop.
    drop(app);
    let _ = engine.join();

    result
}

async fn event_loop(
    app: &mut App,
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    mut msg_rx: UnboundedReceiver<Msg>,
) -> Result<(), AwsLogsError> {
    // Terminal input is blocking, so read it on a dedicated OS thread and
    // forward events over a channel the async loop can select on. The poll
    // timeout lets the thread notice `running` flipping to false and exit.
    let (key_tx, mut key_rx) = mpsc::unbounded_channel::<Event>();
    let running = Arc::new(AtomicBool::new(true));
    let running_reader = running.clone();
    let reader = std::thread::spawn(move || {
        while running_reader.load(Ordering::Relaxed) {
            match event::poll(Duration::from_millis(100)) {
                Ok(true) => match event::read() {
                    Ok(ev) => {
                        if key_tx.send(ev).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                },
                Ok(false) => {}
                Err(_) => break,
            }
        }
    });

    loop {
        terminal
            .draw(|f| app.draw(f))
            .map_err(|e| AwsLogsError::Aws(e.into()))?;
        if app.should_quit {
            break;
        }
        tokio::select! {
            maybe_event = key_rx.recv() => {
                match maybe_event {
                    Some(Event::Key(key)) if key.kind != KeyEventKind::Release => {
                        app.on_key(key.code, key.modifiers);
                    }
                    Some(_) => {}
                    None => break,
                }
            }
            maybe_msg = msg_rx.recv() => {
                match maybe_msg {
                    Some(msg) => app.on_msg(msg),
                    None => break,
                }
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    let _ = reader.join();
    Ok(())
}
