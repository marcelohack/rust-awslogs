//! Port of `awslogs/awslogs/core.py::AWSLogs`.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;

use crate::client::{FilterParams, LogEvent, LogsClient, StreamMeta};
use crate::exceptions::AwsLogsError;

pub const FILTER_LOG_EVENTS_STREAMS_LIMIT: usize = 100;
pub const MAX_EVENTS_PER_CALL: usize = 10_000;
pub const ALL_WILDCARD: &str = "ALL";

// ANSI 8-color codes, kept exactly compatible with Python's `termcolor` so the
// integration tests can byte-compare against the Python suite's expected output.
const ANSI_RESET: &str = "\x1b[0m";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Color {
    Green,
    Cyan,
    Yellow,
    Blue,
    Red,
}

impl Color {
    fn code(self) -> u8 {
        match self {
            Color::Green => 32,
            Color::Cyan => 36,
            Color::Yellow => 33,
            Color::Blue => 34,
            Color::Red => 31,
        }
    }
}

pub fn ansi_colored(text: &str, color: Color) -> String {
    format!("\x1b[{}m{text}{ANSI_RESET}", color.code())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorPreference {
    Auto,
    Always,
    Never,
}

impl ColorPreference {
    fn enabled(self) -> bool {
        match self {
            ColorPreference::Always => true,
            ColorPreference::Never => false,
            ColorPreference::Auto => supports_color::on(supports_color::Stream::Stdout).is_some(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AwsLogsConfig {
    // Selection
    pub log_group_name: Option<String>,
    pub log_stream_name: Option<String>,
    pub log_group_prefix: Option<String>,
    pub filter_pattern: Option<String>,

    // Window (epoch ms)
    pub start: Option<i64>,
    pub end: Option<i64>,

    // Watch / interval
    pub watch: bool,
    pub watch_interval: Duration,

    // Output flags
    pub color: ColorPreference,
    pub output_group_enabled: bool,
    pub output_stream_enabled: bool,
    pub output_timestamp_enabled: bool,
    pub output_ingestion_time_enabled: bool,

    // JMESPath query
    pub query: Option<String>,
}

impl Default for AwsLogsConfig {
    fn default() -> Self {
        Self {
            log_group_name: None,
            log_stream_name: None,
            log_group_prefix: None,
            filter_pattern: None,
            start: None,
            end: None,
            watch: false,
            watch_interval: Duration::from_secs(1),
            color: ColorPreference::Auto,
            output_group_enabled: true,
            output_stream_enabled: true,
            output_timestamp_enabled: false,
            output_ingestion_time_enabled: false,
            query: None,
        }
    }
}

pub struct AwsLogs {
    cfg: AwsLogsConfig,
    client: Arc<dyn LogsClient>,
    query: Option<jmespath::Expression<'static>>,
}

impl AwsLogs {
    pub fn new(cfg: AwsLogsConfig, client: Arc<dyn LogsClient>) -> Result<Self, AwsLogsError> {
        let query = match &cfg.query {
            Some(q) => Some(
                jmespath::compile(q)
                    .map_err(|e| AwsLogsError::Aws(anyhow::anyhow!("invalid JMESPath: {e}")))?,
            ),
            None => None,
        };
        Ok(Self { cfg, client, query })
    }

    pub fn config(&self) -> &AwsLogsConfig {
        &self.cfg
    }

    /// `awslogs groups` — write each group on its own line.
    pub async fn list_groups(&self) -> Result<(), AwsLogsError> {
        let mut stdout = io::stdout();
        self.list_groups_into(&mut stdout).await
    }

    pub async fn list_groups_into<W: Write>(&self, writer: &mut W) -> Result<(), AwsLogsError> {
        for group in self.get_groups().await? {
            writeln!(writer, "{group}").map_err(|e| AwsLogsError::Aws(anyhow::Error::from(e)))?;
        }
        Ok(())
    }

    /// `awslogs streams GROUP` — write each stream on its own line.
    pub async fn list_streams(&self) -> Result<(), AwsLogsError> {
        let mut stdout = io::stdout();
        self.list_streams_into(&mut stdout).await
    }

    pub async fn list_streams_into<W: Write>(&self, writer: &mut W) -> Result<(), AwsLogsError> {
        for stream in self.get_streams(None).await? {
            writeln!(writer, "{stream}").map_err(|e| AwsLogsError::Aws(anyhow::Error::from(e)))?;
        }
        Ok(())
    }

    /// Names of every group matching the configured prefix.
    pub async fn get_groups(&self) -> Result<Vec<String>, AwsLogsError> {
        self.client
            .describe_log_groups(self.cfg.log_group_prefix.as_deref())
            .await
            .map_err(AwsLogsError::from)
    }

    /// Names of every stream in the (optionally overridden) group, filtered to
    /// the configured time window the same way Python's `get_streams` does.
    pub async fn get_streams(
        &self,
        log_group_name: Option<&str>,
    ) -> Result<Vec<String>, AwsLogsError> {
        let group = log_group_name
            .or(self.cfg.log_group_name.as_deref())
            .ok_or_else(|| AwsLogsError::Aws(anyhow::anyhow!("log_group_name required")))?;

        let metas = self
            .client
            .describe_log_streams(group)
            .await
            .map_err(AwsLogsError::from)?;
        Ok(filter_streams_by_window(
            &metas,
            self.cfg.start,
            self.cfg.end,
        ))
    }

    /// Stream names within `group` matching `pattern` (regex, anchored to start;
    /// `ALL` becomes `.*`).
    pub async fn streams_matching(
        &self,
        group: &str,
        pattern: &str,
    ) -> Result<Vec<String>, AwsLogsError> {
        let regex_src = if pattern == ALL_WILDCARD {
            ".*".to_string()
        } else {
            format!("^{pattern}")
        };
        let re = Regex::new(&regex_src)
            .map_err(|e| AwsLogsError::Aws(anyhow::anyhow!("invalid pattern: {e}")))?;
        Ok(self
            .get_streams(Some(group))
            .await?
            .into_iter()
            .filter(|s| re.is_match(s))
            .collect())
    }

    /// `awslogs get GROUP [STREAM_EXPR]`
    pub async fn list_logs(&self) -> Result<(), AwsLogsError> {
        // Use an unlocked stdout handle: it locks per write and releases
        // between lines. Holding a StdoutLock across the (potentially infinite)
        // `--watch` loop would deadlock the Ctrl-C handler, which also writes to
        // stdout and would block forever waiting for the lock.
        let mut stdout = io::stdout();
        self.list_logs_into(&mut stdout).await
    }

    /// Same as [`list_logs`] but writes to an arbitrary writer (testable).
    pub async fn list_logs_into<W: Write>(&self, writer: &mut W) -> Result<(), AwsLogsError> {
        let group = self
            .cfg
            .log_group_name
            .clone()
            .ok_or_else(|| AwsLogsError::Aws(anyhow::anyhow!("log_group_name required")))?;
        let stream_pattern = self
            .cfg
            .log_stream_name
            .clone()
            .unwrap_or_else(|| ALL_WILDCARD.to_string());

        let streams: Vec<String> = if stream_pattern != ALL_WILDCARD {
            let matched = self.streams_matching(&group, &stream_pattern).await?;
            if matched.len() > FILTER_LOG_EVENTS_STREAMS_LIMIT {
                return Err(AwsLogsError::TooManyStreamsFiltered {
                    pattern: stream_pattern,
                    count: matched.len(),
                    limit: FILTER_LOG_EVENTS_STREAMS_LIMIT,
                });
            }
            if matched.is_empty() {
                return Err(AwsLogsError::NoStreamsFiltered(stream_pattern));
            }
            matched
        } else {
            Vec::new()
        };

        let max_stream_length = streams.iter().map(|s| s.len()).max().unwrap_or(10);
        let group_length = group.len();
        let color_enabled = self.cfg.color.enabled();

        let mut params = FilterParams {
            log_group_name: group.clone(),
            log_stream_names: streams.clone(),
            start_time: self.cfg.start,
            end_time: self.cfg.end,
            filter_pattern: self.cfg.filter_pattern.clone(),
            interleaved: true,
        };

        let mut seen: VecDeque<String> = VecDeque::with_capacity(MAX_EVENTS_PER_CALL);
        let mut next_token: Option<String> = None;

        loop {
            let resp = self
                .client
                .filter_log_events(&params, next_token.as_deref())
                .await
                .map_err(AwsLogsError::from)?;

            for event in resp.events {
                if seen.iter().any(|id| id == &event.event_id) {
                    continue;
                }
                if seen.len() == MAX_EVENTS_PER_CALL {
                    seen.pop_front();
                }
                seen.push_back(event.event_id.clone());

                let line = self.format_event(
                    &event,
                    &group,
                    group_length,
                    max_stream_length,
                    color_enabled,
                );
                if let Err(err) = writeln!(writer, "{line}") {
                    if err.kind() == io::ErrorKind::BrokenPipe {
                        std::process::exit(0);
                    }
                    return Err(AwsLogsError::Aws(anyhow::Error::from(err)));
                }
                if let Err(err) = writer.flush() {
                    if err.kind() == io::ErrorKind::BrokenPipe {
                        std::process::exit(0);
                    }
                    return Err(AwsLogsError::Aws(anyhow::Error::from(err)));
                }
            }

            match resp.next_token {
                Some(tok) => {
                    next_token = Some(tok);
                    params.log_stream_names = streams.clone();
                }
                None => {
                    if self.cfg.watch {
                        tokio::time::sleep(self.cfg.watch_interval).await;
                        next_token = None;
                        continue;
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    fn format_event(
        &self,
        event: &LogEvent,
        group: &str,
        group_length: usize,
        max_stream_length: usize,
        color_enabled: bool,
    ) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.cfg.output_group_enabled {
            let padded = ljust(group, group_length);
            parts.push(maybe_color(&padded, Color::Green, color_enabled));
        }
        if self.cfg.output_stream_enabled {
            let padded = ljust(&event.log_stream_name, max_stream_length);
            parts.push(maybe_color(&padded, Color::Cyan, color_enabled));
        }
        if self.cfg.output_timestamp_enabled {
            parts.push(maybe_color(
                &millis_to_iso(event.timestamp),
                Color::Yellow,
                color_enabled,
            ));
        }
        if self.cfg.output_ingestion_time_enabled {
            parts.push(maybe_color(
                &millis_to_iso(event.ingestion_time),
                Color::Blue,
                color_enabled,
            ));
        }

        let mut message = event.message.clone();
        if let Some(expr) = &self.query
            && message.starts_with('{')
            && let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&message)
            && let Ok(jmes_var) = jmespath::Variable::from_serializable(&parsed)
            && let Ok(result) = expr.search(jmes_var)
        {
            message = match &*result {
                jmespath::Variable::String(s) => s.clone(),
                other => other.to_string(),
            };
        }

        parts.push(rstrip(&message).to_string());
        parts.join(" ")
    }
}

fn ljust(text: &str, width: usize) -> String {
    if text.chars().count() >= width {
        text.to_string()
    } else {
        let pad = width - text.chars().count();
        let mut out = String::with_capacity(text.len() + pad);
        out.push_str(text);
        for _ in 0..pad {
            out.push(' ');
        }
        out
    }
}

fn maybe_color(text: &str, color: Color, enabled: bool) -> String {
    if enabled {
        ansi_colored(text, color)
    } else {
        text.to_string()
    }
}

fn rstrip(s: &str) -> &str {
    s.trim_end_matches(|c: char| c.is_whitespace())
}

pub(crate) fn millis_to_iso(millis: i64) -> String {
    use chrono::{TimeZone, Utc};
    let secs = millis.div_euclid(1000);
    let nanos = (millis.rem_euclid(1000) * 1_000_000) as u32;
    let dt = Utc.timestamp_opt(secs, nanos).single().unwrap_or_default();
    // Python: `(res + ".000")[:23] + "Z"` → always 3-digit ms suffix.
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Same logic as `core.py::get_streams`: drop streams whose lifetime doesn't
/// overlap `[start, end]`. Streams without a `first_event_timestamp` (the case
/// of a stream returned directly by name rather than via group listing) pass
/// through unconditionally.
pub fn filter_streams_by_window(
    metas: &[StreamMeta],
    start: Option<i64>,
    end: Option<i64>,
) -> Vec<String> {
    let window_start = start.unwrap_or(0);
    let window_end = end.unwrap_or(i64::MAX);
    metas
        .iter()
        .filter(|s| match (s.first_event_timestamp, s.last_ingestion_time) {
            (Some(first), Some(last)) => first.max(window_start) <= last.min(window_end),
            _ => true,
        })
        .map(|s| s.name.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ljust_pads_to_width() {
        assert_eq!(ljust("foo", 5), "foo  ");
        assert_eq!(ljust("foobar", 3), "foobar");
    }

    #[test]
    fn millis_to_iso_matches_python_format() {
        assert_eq!(millis_to_iso(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(millis_to_iso(1_500), "1970-01-01T00:00:01.500Z");
        assert_eq!(millis_to_iso(5_006), "1970-01-01T00:00:05.006Z");
    }

    #[test]
    fn ansi_colored_matches_termcolor() {
        assert_eq!(ansi_colored("AAA", Color::Green), "\x1b[32mAAA\x1b[0m");
        assert_eq!(ansi_colored("DDD", Color::Cyan), "\x1b[36mDDD\x1b[0m");
    }
}
