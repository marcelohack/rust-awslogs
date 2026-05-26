//! Search Kinesis Data Streams across all shards.
//!
//! Kinesis has no server-side filtering like CloudWatch's `filter_log_events`,
//! so "search" here means: enumerate the stream's shards, read records within
//! the requested time window via `GetRecords`, and match each record's
//! (UTF-8-decoded) payload locally. Shards are read sequentially, which keeps
//! us comfortably inside the per-shard `GetRecords` rate limit.

use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use regex::Regex;

use crate::client::{KinesisClient, KinesisRecord, ShardIteratorPosition};
use crate::core::{Color, ColorPreference, ljust, maybe_color, millis_to_iso, rstrip};
use crate::exceptions::AwsLogsError;

/// Records requested per `GetRecords` call.
const GET_RECORDS_LIMIT: i32 = 1000;

/// Minimum delay between `GetRecords` calls on the same shard while catching up.
/// Kinesis permits 5 `GetRecords`/sec per shard; 250ms stays safely under that.
const SHARD_POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone)]
pub struct KinesisSearchConfig {
    pub stream_name: String,
    /// Pattern to match against each record payload; `None` matches everything.
    pub filter_pattern: Option<String>,
    /// Treat `filter_pattern` as a regex instead of a literal substring.
    pub regex: bool,
    /// Specific shards to read; empty means every shard in the stream.
    pub shard_ids: Vec<String>,
    /// Window start (epoch ms); `None` reads from the oldest record (TRIM_HORIZON).
    pub start: Option<i64>,
    /// Window end (epoch ms); records arriving after this stop the read.
    pub end: Option<i64>,
    pub watch: bool,
    pub watch_interval: Duration,
    pub color: ColorPreference,
    pub output_shard_enabled: bool,
    pub output_timestamp_enabled: bool,
    pub query: Option<String>,
}

impl Default for KinesisSearchConfig {
    fn default() -> Self {
        Self {
            stream_name: String::new(),
            filter_pattern: None,
            regex: false,
            shard_ids: Vec::new(),
            start: None,
            end: None,
            watch: false,
            watch_interval: Duration::from_secs(1),
            color: ColorPreference::Auto,
            output_shard_enabled: true,
            output_timestamp_enabled: false,
            query: None,
        }
    }
}

/// How a decoded record payload is tested against the search pattern.
enum Matcher {
    All,
    Substring(String),
    Regex(Regex),
}

impl Matcher {
    fn matches(&self, text: &str) -> bool {
        match self {
            Matcher::All => true,
            Matcher::Substring(s) => text.contains(s),
            Matcher::Regex(re) => re.is_match(text),
        }
    }
}

pub struct KinesisSearch {
    cfg: KinesisSearchConfig,
    client: Arc<dyn KinesisClient>,
    query: Option<jmespath::Expression<'static>>,
    matcher: Matcher,
}

impl KinesisSearch {
    pub fn new(
        cfg: KinesisSearchConfig,
        client: Arc<dyn KinesisClient>,
    ) -> Result<Self, AwsLogsError> {
        let query = match &cfg.query {
            Some(q) => Some(
                jmespath::compile(q)
                    .map_err(|e| AwsLogsError::Aws(anyhow::anyhow!("invalid JMESPath: {e}")))?,
            ),
            None => None,
        };
        let matcher = match &cfg.filter_pattern {
            None => Matcher::All,
            Some(p) if cfg.regex => Matcher::Regex(
                Regex::new(p)
                    .map_err(|e| AwsLogsError::Aws(anyhow::anyhow!("invalid pattern: {e}")))?,
            ),
            Some(p) => Matcher::Substring(p.clone()),
        };
        Ok(Self {
            cfg,
            client,
            query,
            matcher,
        })
    }

    pub fn config(&self) -> &KinesisSearchConfig {
        &self.cfg
    }

    /// `awslogs kinesis shards STREAM` — write each shard id on its own line.
    pub async fn list_shards_into<W: Write>(&self, writer: &mut W) -> Result<(), AwsLogsError> {
        for shard_id in self.resolve_shards().await? {
            writeln!(writer, "{shard_id}")
                .map_err(|e| AwsLogsError::Aws(anyhow::Error::from(e)))?;
        }
        Ok(())
    }

    /// `awslogs kinesis search STREAM` — read every (selected) shard and write
    /// each matching record on its own line.
    pub async fn search_into<W: Write>(&self, writer: &mut W) -> Result<(), AwsLogsError> {
        let shards = self.resolve_shards().await?;
        let max_shard_length = shards.iter().map(|s| s.len()).max().unwrap_or(10);
        let color_enabled = self.cfg.color.enabled();

        let position = match self.cfg.start {
            Some(ms) => ShardIteratorPosition::AtTimestamp(ms),
            None => ShardIteratorPosition::TrimHorizon,
        };

        for shard_id in &shards {
            let Some(mut iter) = self
                .client
                .get_shard_iterator(&self.cfg.stream_name, shard_id, &position)
                .await
                .map_err(AwsLogsError::from)?
            else {
                continue;
            };

            loop {
                let resp = self
                    .client
                    .get_records(shard_id, &iter, Some(GET_RECORDS_LIMIT))
                    .await
                    .map_err(AwsLogsError::from)?;

                let mut reached_end = false;
                for rec in &resp.records {
                    // Records on a shard arrive in order, so the first one past
                    // the window end means we're done with this shard.
                    if let (Some(end), Some(ts)) = (self.cfg.end, rec.approximate_arrival_timestamp)
                        && ts > end
                    {
                        reached_end = true;
                        break;
                    }
                    if let Some(line) = self.format_record(rec, max_shard_length, color_enabled) {
                        write_line(writer, &line)?;
                    }
                }
                if reached_end {
                    break;
                }

                match resp.next_shard_iterator {
                    None => break,
                    Some(next) => {
                        iter = next;
                        if resp.millis_behind_latest == Some(0) {
                            // Caught up to the tip of the shard.
                            if self.cfg.watch {
                                tokio::time::sleep(self.cfg.watch_interval).await;
                            } else {
                                break;
                            }
                        } else {
                            tokio::time::sleep(SHARD_POLL_INTERVAL).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn resolve_shards(&self) -> Result<Vec<String>, AwsLogsError> {
        if !self.cfg.shard_ids.is_empty() {
            return Ok(self.cfg.shard_ids.clone());
        }
        Ok(self
            .client
            .list_shards(&self.cfg.stream_name)
            .await
            .map_err(AwsLogsError::from)?
            .into_iter()
            .map(|s| s.shard_id)
            .collect())
    }

    /// Decode, filter, optionally reshape via JMESPath, and format one record.
    /// Returns `None` when the record does not match the search pattern.
    fn format_record(
        &self,
        rec: &KinesisRecord,
        max_shard_length: usize,
        color_enabled: bool,
    ) -> Option<String> {
        let text = String::from_utf8_lossy(&rec.data);
        if !self.matcher.matches(&text) {
            return None;
        }

        let mut message = text.into_owned();
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

        let mut parts: Vec<String> = Vec::new();
        if self.cfg.output_shard_enabled {
            let padded = ljust(&rec.shard_id, max_shard_length);
            parts.push(maybe_color(&padded, Color::Cyan, color_enabled));
        }
        if self.cfg.output_timestamp_enabled {
            let ts = rec.approximate_arrival_timestamp.unwrap_or_default();
            parts.push(maybe_color(
                &millis_to_iso(ts),
                Color::Yellow,
                color_enabled,
            ));
        }
        parts.push(rstrip(&message).to_string());
        Some(parts.join(" "))
    }
}

/// Write a line + flush, mirroring `core`'s broken-pipe handling: a closed
/// downstream pipe (e.g. `| head`) is a clean exit, not an error.
fn write_line<W: Write>(writer: &mut W, line: &str) -> Result<(), AwsLogsError> {
    for res in [writeln!(writer, "{line}"), writer.flush()] {
        if let Err(err) = res {
            if err.kind() == io::ErrorKind::BrokenPipe {
                std::process::exit(0);
            }
            return Err(AwsLogsError::Aws(anyhow::Error::from(err)));
        }
    }
    Ok(())
}
