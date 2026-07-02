//! Abstraction over the CloudWatch Logs SDK.
//!
//! The [`LogsClient`] trait is the seam tests mock against — equivalent to the
//! Python `@patch('awslogs.core.boto3_client')` pattern. Production code uses
//! [`AwsLogsClient`], a thin wrapper over `aws_sdk_cloudwatchlogs::Client`.

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_cloudwatchlogs::Client as SdkClient;
use aws_sdk_cloudwatchlogs::config::Region;
use aws_sdk_kinesis::Client as KinesisSdkClient;
use aws_sdk_sts::Client as StsSdkClient;

#[derive(Debug, Clone, Default)]
pub struct StreamMeta {
    pub name: String,
    pub first_event_timestamp: Option<i64>,
    pub last_ingestion_time: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct LogEvent {
    pub event_id: String,
    pub timestamp: i64,
    pub ingestion_time: i64,
    pub message: String,
    pub log_stream_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct FilterParams {
    pub log_group_name: String,
    pub log_stream_names: Vec<String>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub filter_pattern: Option<String>,
    pub interleaved: bool,
}

#[derive(Debug, Clone, Default)]
pub struct FilterResponse {
    pub events: Vec<LogEvent>,
    pub next_token: Option<String>,
}

/// All the CloudWatch Logs operations awslogs uses.
#[async_trait]
pub trait LogsClient: Send + Sync {
    /// List every log group, optionally narrowed by name prefix.
    async fn describe_log_groups(&self, prefix: Option<&str>)
    -> Result<Vec<String>, anyhow::Error>;

    /// List every stream in `log_group_name`.
    async fn describe_log_streams(
        &self,
        log_group_name: &str,
    ) -> Result<Vec<StreamMeta>, anyhow::Error>;

    /// Single `filter_log_events` request (caller drives pagination).
    async fn filter_log_events(
        &self,
        params: &FilterParams,
        next_token: Option<&str>,
    ) -> Result<FilterResponse, anyhow::Error>;
}

/// Real client that talks to AWS.
pub struct AwsLogsClient {
    inner: SdkClient,
}

/// Resolve a shared AWS config from CLI credential options. Shared by every
/// service client so credential/region/endpoint handling stays identical.
async fn load_shared_config(opts: &AwsCredentialOptions) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

    if let Some(profile) = &opts.profile {
        loader = loader.profile_name(profile);
    }
    if let Some(region) = &opts.region {
        loader = loader.region(Region::new(region.clone()));
    }
    if let (Some(key), Some(secret)) = (&opts.access_key_id, &opts.secret_access_key) {
        let creds = Credentials::new(key, secret, opts.session_token.clone(), None, "awslogs-cli");
        loader = loader.credentials_provider(creds);
    }
    if let Some(url) = &opts.endpoint_url {
        loader = loader.endpoint_url(url);
    }
    loader.load().await
}

impl AwsLogsClient {
    pub async fn new(opts: &AwsCredentialOptions) -> anyhow::Result<Self> {
        let shared = load_shared_config(opts).await;
        let mut builder = aws_sdk_cloudwatchlogs::config::Builder::from(&shared);
        if let Some(url) = &opts.endpoint_url {
            builder = builder.endpoint_url(url);
        }
        let cfg = builder.build();
        Ok(Self {
            inner: SdkClient::from_conf(cfg),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct AwsCredentialOptions {
    pub profile: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub endpoint_url: Option<String>,
}

#[async_trait]
impl LogsClient for AwsLogsClient {
    async fn describe_log_groups(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut paginator = self
            .inner
            .describe_log_groups()
            .set_log_group_name_prefix(prefix.map(str::to_string))
            .into_paginator()
            .send();

        let mut out = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(groups) = page.log_groups {
                for g in groups {
                    if let Some(name) = g.log_group_name {
                        out.push(name);
                    }
                }
            }
        }
        Ok(out)
    }

    async fn describe_log_streams(
        &self,
        log_group_name: &str,
    ) -> Result<Vec<StreamMeta>, anyhow::Error> {
        let mut paginator = self
            .inner
            .describe_log_streams()
            .log_group_name(log_group_name)
            .into_paginator()
            .send();

        let mut out = Vec::new();
        while let Some(page) = paginator.next().await {
            let page = page?;
            if let Some(streams) = page.log_streams {
                for s in streams {
                    if let Some(name) = s.log_stream_name {
                        out.push(StreamMeta {
                            name,
                            first_event_timestamp: s.first_event_timestamp,
                            last_ingestion_time: s.last_ingestion_time,
                        });
                    }
                }
            }
        }
        Ok(out)
    }

    async fn filter_log_events(
        &self,
        params: &FilterParams,
        next_token: Option<&str>,
    ) -> Result<FilterResponse, anyhow::Error> {
        // `interleaved` was deprecated in 2019 — events are always interleaved
        // server-side now — so we don't pass it. The field remains on
        // FilterParams for parity with the Python config.
        let _ = params.interleaved;
        let mut req = self
            .inner
            .filter_log_events()
            .log_group_name(&params.log_group_name);

        for name in &params.log_stream_names {
            req = req.log_stream_names(name);
        }
        if let Some(start) = params.start_time {
            req = req.start_time(start);
        }
        if let Some(end) = params.end_time {
            req = req.end_time(end);
        }
        if let Some(p) = &params.filter_pattern {
            req = req.filter_pattern(p);
        }
        if let Some(tok) = next_token {
            req = req.next_token(tok);
        }

        let resp = req.send().await?;
        let events = resp
            .events
            .unwrap_or_default()
            .into_iter()
            .filter_map(|e| {
                Some(LogEvent {
                    event_id: e.event_id?,
                    timestamp: e.timestamp.unwrap_or_default(),
                    ingestion_time: e.ingestion_time.unwrap_or_default(),
                    message: e.message.unwrap_or_default(),
                    log_stream_name: e.log_stream_name.unwrap_or_default(),
                })
            })
            .collect();
        Ok(FilterResponse {
            events,
            next_token: resp.next_token,
        })
    }
}

// ─────────────────────────────── Kinesis ──────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ShardInfo {
    pub shard_id: String,
}

/// A single Kinesis record, annotated with the shard it came from.
#[derive(Debug, Clone)]
pub struct KinesisRecord {
    pub shard_id: String,
    pub sequence_number: String,
    pub partition_key: String,
    /// Approximate arrival time in epoch milliseconds, when the server reports it.
    pub approximate_arrival_timestamp: Option<i64>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct GetRecordsResponse {
    pub records: Vec<KinesisRecord>,
    /// Iterator to use on the next `get_records` call; `None` once the shard is
    /// closed and fully drained.
    pub next_shard_iterator: Option<String>,
    /// Milliseconds the response is behind the tip of the shard; `Some(0)` means
    /// we have caught up to the latest record.
    pub millis_behind_latest: Option<i64>,
}

/// Where a shard iterator should start reading from.
#[derive(Debug, Clone)]
pub enum ShardIteratorPosition {
    /// Oldest untrimmed record in the shard.
    TrimHorizon,
    /// Only records added after the iterator is created.
    Latest,
    /// First record at or after the given epoch-millisecond timestamp.
    AtTimestamp(i64),
}

/// The subset of Kinesis Data Streams operations awslogs uses to search a stream.
#[async_trait]
pub trait KinesisClient: Send + Sync {
    /// List every shard in `stream_name`.
    async fn list_shards(&self, stream_name: &str) -> Result<Vec<ShardInfo>, anyhow::Error>;

    /// Open an iterator into `shard_id` at `position`. Returns `None` only if the
    /// service declines to produce one (e.g. a closed shard past TRIM_HORIZON).
    async fn get_shard_iterator(
        &self,
        stream_name: &str,
        shard_id: &str,
        position: &ShardIteratorPosition,
    ) -> Result<Option<String>, anyhow::Error>;

    /// Single `get_records` request (caller drives pagination via the returned
    /// `next_shard_iterator`). `shard_id` is supplied so the records can be
    /// annotated with their origin.
    async fn get_records(
        &self,
        shard_id: &str,
        shard_iterator: &str,
        limit: Option<i32>,
    ) -> Result<GetRecordsResponse, anyhow::Error>;
}

/// Real client that talks to AWS Kinesis.
pub struct AwsKinesisClient {
    inner: KinesisSdkClient,
}

impl AwsKinesisClient {
    pub async fn new(opts: &AwsCredentialOptions) -> anyhow::Result<Self> {
        let shared = load_shared_config(opts).await;
        let mut builder = aws_sdk_kinesis::config::Builder::from(&shared);
        if let Some(url) = &opts.endpoint_url {
            builder = builder.endpoint_url(url);
        }
        let cfg = builder.build();
        Ok(Self {
            inner: KinesisSdkClient::from_conf(cfg),
        })
    }
}

// ─────────────────────────────── Identity (STS) ───────────────────────────────

/// The caller's AWS identity, used by the TUI status bar.
#[derive(Debug, Clone, Default)]
pub struct CallerIdentity {
    pub account_id: String,
    /// Resolved region for the session (may be empty if none was configured).
    pub region: String,
}

/// The single STS operation awslogs needs: resolve the caller's account id.
#[async_trait]
pub trait IdentityClient: Send + Sync {
    async fn caller_identity(&self) -> Result<CallerIdentity, anyhow::Error>;
}

/// Real client that talks to AWS STS.
pub struct AwsStsClient {
    inner: StsSdkClient,
    region: String,
}

impl AwsStsClient {
    pub async fn new(opts: &AwsCredentialOptions) -> anyhow::Result<Self> {
        let shared = load_shared_config(opts).await;
        let region = shared.region().map(|r| r.to_string()).unwrap_or_default();
        let mut builder = aws_sdk_sts::config::Builder::from(&shared);
        if let Some(url) = &opts.endpoint_url {
            builder = builder.endpoint_url(url);
        }
        let cfg = builder.build();
        Ok(Self {
            inner: StsSdkClient::from_conf(cfg),
            region,
        })
    }
}

#[async_trait]
impl IdentityClient for AwsStsClient {
    async fn caller_identity(&self) -> Result<CallerIdentity, anyhow::Error> {
        let resp = self.inner.get_caller_identity().send().await?;
        Ok(CallerIdentity {
            account_id: resp.account.unwrap_or_default(),
            region: self.region.clone(),
        })
    }
}

#[async_trait]
impl KinesisClient for AwsKinesisClient {
    async fn list_shards(&self, stream_name: &str) -> Result<Vec<ShardInfo>, anyhow::Error> {
        let mut out = Vec::new();
        let mut next_token: Option<String> = None;
        loop {
            // `next_token` and `stream_name` are mutually exclusive on
            // ListShards, so only set the stream name on the first request.
            let mut req = self.inner.list_shards();
            req = match &next_token {
                Some(tok) => req.next_token(tok),
                None => req.stream_name(stream_name),
            };
            let resp = req.send().await?;
            for shard in resp.shards.unwrap_or_default() {
                out.push(ShardInfo {
                    shard_id: shard.shard_id,
                });
            }
            match resp.next_token {
                Some(tok) => next_token = Some(tok),
                None => break,
            }
        }
        Ok(out)
    }

    async fn get_shard_iterator(
        &self,
        stream_name: &str,
        shard_id: &str,
        position: &ShardIteratorPosition,
    ) -> Result<Option<String>, anyhow::Error> {
        use aws_sdk_kinesis::primitives::DateTime;
        use aws_sdk_kinesis::types::ShardIteratorType;

        let mut req = self
            .inner
            .get_shard_iterator()
            .stream_name(stream_name)
            .shard_id(shard_id);

        req = match position {
            ShardIteratorPosition::TrimHorizon => {
                req.shard_iterator_type(ShardIteratorType::TrimHorizon)
            }
            ShardIteratorPosition::Latest => req.shard_iterator_type(ShardIteratorType::Latest),
            ShardIteratorPosition::AtTimestamp(ms) => req
                .shard_iterator_type(ShardIteratorType::AtTimestamp)
                .timestamp(DateTime::from_millis(*ms)),
        };

        let resp = req.send().await?;
        Ok(resp.shard_iterator)
    }

    async fn get_records(
        &self,
        shard_id: &str,
        shard_iterator: &str,
        limit: Option<i32>,
    ) -> Result<GetRecordsResponse, anyhow::Error> {
        let mut req = self.inner.get_records().shard_iterator(shard_iterator);
        if let Some(n) = limit {
            req = req.limit(n);
        }
        let resp = req.send().await?;

        let records = resp
            .records
            .into_iter()
            .map(|r| KinesisRecord {
                shard_id: shard_id.to_string(),
                sequence_number: r.sequence_number,
                partition_key: r.partition_key,
                approximate_arrival_timestamp: r
                    .approximate_arrival_timestamp
                    .map(|t| t.to_millis().unwrap_or_default()),
                data: r.data.into_inner(),
            })
            .collect();

        Ok(GetRecordsResponse {
            records,
            next_shard_iterator: resp.next_shard_iterator,
            millis_behind_latest: resp.millis_behind_latest,
        })
    }
}
