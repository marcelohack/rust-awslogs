//! Abstraction over the CloudWatch Logs SDK.
//!
//! The [`LogsClient`] trait is the seam tests mock against — equivalent to the
//! Python `@patch('awslogs.core.boto3_client')` pattern. Production code uses
//! [`AwsLogsClient`], a thin wrapper over `aws_sdk_cloudwatchlogs::Client`.

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_cloudwatchlogs::Client as SdkClient;
use aws_sdk_cloudwatchlogs::config::Region;

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

impl AwsLogsClient {
    pub async fn new(opts: &AwsCredentialOptions) -> anyhow::Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(profile) = &opts.profile {
            loader = loader.profile_name(profile);
        }
        if let Some(region) = &opts.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let (Some(key), Some(secret)) = (&opts.access_key_id, &opts.secret_access_key) {
            let creds =
                Credentials::new(key, secret, opts.session_token.clone(), None, "awslogs-cli");
            loader = loader.credentials_provider(creds);
        }
        if let Some(url) = &opts.endpoint_url {
            loader = loader.endpoint_url(url);
        }

        let shared = loader.load().await;
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
