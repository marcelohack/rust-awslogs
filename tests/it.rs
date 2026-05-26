//! Integration tests ported from `awslogs/tests/test_it.py::TestAWSLogs`.
//!
//! Every test uses the [`MockLogsClient`] so no real AWS call is made — matching
//! the Python pattern of `@patch('awslogs.core.boto3_client')`.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use awslogs::cli::{Cli, Command, CommonAwsArgs};
use awslogs::client::{
    FilterParams, FilterResponse, GetRecordsResponse, KinesisClient, KinesisRecord, LogEvent,
    LogsClient, ShardInfo, ShardIteratorPosition, StreamMeta,
};
use awslogs::core::{
    ALL_WILDCARD, AwsLogs, AwsLogsConfig, ColorPreference, filter_streams_by_window,
};
use clap::Parser;

// ─────────────────────────── mock client ──────────────────────────────────────

#[derive(Default)]
pub struct MockLogsClient {
    groups: Mutex<Vec<String>>,
    streams_for_group: Mutex<HashMap<String, Vec<StreamMeta>>>,
    default_streams: Mutex<Vec<StreamMeta>>,
    filter_responses: Mutex<VecDeque<FilterResponse>>,
    recorded_prefix: Mutex<Option<Option<String>>>,
}

impl MockLogsClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_groups(self, groups: impl IntoIterator<Item = &'static str>) -> Self {
        *self.groups.lock().unwrap() = groups.into_iter().map(String::from).collect();
        self
    }

    pub fn with_default_streams(self, streams: Vec<StreamMeta>) -> Self {
        *self.default_streams.lock().unwrap() = streams;
        self
    }

    pub fn with_streams_for_group(self, group: &str, streams: Vec<StreamMeta>) -> Self {
        self.streams_for_group
            .lock()
            .unwrap()
            .insert(group.to_string(), streams);
        self
    }

    pub fn with_filter_responses(self, responses: Vec<FilterResponse>) -> Self {
        *self.filter_responses.lock().unwrap() = responses.into();
        self
    }

    pub fn recorded_prefix(&self) -> Option<String> {
        self.recorded_prefix.lock().unwrap().clone().unwrap_or(None)
    }
}

#[async_trait]
impl LogsClient for MockLogsClient {
    async fn describe_log_groups(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, anyhow::Error> {
        *self.recorded_prefix.lock().unwrap() = Some(prefix.map(String::from));
        Ok(self.groups.lock().unwrap().clone())
    }

    async fn describe_log_streams(
        &self,
        log_group_name: &str,
    ) -> Result<Vec<StreamMeta>, anyhow::Error> {
        if let Some(specific) = self.streams_for_group.lock().unwrap().get(log_group_name) {
            return Ok(specific.clone());
        }
        Ok(self.default_streams.lock().unwrap().clone())
    }

    async fn filter_log_events(
        &self,
        _params: &FilterParams,
        _next_token: Option<&str>,
    ) -> Result<FilterResponse, anyhow::Error> {
        Ok(self
            .filter_responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_default())
    }
}

// ──────────────────────── mock kinesis client ─────────────────────────────────

#[derive(Default)]
pub struct MockKinesisClient {
    shards: Mutex<Vec<String>>,
    records: Mutex<HashMap<String, Vec<KinesisRecord>>>,
}

impl MockKinesisClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_shards(self, shards: impl IntoIterator<Item = &'static str>) -> Self {
        *self.shards.lock().unwrap() = shards.into_iter().map(String::from).collect();
        self
    }

    pub fn with_records(self, shard_id: &str, records: Vec<KinesisRecord>) -> Self {
        self.records
            .lock()
            .unwrap()
            .insert(shard_id.to_string(), records);
        self
    }
}

#[async_trait]
impl KinesisClient for MockKinesisClient {
    async fn list_shards(&self, _stream_name: &str) -> Result<Vec<ShardInfo>, anyhow::Error> {
        Ok(self
            .shards
            .lock()
            .unwrap()
            .iter()
            .map(|shard_id| ShardInfo {
                shard_id: shard_id.clone(),
            })
            .collect())
    }

    async fn get_shard_iterator(
        &self,
        _stream_name: &str,
        shard_id: &str,
        _position: &ShardIteratorPosition,
    ) -> Result<Option<String>, anyhow::Error> {
        // The iterator token is just the shard id; one GetRecords call drains it.
        Ok(Some(shard_id.to_string()))
    }

    async fn get_records(
        &self,
        shard_id: &str,
        _shard_iterator: &str,
        _limit: Option<i32>,
    ) -> Result<GetRecordsResponse, anyhow::Error> {
        let records = self
            .records
            .lock()
            .unwrap()
            .get(shard_id)
            .cloned()
            .unwrap_or_default();
        Ok(GetRecordsResponse {
            records,
            next_shard_iterator: None,
            millis_behind_latest: Some(0),
        })
    }
}

fn krecord(shard_id: &str, ts: i64, data: &str) -> KinesisRecord {
    KinesisRecord {
        shard_id: shard_id.to_string(),
        sequence_number: "0".to_string(),
        partition_key: "pk".to_string(),
        approximate_arrival_timestamp: Some(ts),
        data: data.as_bytes().to_vec(),
    }
}

// ────────────────────────── small helpers ─────────────────────────────────────

fn stream(name: &str) -> StreamMeta {
    StreamMeta {
        name: name.to_string(),
        first_event_timestamp: Some(0),
        last_ingestion_time: Some(i64::MAX),
    }
}

fn stream_with(name: &str, first: i64, ingestion: i64) -> StreamMeta {
    StreamMeta {
        name: name.to_string(),
        first_event_timestamp: Some(first),
        last_ingestion_time: Some(ingestion),
    }
}

fn event(id: u32, timestamp: i64, ingestion: i64, message: &str, stream: &str) -> LogEvent {
    LogEvent {
        event_id: id.to_string(),
        timestamp,
        ingestion_time: ingestion,
        message: message.to_string(),
        log_stream_name: stream.to_string(),
    }
}

/// Builds the standard 6-event ABCDE fixture from the Python tests.
fn abcde_filter_responses() -> Vec<FilterResponse> {
    vec![
        FilterResponse {
            events: vec![
                event(1, 0, 5000, "Hello 1", "DDD"),
                event(2, 0, 5000, "Hello 2", "EEE"),
                event(3, 0, 5006, "Hello 3 👎", "DDD"),
            ],
            next_token: Some("token".into()),
        },
        FilterResponse {
            events: vec![
                event(4, 0, 5000, "Hello 4", "EEE"),
                event(5, 0, 5000, "Hello 5", "DDD"),
                event(6, 0, 5009, "Hello 6 👍", "EEE"),
            ],
            next_token: Some("token".into()),
        },
        FilterResponse {
            events: vec![],
            next_token: None,
        },
    ]
}

fn abcde_client() -> Arc<MockLogsClient> {
    let mock = MockLogsClient::new()
        .with_groups(["AAA", "BBB", "CCC"])
        .with_default_streams(vec![stream("DDD"), stream("EEE")])
        .with_filter_responses(abcde_filter_responses());
    Arc::new(mock)
}

/// Parse a Python-style argv (incl. argv[0]) and return the [`Command`].
fn parse_argv(argv: &[&str]) -> Command {
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    cli.command.expect("subcommand provided")
}

/// Run a parsed [`Command`] through the CLI pipeline with a mock client.
/// Returns (exit_code, stdout_bytes, stderr_bytes).
async fn run_cli(argv: &[&str], client: Arc<dyn LogsClient>) -> (i32, Vec<u8>, Vec<u8>) {
    use awslogs::cli::{ClientFactory, KinesisClientFactory, execute};

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let command = parse_argv(argv);
    let factory: ClientFactory = Box::new(move |_common: CommonAwsArgs| {
        let client = client.clone();
        Box::pin(async move { Ok(client) })
    });
    // Log commands never invoke the Kinesis factory; supply one that panics if
    // it ever runs so a routing mistake is caught loudly.
    let kinesis: KinesisClientFactory = Box::new(|_common: CommonAwsArgs| {
        Box::pin(async move { panic!("log command unexpectedly built a Kinesis client") })
    });
    let code = execute(command, &mut stdout, &mut stderr, factory, kinesis).await;
    (code, stdout, stderr)
}

/// Kinesis counterpart to [`run_cli`]: routes a `kinesis ...` argv through the
/// CLI with a mock Kinesis client.
async fn run_kinesis_cli(argv: &[&str], client: Arc<dyn KinesisClient>) -> (i32, Vec<u8>, Vec<u8>) {
    use awslogs::cli::{ClientFactory, KinesisClientFactory, execute};

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let command = parse_argv(argv);
    let logs: ClientFactory = Box::new(|_common: CommonAwsArgs| {
        Box::pin(async move { panic!("kinesis command unexpectedly built a logs client") })
    });
    let kinesis: KinesisClientFactory = Box::new(move |_common: CommonAwsArgs| {
        let client = client.clone();
        Box::pin(async move { Ok(client) })
    });
    let code = execute(command, &mut stdout, &mut stderr, logs, kinesis).await;
    (code, stdout, stderr)
}

// ────────────────────────── port: TestAWSLogs ────────────────────────────────

#[tokio::test]
async fn test_get_groups() {
    let mock = Arc::new(MockLogsClient::new().with_groups(["A", "B", "C", "D", "E", "F", "G"]));
    let logs = AwsLogs::new(AwsLogsConfig::default(), mock.clone()).unwrap();
    assert_eq!(
        logs.get_groups().await.unwrap(),
        vec!["A", "B", "C", "D", "E", "F", "G"]
    );
}

#[tokio::test]
async fn test_get_groups_with_log_group_prefix() {
    let mock = Arc::new(MockLogsClient::new().with_groups(["A"]));
    let cfg = AwsLogsConfig {
        log_group_prefix: Some("log_group_prefix".into()),
        ..Default::default()
    };
    let logs = AwsLogs::new(cfg, mock.clone()).unwrap();
    assert_eq!(logs.get_groups().await.unwrap(), vec!["A"]);
    assert_eq!(mock.recorded_prefix().as_deref(), Some("log_group_prefix"));
}

#[tokio::test]
async fn test_get_streams() {
    let mock = Arc::new(MockLogsClient::new().with_default_streams(vec![
        stream("A"),
        stream("B"),
        stream("C"),
        stream("D"),
        stream("E"),
        stream("F"),
        stream("G"),
    ]));
    let cfg = AwsLogsConfig {
        log_group_name: Some("group".into()),
        ..Default::default()
    };
    let logs = AwsLogs::new(cfg, mock).unwrap();
    assert_eq!(
        logs.get_streams(None).await.unwrap(),
        vec!["A", "B", "C", "D", "E", "F", "G"]
    );
}

#[tokio::test]
async fn test_get_streams_filtered_by_date() {
    let metas = vec![
        // A: first=0, ingestion=1
        stream_with("A", 0, 1),
        // B: first=0, ingestion=6
        stream_with("B", 0, 6),
        // C: defaults (first=0, ingestion=i64::MAX)
        stream("C"),
        // D: first=MAX-1, ingestion=MAX
        stream_with("D", i64::MAX - 1, i64::MAX),
        // E: first=0, ingestion=5  (Python uses ingestion=5, end=4; we only
        //    keep first/ingestion — `last_event_timestamp` isn't consulted in
        //    Python's filter either, so dropping it preserves behavior.)
        stream_with("E", 0, 5),
    ];
    let result = filter_streams_by_window(&metas, Some(5), Some(7));
    assert_eq!(result, vec!["B", "C", "E"]);
}

#[tokio::test]
async fn test_streams_matching() {
    let streams = vec![
        stream("AAA"),
        stream("ABA"),
        stream("ACA"),
        stream("BAA"),
        stream("BBA"),
        stream("BBB"),
        stream("CAC"),
    ];
    let mock = Arc::new(
        MockLogsClient::new()
            .with_streams_for_group("X", streams.clone())
            .with_default_streams(streams),
    );
    let logs = AwsLogs::new(AwsLogsConfig::default(), mock).unwrap();

    assert_eq!(
        logs.streams_matching("X", ALL_WILDCARD).await.unwrap(),
        vec!["AAA", "ABA", "ACA", "BAA", "BBA", "BBB", "CAC"]
    );
    assert_eq!(
        logs.streams_matching("X", "A").await.unwrap(),
        vec!["AAA", "ABA", "ACA"]
    );
    assert_eq!(
        logs.streams_matching("X", "A[AC]A").await.unwrap(),
        vec!["AAA", "ACA"]
    );
}

#[tokio::test]
async fn test_main_get() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &["awslogs", "get", "AAA", "DDD", "--color=never"],
        mock.clone(),
    )
    .await;
    let expected = concat!(
        "AAA DDD Hello 1\n",
        "AAA EEE Hello 2\n",
        "AAA DDD Hello 3 \u{1f44e}\n",
        "AAA EEE Hello 4\n",
        "AAA DDD Hello 5\n",
        "AAA EEE Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_main_get_with_color() {
    let mock = abcde_client();
    let (code, stdout, _) =
        run_cli(&["awslogs", "get", "AAA", "DDD", "--color=always"], mock).await;
    let expected = concat!(
        "\x1b[32mAAA\x1b[0m \x1b[36mDDD\x1b[0m Hello 1\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mEEE\x1b[0m Hello 2\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mDDD\x1b[0m Hello 3 \u{1f44e}\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mEEE\x1b[0m Hello 4\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mDDD\x1b[0m Hello 5\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mEEE\x1b[0m Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_main_get_query() {
    let mock = Arc::new(
        MockLogsClient::new()
            .with_groups(["AAA", "BBB", "CCC"])
            .with_default_streams(vec![stream("DDD"), stream("EEE")])
            .with_filter_responses(vec![
                FilterResponse {
                    events: vec![
                        event(1, 0, 5000, r#"{"foo": "bar"}"#, "DDD"),
                        event(2, 0, 5000, r#"{"foo": {"bar": "baz"}}"#, "EEE"),
                        event(3, 0, 5006, "Hello 3 \u{1f44e}", "DDD"),
                    ],
                    next_token: Some("token".into()),
                },
                FilterResponse::default(),
            ]),
    );
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "AAA",
            "DDD",
            "--query",
            "foo",
            "--color=always",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "\x1b[32mAAA\x1b[0m \x1b[36mDDD\x1b[0m bar\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mEEE\x1b[0m {\"bar\":\"baz\"}\n",
        "\x1b[32mAAA\x1b[0m \x1b[36mDDD\x1b[0m Hello 3 \u{1f44e}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_nogroup() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "--no-group",
            "AAA",
            "DDD",
            "--color=never",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "DDD Hello 1\n",
        "EEE Hello 2\n",
        "DDD Hello 3 \u{1f44e}\n",
        "EEE Hello 4\n",
        "DDD Hello 5\n",
        "EEE Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_nostream() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "--no-stream",
            "AAA",
            "DDD",
            "--color=never",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "AAA Hello 1\n",
        "AAA Hello 2\n",
        "AAA Hello 3 \u{1f44e}\n",
        "AAA Hello 4\n",
        "AAA Hello 5\n",
        "AAA Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_nogroup_nostream() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "--no-group",
            "--no-stream",
            "AAA",
            "DDD",
            "--color=never",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "Hello 1\n",
        "Hello 2\n",
        "Hello 3 \u{1f44e}\n",
        "Hello 4\n",
        "Hello 5\n",
        "Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_nogroup_nostream_short_forms() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &["awslogs", "get", "-GS", "AAA", "DDD", "--color=never"],
        mock,
    )
    .await;
    let expected = concat!(
        "Hello 1\n",
        "Hello 2\n",
        "Hello 3 \u{1f44e}\n",
        "Hello 4\n",
        "Hello 5\n",
        "Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_timestamp() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "--timestamp",
            "--no-group",
            "--no-stream",
            "AAA",
            "DDD",
            "--color=never",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "1970-01-01T00:00:00.000Z Hello 1\n",
        "1970-01-01T00:00:00.000Z Hello 2\n",
        "1970-01-01T00:00:00.000Z Hello 3 \u{1f44e}\n",
        "1970-01-01T00:00:00.000Z Hello 4\n",
        "1970-01-01T00:00:00.000Z Hello 5\n",
        "1970-01-01T00:00:00.000Z Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_ingestion_time() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "--ingestion-time",
            "--no-group",
            "--no-stream",
            "AAA",
            "DDD",
            "--color=never",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "1970-01-01T00:00:05.000Z Hello 1\n",
        "1970-01-01T00:00:05.000Z Hello 2\n",
        "1970-01-01T00:00:05.006Z Hello 3 \u{1f44e}\n",
        "1970-01-01T00:00:05.000Z Hello 4\n",
        "1970-01-01T00:00:05.000Z Hello 5\n",
        "1970-01-01T00:00:05.009Z Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_get_timestamp_and_ingestion_time() {
    let mock = abcde_client();
    let (code, stdout, _) = run_cli(
        &[
            "awslogs",
            "get",
            "--timestamp",
            "--ingestion-time",
            "--no-group",
            "--no-stream",
            "AAA",
            "DDD",
            "--color=never",
        ],
        mock,
    )
    .await;
    let expected = concat!(
        "1970-01-01T00:00:00.000Z 1970-01-01T00:00:05.000Z Hello 1\n",
        "1970-01-01T00:00:00.000Z 1970-01-01T00:00:05.000Z Hello 2\n",
        "1970-01-01T00:00:00.000Z 1970-01-01T00:00:05.006Z Hello 3 \u{1f44e}\n",
        "1970-01-01T00:00:00.000Z 1970-01-01T00:00:05.000Z Hello 4\n",
        "1970-01-01T00:00:00.000Z 1970-01-01T00:00:05.000Z Hello 5\n",
        "1970-01-01T00:00:00.000Z 1970-01-01T00:00:05.009Z Hello 6 \u{1f44d}\n",
    );
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_main_get_deduplication() {
    // Same events repeated across two pages — should dedup by event_id.
    let mock = Arc::new(
        MockLogsClient::new()
            .with_groups(["AAA", "BBB", "CCC"])
            .with_default_streams(vec![stream("DDD"), stream("EEE")])
            .with_filter_responses(vec![
                FilterResponse {
                    events: vec![
                        event(1, 0, 0, "Hello 1", "DDD"),
                        event(2, 0, 0, "Hello 2", "EEE"),
                        event(3, 0, 0, "Hello 3", "DDD"),
                    ],
                    next_token: Some("token".into()),
                },
                FilterResponse {
                    events: vec![
                        event(1, 0, 0, "Hello 1", "EEE"),
                        event(2, 0, 0, "Hello 2", "DDD"),
                        event(3, 0, 0, "Hello 3", "EEE"),
                    ],
                    next_token: Some("token".into()),
                },
                FilterResponse::default(),
            ]),
    );
    let (code, stdout, _) = run_cli(&["awslogs", "get", "AAA", "DDD", "--color=never"], mock).await;
    let expected = "AAA DDD Hello 1\nAAA EEE Hello 2\nAAA DDD Hello 3\n";
    assert_eq!(String::from_utf8(stdout).unwrap(), expected);
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_main_get_no_matching_streams() {
    let mock = Arc::new(
        MockLogsClient::new()
            .with_groups(["AAA"])
            .with_default_streams(vec![stream("DDD"), stream("EEE")]),
    );
    let (code, _stdout, stderr) = run_cli(&["awslogs", "get", "AAA", "foo.*"], mock).await;
    let expected =
        "\x1b[31mNo streams match your pattern 'foo.*' for the given time period.\n\x1b[0m";
    assert_eq!(String::from_utf8(stderr).unwrap(), expected);
    assert_eq!(code, 7);
}

#[tokio::test]
async fn test_main_groups() {
    let mock = Arc::new(MockLogsClient::new().with_groups(["AAA", "BBB", "CCC"]));
    let (code, stdout, _) = run_cli(&["awslogs", "groups"], mock).await;
    assert_eq!(String::from_utf8(stdout).unwrap(), "AAA\nBBB\nCCC\n");
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_main_streams() {
    let mock = Arc::new(
        MockLogsClient::new()
            .with_groups(["AAA", "BBB", "CCC"])
            .with_default_streams(vec![stream("DDD"), stream("EEE")]),
    );
    let (code, stdout, _) = run_cli(&["awslogs", "streams", "AAA"], mock).await;
    assert_eq!(String::from_utf8(stdout).unwrap(), "DDD\nEEE\n");
    assert_eq!(code, 0);
}

#[tokio::test]
async fn test_unknown_date_error() {
    let mock = Arc::new(MockLogsClient::new());
    let (code, _stdout, stderr) = run_cli(&["awslogs", "get", "AAA", "BBB", "-sX"], mock).await;
    let expected = "\x1b[31mawslogs doesn't understand 'X' as a date.\n\x1b[0m";
    assert_eq!(String::from_utf8(stderr).unwrap(), expected);
    assert_eq!(code, 3);
}

// ───────────────── extras: TooManyStreams + auto-color preference ────────────

#[tokio::test]
async fn test_too_many_streams_returns_code_6() {
    let streams: Vec<StreamMeta> = (0..200).map(|i| stream(&format!("A{i:03}"))).collect();
    let mock = Arc::new(
        MockLogsClient::new()
            .with_groups(["AAA"])
            .with_default_streams(streams),
    );
    let (code, _stdout, stderr) = run_cli(&["awslogs", "get", "AAA", "A"], mock).await;
    let s = String::from_utf8(stderr).unwrap();
    assert!(s.contains("AWS API limits the number of streams"));
    assert_eq!(code, 6);
}

#[tokio::test]
async fn test_color_preference_default_is_auto() {
    assert_eq!(AwsLogsConfig::default().color, ColorPreference::Auto);
}

// ─────────────────────────────── kinesis ──────────────────────────────────────

fn kinesis_two_shard_client() -> Arc<MockKinesisClient> {
    Arc::new(
        MockKinesisClient::new()
            .with_shards(["shardId-0", "shardId-1"])
            .with_records(
                "shardId-0",
                vec![
                    krecord("shardId-0", 1000, "hello from zero"),
                    krecord("shardId-0", 3000, "error: boom on zero"),
                ],
            )
            .with_records(
                "shardId-1",
                vec![
                    krecord("shardId-1", 2000, "hello from one"),
                    krecord("shardId-1", 4000, "error: boom on one"),
                ],
            ),
    )
}

#[tokio::test]
async fn test_kinesis_shards_lists_every_shard() {
    let client = kinesis_two_shard_client();
    let (code, stdout, _stderr) =
        run_kinesis_cli(&["awslogs", "kinesis", "shards", "my-stream"], client).await;
    assert_eq!(code, 0);
    assert_eq!(String::from_utf8(stdout).unwrap(), "shardId-0\nshardId-1\n");
}

#[tokio::test]
async fn test_kinesis_search_reads_all_shards() {
    let client = kinesis_two_shard_client();
    let (code, stdout, _stderr) = run_kinesis_cli(
        &["awslogs", "kinesis", "search", "my-stream", "--no-shard"],
        client,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        "hello from zero\nerror: boom on zero\nhello from one\nerror: boom on one\n"
    );
}

#[tokio::test]
async fn test_kinesis_search_substring_filter() {
    let client = kinesis_two_shard_client();
    let (code, stdout, _stderr) = run_kinesis_cli(
        &[
            "awslogs",
            "kinesis",
            "search",
            "my-stream",
            "--no-shard",
            "-f",
            "error:",
        ],
        client,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        "error: boom on zero\nerror: boom on one\n"
    );
}

#[tokio::test]
async fn test_kinesis_search_regex_filter() {
    let client = kinesis_two_shard_client();
    let (code, stdout, _stderr) = run_kinesis_cli(
        &[
            "awslogs",
            "kinesis",
            "search",
            "my-stream",
            "--no-shard",
            "--regex",
            "-f",
            "boom on (zero|one)$",
        ],
        client,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        "error: boom on zero\nerror: boom on one\n"
    );
}

#[tokio::test]
async fn test_kinesis_search_includes_shard_id_by_default() {
    let client = Arc::new(
        MockKinesisClient::new()
            .with_shards(["shardId-0"])
            .with_records("shardId-0", vec![krecord("shardId-0", 1000, "payload")]),
    );
    let (code, stdout, _stderr) =
        run_kinesis_cli(&["awslogs", "kinesis", "search", "my-stream"], client).await;
    assert_eq!(code, 0);
    assert_eq!(String::from_utf8(stdout).unwrap(), "shardId-0 payload\n");
}

#[tokio::test]
async fn test_kinesis_search_end_time_stops_reading() {
    let client = kinesis_two_shard_client();
    // End at 2s: keeps the 1000ms/2000ms records, drops the 3000ms/4000ms ones.
    let (code, stdout, _stderr) = run_kinesis_cli(
        &[
            "awslogs",
            "kinesis",
            "search",
            "my-stream",
            "--no-shard",
            "-e",
            "1970-01-01T00:00:02Z",
        ],
        client,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(
        String::from_utf8(stdout).unwrap(),
        "hello from zero\nhello from one\n"
    );
}

#[tokio::test]
async fn test_kinesis_search_jmespath_query_on_json() {
    let client = Arc::new(
        MockKinesisClient::new()
            .with_shards(["shardId-0"])
            .with_records(
                "shardId-0",
                vec![krecord(
                    "shardId-0",
                    1000,
                    r#"{"level":"error","msg":"disk full"}"#,
                )],
            ),
    );
    let (code, stdout, _stderr) = run_kinesis_cli(
        &[
            "awslogs",
            "kinesis",
            "search",
            "my-stream",
            "--no-shard",
            "-q",
            "msg",
        ],
        client,
    )
    .await;
    assert_eq!(code, 0);
    assert_eq!(String::from_utf8(stdout).unwrap(), "disk full\n");
}
