//! CLI mirroring `awslogs/bin.py::main`.

use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use clap::{ArgAction, Args, CommandFactory, Parser, Subcommand, ValueEnum};

use crate::client::{AwsCredentialOptions, AwsLogsClient, LogsClient};
use crate::core::{AwsLogs, AwsLogsConfig, Color, ColorPreference, ansi_colored};
use crate::exceptions::AwsLogsError;
use crate::time::parse_datetime;

/// Read AWS CloudWatch logs from the command line.
#[derive(Debug, Parser)]
#[command(
    name = "awslogs",
    version,
    about = "awslogs [ get | groups | streams ]",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Get logs
    Get(GetArgs),
    /// List groups
    Groups(GroupsArgs),
    /// List streams
    Streams(StreamsArgs),
}

#[derive(Debug, Args, Clone)]
pub struct CommonAwsArgs {
    /// aws access key id
    #[arg(long = "aws-access-key-id", value_name = "AWS_ACCESS_KEY_ID")]
    pub aws_access_key_id: Option<String>,

    /// aws secret access key
    #[arg(long = "aws-secret-access-key", value_name = "AWS_SECRET_ACCESS_KEY")]
    pub aws_secret_access_key: Option<String>,

    /// aws session token
    #[arg(long = "aws-session-token", value_name = "AWS_SESSION_TOKEN")]
    pub aws_session_token: Option<String>,

    /// aws profile
    #[arg(long = "profile", env = "AWS_PROFILE")]
    pub aws_profile: Option<String>,

    /// aws region
    #[arg(long = "aws-region", env = "AWS_REGION")]
    pub aws_region: Option<String>,

    /// aws endpoint url to services such localstack, fakes3, others
    #[arg(long = "aws-endpoint-url", env = "AWS_ENDPOINT_URL")]
    pub aws_endpoint_url: Option<String>,
}

impl CommonAwsArgs {
    fn into_credential_options(self) -> AwsCredentialOptions {
        AwsCredentialOptions {
            profile: self.aws_profile,
            region: self.aws_region,
            access_key_id: self.aws_access_key_id,
            secret_access_key: self.aws_secret_access_key,
            session_token: self.aws_session_token,
            endpoint_url: self.aws_endpoint_url,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ColorWhen {
    Auto,
    Always,
    Never,
}

impl From<ColorWhen> for ColorPreference {
    fn from(value: ColorWhen) -> Self {
        match value {
            ColorWhen::Auto => ColorPreference::Auto,
            ColorWhen::Always => ColorPreference::Always,
            ColorWhen::Never => ColorPreference::Never,
        }
    }
}

#[derive(Debug, Args)]
pub struct GetArgs {
    /// log group name
    #[arg(default_value = "ALL")]
    pub log_group_name: String,

    /// log stream name
    #[arg(default_value = "ALL")]
    pub log_stream_name: String,

    /// A valid CloudWatch Logs filter pattern to use for filtering the response.
    #[arg(short = 'f', long = "filter-pattern")]
    pub filter_pattern: Option<String>,

    /// Query for new log lines constantly
    #[arg(short = 'w', long = "watch", action = ArgAction::SetTrue)]
    pub watch: bool,

    /// Interval in seconds at which to query for new log lines
    #[arg(short = 'i', long = "watch-interval", default_value_t = 1)]
    pub watch_interval: u64,

    /// Do not display group name
    #[arg(short = 'G', long = "no-group", action = ArgAction::SetFalse)]
    pub output_group_enabled: bool,

    /// Do not display stream name
    #[arg(short = 'S', long = "no-stream", action = ArgAction::SetFalse)]
    pub output_stream_enabled: bool,

    /// Add creation timestamp to the output
    #[arg(long = "timestamp", action = ArgAction::SetTrue)]
    pub output_timestamp_enabled: bool,

    /// Add ingestion time to the output
    #[arg(long = "ingestion-time", action = ArgAction::SetTrue)]
    pub output_ingestion_time_enabled: bool,

    /// Start time (default 5m)
    #[arg(short = 's', long = "start", default_value = "5m")]
    pub start: String,

    /// End time
    #[arg(short = 'e', long = "end")]
    pub end: Option<String>,

    /// When to color output: auto (default), never, always.
    #[arg(long = "color", value_name = "WHEN", default_value_t = ColorWhen::Auto, value_enum)]
    pub color: ColorWhen,

    /// JMESPath query to use in filtering the response data
    #[arg(short = 'q', long = "query")]
    pub query: Option<String>,

    #[command(flatten)]
    pub common: CommonAwsArgs,
}

#[derive(Debug, Args)]
pub struct GroupsArgs {
    /// List only groups matching the prefix
    #[arg(short = 'p', long = "log-group-prefix")]
    pub log_group_prefix: Option<String>,

    #[command(flatten)]
    pub common: CommonAwsArgs,
}

#[derive(Debug, Args)]
pub struct StreamsArgs {
    /// log group name
    pub log_group_name: String,

    /// Start time (default 1h)
    #[arg(short = 's', long = "start", default_value = "1h")]
    pub start: String,

    /// End time
    #[arg(short = 'e', long = "end")]
    pub end: Option<String>,

    #[command(flatten)]
    pub common: CommonAwsArgs,
}

/// Production entry point invoked from `main`.
pub async fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(c) => c,
        Err(err) => err.exit(),
    };

    let Some(command) = cli.command else {
        let mut help = Cli::command();
        let _ = help.print_help();
        println!();
        return 1;
    };

    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    let factory: ClientFactory = Box::new(|common: CommonAwsArgs| {
        Box::pin(async move {
            let opts = common.into_credential_options();
            let client = AwsLogsClient::new(&opts).await.map_err(AwsLogsError::Aws)?;
            let arc: Arc<dyn LogsClient> = Arc::new(client);
            Ok(arc)
        })
    });
    execute(command, &mut stdout, &mut stderr, factory).await
}

/// Boxed async factory returning a [`LogsClient`] — abstracted so integration
/// tests can substitute a mock without going through `aws-config`.
pub type ClientFactory = Box<
    dyn FnOnce(
            CommonAwsArgs,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<Arc<dyn LogsClient>, AwsLogsError>> + Send>,
        > + Send,
>;

/// Run a parsed [`Command`] writing output to `stdout` and errors to `stderr`.
/// Returns the same exit code the Python CLI would have produced.
pub async fn execute<O: Write, E: Write>(
    command: Command,
    stdout: &mut O,
    stderr: &mut E,
    make_client: ClientFactory,
) -> i32 {
    let result: Result<(), AwsLogsError> = match command {
        Command::Get(args) => run_get(args, stdout, make_client).await,
        Command::Groups(args) => run_groups(args, stdout, make_client).await,
        Command::Streams(args) => run_streams(args, stdout, make_client).await,
    };
    handle_result(result, stderr)
}

fn handle_result<E: Write>(result: Result<(), AwsLogsError>, stderr: &mut E) -> i32 {
    match result {
        Ok(()) => 0,
        Err(err) => {
            let code = err.code();
            let msg = err.hint();
            if let AwsLogsError::Aws(inner) = &err
                && let Some(hint) = aws_access_or_expired_hint(inner)
            {
                let _ = write!(
                    stderr,
                    "{}",
                    ansi_colored(&format!("{hint}\n"), Color::Yellow)
                );
                return 4;
            }
            // For SDK errors the top-level Display is terse (e.g. "dispatch
            // failure"); surface the full source chain so the real cause is
            // visible. Our own typed errors have no extra chain, so this is a
            // no-op for them.
            let detail = match &err {
                AwsLogsError::Aws(inner) => full_chain(inner),
                _ => msg,
            };
            // Match Python: `colored(msg + "\n", "red")` — the newline is INSIDE
            // the color escape, no separate trailing newline.
            let _ = write!(stderr, "{}", ansi_colored(&format!("{detail}\n"), Color::Red));
            code
        }
    }
}

/// Render an error and its full source chain as "top: cause: root-cause".
fn full_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join(": ")
}

fn aws_access_or_expired_hint(err: &anyhow::Error) -> Option<String> {
    let chain: Vec<String> = err.chain().map(|e| e.to_string()).collect();
    let joined = chain.join("\n");
    for marker in ["AccessDeniedException", "ExpiredTokenException"] {
        if joined.contains(marker) {
            return Some(joined);
        }
    }
    None
}

async fn run_get<O: Write>(
    args: GetArgs,
    writer: &mut O,
    make_client: ClientFactory,
) -> Result<(), AwsLogsError> {
    let start = parse_datetime(Some(&args.start))?;
    let end = match args.end {
        Some(s) => parse_datetime(Some(&s))?,
        None => None,
    };
    let cfg = AwsLogsConfig {
        log_group_name: Some(args.log_group_name),
        log_stream_name: Some(args.log_stream_name),
        filter_pattern: args.filter_pattern,
        start,
        end,
        watch: args.watch,
        watch_interval: Duration::from_secs(args.watch_interval),
        color: args.color.into(),
        output_group_enabled: args.output_group_enabled,
        output_stream_enabled: args.output_stream_enabled,
        output_timestamp_enabled: args.output_timestamp_enabled,
        output_ingestion_time_enabled: args.output_ingestion_time_enabled,
        query: args.query,
        ..Default::default()
    };
    let client = make_client(args.common).await?;
    AwsLogs::new(cfg, client)?.list_logs_into(writer).await
}

async fn run_groups<O: Write>(
    args: GroupsArgs,
    writer: &mut O,
    make_client: ClientFactory,
) -> Result<(), AwsLogsError> {
    let cfg = AwsLogsConfig {
        log_group_prefix: args.log_group_prefix,
        ..Default::default()
    };
    let client = make_client(args.common).await?;
    AwsLogs::new(cfg, client)?.list_groups_into(writer).await
}

async fn run_streams<O: Write>(
    args: StreamsArgs,
    writer: &mut O,
    make_client: ClientFactory,
) -> Result<(), AwsLogsError> {
    let start = parse_datetime(Some(&args.start))?;
    let end = match args.end {
        Some(s) => parse_datetime(Some(&s))?,
        None => None,
    };
    let cfg = AwsLogsConfig {
        log_group_name: Some(args.log_group_name),
        start,
        end,
        ..Default::default()
    };
    let client = make_client(args.common).await?;
    AwsLogs::new(cfg, client)?.list_streams_into(writer).await
}
