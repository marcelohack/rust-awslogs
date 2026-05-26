# awslogs (Rust)

A Rust port of [`jorgebastida/awslogs`](https://github.com/jorgebastida/awslogs) — a small command-line tool for querying groups, streams, and events from [Amazon CloudWatch Logs](https://aws.amazon.com/cloudwatch/), extended with a `kinesis` command for searching [Kinesis data streams](#kinesis-data-streams).

This is a single-binary reimplementation built on the [AWS SDK for Rust](https://aws.amazon.com/sdk-for-rust/) (`aws-sdk-cloudwatchlogs` and `aws-sdk-kinesis`). The CLI surface, exit codes, color output, dedup behavior, and time expressions are kept byte-for-byte compatible with the Python original wherever possible; the integration test suite is ported from upstream so this binary passes the same assertions.

One of the most powerful features is querying events from several streams and consuming them (ordered) in pseudo-realtime with your favourite tools such as `grep`:

```bash
awslogs get /var/log/syslog 'ip-10-1.*' --start='2h ago' | grep ERROR
```

---

## Install

### From source (any platform)

Requires Rust 1.85+ (edition 2024 — local toolchain tested with 1.95).

```bash
git clone <this repo>
cd rust-awslogs
cargo install --path .          # installs `awslogs` to ~/.cargo/bin
# or just build it:
cargo build --release           # binary at target/release/awslogs
```

### Run without installing

```bash
cargo run --release -- get /var/log/syslog ALL --start='1h ago'
```

---

## Commands

```text
awslogs [ get | groups | streams | kinesis ]
```

* `awslogs groups` — list existing groups
* `awslogs streams GROUP` — list existing streams within `GROUP`
* `awslogs get GROUP [STREAM_EXPRESSION]` — get logs matching `STREAM_EXPRESSION` in `GROUP`.
  * Expressions are regular expressions (anchored at the start), or the literal `ALL` as a shortcut for `.*`.
* `awslogs kinesis shards STREAM` — list the shards of a Kinesis data stream
* `awslogs kinesis search STREAM` — read and search records across every shard of a Kinesis data stream (see [Kinesis data streams](#kinesis-data-streams))

You must supply a region via `--aws-region` or the `AWS_REGION` env var (or have one configured in your AWS profile).

---

## Usage examples

```bash
# All groups
awslogs groups
awslogs groups --log-group-prefix /aws/lambda/

# Streams in a group, created in the last hour (default for `streams`)
awslogs streams /aws/lambda/my-function

# Tail all streams in a group in real time
awslogs get /aws/lambda/my-function ALL --watch

# Stream-name regex
awslogs get /var/log/syslog 'ip-10-1.*' --start='2h ago'

# Only error rows in the last day, via a CloudWatch filter pattern
awslogs get /aws/lambda/my-function --filter-pattern='ERROR' -s1d

# Strip group and stream columns, add a timestamp
awslogs get -GS --timestamp /aws/lambda/my-function ALL -s10m
```

---

## Time options (`--start` / `--end`)

Relative or absolute. Time is parsed in UTC.

| Form | Example |
|---|---|
| Minutes | `--start=2m`, `--start='1 minute'`, `--start='5 minutes ago'` |
| Hours | `--start=2h`, `--start='1 hour'`, `--start='5 hours ago'` |
| Days | `--start=2d`, `--start='1 day'`, `--start='5 days ago'` |
| Weeks | `--start=2w`, `--start='1 week'`, `--start='5 weeks ago'` |
| Absolute date | `--start='23/1/2015 12:00'`, `--start='1/1/2015'` |
| ISO 8601 / RFC 3339 | `--start='2016-08-31T02:23:25.000Z'` |
| With offset | `--start='2016-08-31 10:23:25 UTC-8'` |

`--end` accepts the same forms. Defaults: `get` → `--start='5m'`; `streams` → `--start='1h'`.

---

## Filter patterns

`--filter-pattern` (`-f`) accepts the [CloudWatch Logs filter pattern syntax](http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/FilterAndPatternSyntax.html). Apply it server-side to avoid pulling the whole stream:

```bash
awslogs get my_lambda_group --filter-pattern='[r=REPORT,...]'
```

## JSON logs (`--query` / `-q`)

Like the `aws-cli` `--query` flag — apply a [JMESPath](https://jmespath.org) expression to each event whose message is a JSON object:

```bash
awslogs get my_lambda_group --query=message
```

Non-JSON lines are passed through unchanged.

---

## Watching

```bash
awslogs get /var/log/syslog ALL --watch
awslogs get /var/log/syslog ALL --watch --watch-interval=5    # poll every 5s
```

`-w` is the short form of `--watch`. The loop polls every `--watch-interval`
seconds (default 1). Press **Ctrl-C** at any time to stop watching — the tool
prints `Closing...` and exits immediately with status 0, even mid-poll or while
streaming output through a pipe.

---

## Kinesis data streams

`awslogs kinesis` searches [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/). Unlike CloudWatch Logs, Kinesis has **no server-side filtering** — there is no `filter_log_events` equivalent. So `search` enumerates the stream's shards (`ListShards`), reads records within the requested time window (`GetShardIterator` + `GetRecords`), and matches each record's payload **locally**. Shards are read sequentially, which keeps the tool inside the per-shard `GetRecords` rate limit.

```bash
# List the shards of a stream
awslogs kinesis shards my-stream

# Read every record on every shard (from the oldest retained record)
awslogs kinesis search my-stream

# Substring search (the default) across all shards
awslogs kinesis search my-stream -f 'order-id'

# Regular-expression search
awslogs kinesis search my-stream --regex -f 'user_id":\s*42'

# Restrict the time window (records arriving after --end stop the read)
awslogs kinesis search my-stream -s '1h ago' -e '10m ago'

# Tail new records in real time
awslogs kinesis search my-stream --watch

# Limit to specific shards (repeatable)
awslogs kinesis search my-stream --shard-id shardId-000000000000 --shard-id shardId-000000000001

# Reshape JSON records with JMESPath, and drop the shard column
awslogs kinesis search my-stream -S -q 'detail.message'
```

Record payloads are decoded as UTF-8 (lossily — invalid bytes become `�`).

Options:

| Flag | Effect |
|---|---|
| `-f`, `--filter-pattern` | Pattern to match against each record. Substring match by default. |
| `--regex` | Treat `--filter-pattern` as a regular expression instead of a literal substring. |
| `--shard-id` | Only read the given shard(s). Repeatable. Defaults to every shard. |
| `-s`, `--start` | Window start. Omitted ⇒ oldest retained record (`TRIM_HORIZON`). Accepts the same forms as [Time options](#time-options---start----end). |
| `-e`, `--end` | Window end. Records arriving after this stop the read. |
| `-w`, `--watch` | Keep polling for new records (see [Watching](#watching)). |
| `-i`, `--watch-interval` | Seconds between polls in `--watch` mode (default 1). |
| `-S`, `--no-shard` | Don't print the shard-id column. |
| `--timestamp` | Prepend the record's approximate arrival timestamp. |
| `-q`, `--query` | Apply a [JMESPath](https://jmespath.org) expression to JSON record payloads. |
| `--color WHEN` | `auto` (default), `always`, `never`. |

> **Note:** `--filter-pattern` here is a plain substring/regex match, *not* the CloudWatch Logs filter-pattern syntax used by `awslogs get`, because Kinesis records are matched client-side.

---

## Output control

| Flag | Effect |
|---|---|
| `-G`, `--no-group` | Don't print the group column |
| `-S`, `--no-stream` | Don't print the stream column |
| `--timestamp` | Prepend the event's creation timestamp |
| `--ingestion-time` | Prepend the event's ingestion time |
| `--color WHEN` | `auto` (default), `always`, `never` |

---

## Third-party endpoints (LocalStack etc.)

```bash
awslogs --aws-endpoint-url=http://localhost:4566 groups
# or set AWS_ENDPOINT_URL in the env
```

---

## AWS credentials

`awslogs` uses the standard AWS Rust SDK credential chain:

1. Explicit flags: `--aws-access-key-id`, `--aws-secret-access-key`, `--aws-session-token`
2. `--profile NAME` (or `AWS_PROFILE` env var) — your `~/.aws/credentials` / `~/.aws/config`
3. Standard env vars (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`)
4. EC2 / ECS / EKS instance roles

The recommended setup is to configure the AWS CLI once and let `awslogs` reuse those credentials.

### Required IAM permissions

For the log commands, the managed policy `CloudWatchLogsReadOnlyAccess` is sufficient. Equivalent inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "logs:Describe*",
      "logs:Get*",
      "logs:List*",
      "logs:StartQuery",
      "logs:StopQuery",
      "logs:TestMetricFilter",
      "logs:FilterLogEvents"
    ],
    "Resource": "*"
  }]
}
```

The `kinesis` commands additionally need read access to the data stream:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kinesis:ListShards",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords"
    ],
    "Resource": "*"
  }]
}
```

---

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Unknown error |
| 3 | Unrecognized date in `--start` / `--end` |
| 4 | `AccessDeniedException` / `ExpiredTokenException` |
| 6 | More than 100 streams match the supplied pattern (AWS API limit) |
| 7 | No streams match the supplied pattern in the time window |

These match the Python implementation.

---

## Development

```bash
cargo build                          # debug build
cargo test                           # 4 unit + 22 integration tests
cargo clippy --all-targets -- -D warnings
cargo fmt --all
```

The CloudWatch SDK is abstracted behind the `LogsClient` trait in `src/client.rs`, which is the seam the integration tests mock against (no network required).

---

## Credit & license

This project is a Rust port of [`jorgebastida/awslogs`](https://github.com/jorgebastida/awslogs) by Jorge Bastida and contributors. The original Python implementation is the source of truth for behavior and ships under the BSD license; see the upstream repo for full attribution.
