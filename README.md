# awslogs (Rust)

A Rust port of [`jorgebastida/awslogs`](https://github.com/jorgebastida/awslogs) — a small command-line tool for querying groups, streams, and events from [Amazon CloudWatch Logs](https://aws.amazon.com/cloudwatch/).

This is a single-binary reimplementation built on the [AWS SDK for Rust](https://aws.amazon.com/sdk-for-rust/) (`aws-sdk-cloudwatchlogs`). The CLI surface, exit codes, color output, dedup behavior, and time expressions are kept byte-for-byte compatible with the Python original wherever possible; the integration test suite is ported from upstream so this binary passes the same assertions.

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
awslogs [ get | groups | streams ]
```

* `awslogs groups` — list existing groups
* `awslogs streams GROUP` — list existing streams within `GROUP`
* `awslogs get GROUP [STREAM_EXPRESSION]` — get logs matching `STREAM_EXPRESSION` in `GROUP`.
  * Expressions are regular expressions (anchored at the start), or the literal `ALL` as a shortcut for `.*`.

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

The managed policy `CloudWatchLogsReadOnlyAccess` is sufficient. Equivalent inline policy:

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
