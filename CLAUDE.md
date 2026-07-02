# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

A working Rust port of [`jorgebastida/awslogs`](https://github.com/jorgebastida/awslogs): a CLI for querying CloudWatch Logs, extended with a `kinesis` command for searching Kinesis data streams and a `tui` command (Ratatui) for browsing both interactively. Rust edition **2024**, built on the AWS SDK for Rust and `tokio`. The CLI surface, exit codes, color output, dedup behavior, and time expressions are kept compatible with the Python original; the integration suite is ported from upstream.

## Commands

```bash
cargo build              # debug build → target/debug/awslogs
cargo build --release    # optimized build → target/release/awslogs
cargo run -- <args>      # run the binary (args after `--` go to the program)
cargo check              # fast type-check without producing a binary
cargo test               # run all tests
cargo test <name>        # run a single test by name substring
cargo test -- --nocapture   # show println! output from tests
cargo fmt                # format (rustfmt)
cargo clippy -- -D warnings  # lint, treating warnings as errors
```

Edition 2024 requires a reasonably current toolchain (Rust 1.85+). If `cargo build` complains about the edition, run `rustup update stable`.

## Architecture

Single binary + library crate (`src/main.rs` is a thin shell over `src/lib.rs`). Async on `tokio` (multi-thread runtime); CLI parsing with `clap` derive.

Module layout:

- **`cli.rs`** — `clap` command definitions and the `run`/`execute` dispatch. `execute` is generic over `Write` sinks and takes boxed async client *factories* (`ClientFactory`/`KinesisClientFactory`), which is the seam integration tests use to inject mock clients. Commands: `get`, `groups`, `streams`, `kinesis {search,shards}`, `tui`.
- **`client.rs`** — the trait seams mocked in tests: `LogsClient`, `KinesisClient`, and `IdentityClient` (STS `GetCallerIdentity`), each with a real `Aws*Client` impl. `load_shared_config` centralizes credential/region/endpoint resolution for all three services.
- **`core.rs`** — `AwsLogs` engine (`get`/`groups`/`streams`). Returns data (`get_groups`/`get_streams`) *and* streams formatted lines to a `Write` (`list_logs_into`). Owns ANSI coloring, dedup, and event formatting.
- **`kinesis.rs`** — `KinesisSearch` engine (`search`/`shards`), same `*_into(writer)` streaming pattern.
- **`time.rs`** — `--start`/`--end` expression parsing (relative like `5m`/`2h ago`, absolute, etc.).
- **`exceptions.rs`** — `AwsLogsError` with Python-compatible exit `code()`s.
- **`tui.rs`** — Ratatui interactive UI (`awslogs tui`). Reuses the `core`/`kinesis` engines by capturing their `Write` output through a `ChannelWriter`, so every view maps to a real CLI command. Because the engines hold a non-`Send` compiled JMESPath `Expression`, **all AWS work runs on a dedicated OS thread with its own current-thread runtime + `LocalSet`**; the UI loop (on the main runtime) talks to it over `EngineCmd`/`Msg` channels. Terminal input is read on a third blocking thread. The status bar shows the account ID from `IdentityClient`.

Key cross-cutting decision: engines expose both a data-returning API and a `*_into(writer)` streaming API. New output surfaces (like the TUI) should reuse the streaming API via a custom `Write` rather than re-implementing formatting.
