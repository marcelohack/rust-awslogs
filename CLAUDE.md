# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

Fresh `cargo init` scaffold — single binary crate named `awslogs`, Rust edition **2024**, no dependencies yet. The implementation has not started; `src/main.rs` is the default "Hello, world!". Project intent (per directory name `rust-awslogs`) is an AWS logs CLI, but no architecture has been committed to. When adding the first real code, update the **Architecture** section below.

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

*Not yet established.* Currently one file: `src/main.rs`. Document module layout, CLI parsing approach (e.g. `clap`), AWS SDK choices (`aws-sdk-cloudwatchlogs` vs. `rusoto`), and async runtime (`tokio`) here as they get introduced — those are the cross-cutting decisions future sessions will need to know without re-reading every file.
