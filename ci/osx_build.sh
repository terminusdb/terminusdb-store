#!/bin/bash
cargo clean
cargo build --verbose $CARGO_OPTIONS
cargo test --verbose $CARGO_OPTIONS
cargo fmt -- --check
