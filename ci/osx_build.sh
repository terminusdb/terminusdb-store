#!/bin/bash
cargo clean
cargo build --verbose
cargo test --verbose
