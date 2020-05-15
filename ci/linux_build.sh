#!/bin/bash
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zprofile -Ccodegen-units=1 -Cinline-threshold=0 -Clink-dead-code -Coverflow-checks=off"
cargo clean
cargo build --verbose $CARGO_OPTIONS
cargo test --verbose $CARGO_OPTIONS
zip -0 ccov.zip `find . \( -name "terminus*.gc*" \) -print`;
./grcov ccov.zip -s . -t lcov --llvm --branch --ignore-not-existing --ignore "/*" -o lcov.info;
bash <(curl -s https://codecov.io/bash) -f lcov.info;
