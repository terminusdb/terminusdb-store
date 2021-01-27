#!/bin/bash
export CARGO_INCREMENTAL=0
export RUSTFLAGS="-Zinstrument-coverage -Zprofile -Ccodegen-units=1 -Copt-level=0 -Clink-dead-code -Coverflow-checks=off -Zpanic_abort_tests -Cpanic=abort"
curl -L https://github.com/mozilla/grcov/releases/latest/download/grcov-linux-x86_64.tar.bz2 | tar jxf -
cargo build --verbose $CARGO_OPTIONS
cargo test --verbose $CARGO_OPTIONS
zip -0 ccov.zip `find . \( -name "terminus*.gc*" \) -print`;
./grcov ccov.zip -s . -t lcov --llvm --branch --ignore-not-existing --ignore "/*" -o lcov.info;
bash <(curl -s https://codecov.io/bash) -f lcov.info;
