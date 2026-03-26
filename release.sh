#!/bin/sh
cargo build --release --bin phdupes && ./target/release/phdupes --version && cargo release patch --no-publish --execute
