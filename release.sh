#!/bin/sh
cargo build --release --bin phdupes && cargo release patch --no-publish --execute
