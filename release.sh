#!/bin/sh
set -e

git fetch origin main
test -z "$(git status --porcelain)" || { echo "Working tree dirty; commit or stash first."; exit 1; }

cargo fmt --check
cargo clippy --all-targets --all-features
cargo deny check
cargo vet --locked
cargo build --release --bin phdupes
cargo +nightly udeps --all-targets
cargo test --release
./target/release/phdupes --version
cargo release patch --no-publish --no-push --no-tag --execute

# Eventually:
# RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
# cargo clippy --all-targets --all-features -- -D warnings

# vetting if feeling lazy:
# cargo vet regenerate exemptions
