set shell := ["nu", "-c"]

default:
    @just --list -u

build:
    cargo build --release
    cargo run --bin data_to_csv --release