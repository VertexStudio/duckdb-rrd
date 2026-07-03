# Vendored dependencies

## `duckdb/` — duckdb-rs 1.10504.0

Verbatim copy of the `duckdb` crate published on crates.io, wired in through
`[patch.crates-io]` in the workspace `Cargo.toml`, with exactly one change:

- `Cargo.toml`: the `comfy-table` requirement is widened from `~7.1` to
  `>=7.1, <8`.

duckdb-rs pins `comfy-table ~7.1` to preserve its own MSRV (comfy-table 7.2
needs Rust 1.88), while rerun's crates require `comfy-table >=7.2.2`. Both
cannot be satisfied at once by cargo's resolver even though the versions are
semver-compatible. This project's toolchain is well past 1.88, so the pin is
irrelevant here.

To refresh: download the new crate from crates.io, re-apply the one-line
change, and update the version in `Cargo.toml`'s `[dependencies]`.
