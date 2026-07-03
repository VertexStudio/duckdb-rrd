# Vendored dependencies

## `duckdb/` — duckdb-rs 1.10504.0

Verbatim copy of the `duckdb` crate published on crates.io, wired in through
`[patch.crates-io]` in the workspace `Cargo.toml`, with these changes:

- `Cargo.toml`: the `comfy-table` requirement is widened from `~7.1` to
  `>=7.1, <8`. duckdb-rs pins `~7.1` to preserve its own MSRV (comfy-table
  7.2 needs Rust 1.88) while rerun's crates require `>=7.2.2`; cargo cannot
  satisfy both even though they are semver-compatible. This project's
  toolchain is well past 1.88, so the pin is irrelevant here.
- `src/vtab/arrow.rs` + `src/core/vector.rs`: two upstream-worthy bug fixes
  in the arrow -> DataChunk list writer, both exposed by real Rerun
  recordings with large nested lists:
  1. nested `List`/`FixedSizeList` children were written without reserving
     the child vector, panicking (`index out of bounds`) past the standard
     vector size of 2048 (new `list_child_with_capacity` /
     `array_child_with_capacity`);
  2. the list child size was never published via
     `duckdb_list_vector_set_size`, so operations that walk the child
     (e.g. `UNNEST`) saw an empty child and returned NULLs.

To refresh: download the new crate from crates.io, re-apply the changes
above, and update the version in `Cargo.toml`'s `[dependencies]`.
