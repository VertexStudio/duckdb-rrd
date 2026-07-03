# Design notes: `rrd` DuckDB extension

Goal: query Rerun `.rrd` recording files directly from DuckDB SQL, including files
that are still being appended to by a live recording.

```sql
SELECT * FROM read_rrd('recording.rrd');
```

## Principles

- The extension **reads `.rrd` files**; it does not embed the Rerun SDK or viewer.
  Only Rerun's low-level format crates are used for decoding
  (`re_log_encoding`, `re_chunk_store`, `re_dataframe`).
- Full SQL integration: `WHERE`, `JOIN`, `GROUP BY`, other DuckDB extensions.
- Entity/component model surfaces naturally: one column per component,
  one row per time point on the chosen timeline.
- Arrow end-to-end: Rerun stores component data as Arrow; we convert record
  batches into DuckDB vectors, preserving nested types (LIST/STRUCT) where
  possible.

## Architecture

```
Writer process               ┌──────────────────────┐
(SDK .save / viewer) ──────► │ recording.rrd        │  (append-only chunks)
                             └──────────┬───────────┘
                                        │
                    ┌───────────────────▼─────────────────────┐
                    │ DuckDB + rrd extension (Rust)            │
                    │  • read_rrd(...) table function          │
                    │  • rrd_entities/rrd_schema helpers       │
                    │  • re_chunk_store + re_dataframe reader  │
                    └───────────────────┬─────────────────────┘
                                        │ Arrow → DuckDB vectors
                                        ▼
                                 DuckDB SQL engine
```

## Table function

```sql
SELECT * FROM read_rrd(
    'recording.rrd',
    entity      => '/camera/**',     -- entity path filter expression
    timeline    => 'frame_nr',       -- index timeline (default: log_time / first found)
    fill_latest => true              -- latest-at semantics for sparse columns
);
```

- `bind` opens the store, resolves the schema of the query, registers result
  columns.
- `init`/`func` stream record batches from `re_dataframe::QueryHandle` into
  output chunks.

## Live read-while-write

`.rrd` files are append-only streams of encoded chunks. Each `read_rrd()` scan
re-opens and decodes the file, tolerating a truncated tail (a chunk that is
still being written). Polling therefore comes for free at the SQL layer:
re-running a query sees new data. No file locks are taken.

## Write support

`.rrd` is an append-only chunk stream, which maps naturally onto DuckDB's
`COPY ... TO` machinery (DuckDB 1.5's C API exposes
`duckdb_register_copy_function`):

```sql
-- Export query results as a new Rerun recording
COPY (SELECT frame_nr, loss, accuracy FROM training_metrics)
TO 'metrics.rrd'
(FORMAT rrd, ENTITY '/metrics', TIMELINE 'frame_nr',
 COLUMNS 'frame_nr,loss,accuracy');
```

Semantics and constraints:

- **Create / export** — each `COPY TO` produces a valid recording: a store
  header followed by data chunks. Values are written as generic Rerun
  component batches (one component per column) on the target `ENTITY`.
- **Index column** — the column named by `TIMELINE` becomes the index
  (sequence for integers, timestamp for TIMESTAMP columns). Remaining columns
  become components.
- **Column names** — DuckDB's copy-function C API surfaces column *types* but
  not *names* at bind time, so names are passed explicitly with the `COLUMNS`
  option; without it, columns are named `col_0..col_N` positionally.
- **Append** — `.rrd` streams are concatenable; appending to an existing
  recording file is a follow-up (requires reusing the original store id).
- **No in-place mutation** — deliberately unsupported; it breaks append-only
  semantics. The supported pattern is read → transform → write a new file.

`COPY ... FROM (FORMAT rrd)` can be wired to the same reader via
`duckdb_copy_function_set_copy_from_function`.

## Schema mapping

| Rerun column kind      | DuckDB column                                    |
|------------------------|--------------------------------------------------|
| Row ID                 | omitted by default                               |
| Index (timeline)       | `BIGINT` / `TIMESTAMP` depending on timeline type|
| Component column       | named `entity_path:Component`, nested LIST/STRUCT|

Arrow → DuckDB type conversion is recursive; unsupported exotic types fall
back to VARCHAR (display form) rather than failing the whole query.
