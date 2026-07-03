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

## Schema mapping

| Rerun column kind      | DuckDB column                                    |
|------------------------|--------------------------------------------------|
| Row ID                 | omitted by default                               |
| Index (timeline)       | `BIGINT` / `TIMESTAMP` depending on timeline type|
| Component column       | named `entity_path:Component`, nested LIST/STRUCT|

Arrow → DuckDB type conversion is recursive; unsupported exotic types fall
back to VARCHAR (display form) rather than failing the whole query.
