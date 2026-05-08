# Day 18 — Three-Way Format Comparison: Delta vs Hudi vs Iceberg

A PySpark pipeline that benchmarks Delta Lake primitives and applies a
workload-to-format rubric to recommend the right open table format for
a given use case.

## What it does

- Writes, reads, upserts, and deletes data using Delta Lake
- Compares Delta, Hudi, and Iceberg across five dimensions: write
  performance, read performance, schema evolution, multi-engine support,
  and streaming
- Recommends a format based on workload profile (engine diversity,
  upsert frequency, primary pattern)
- Includes a MedTrack case study showing why multi-engine requirements
  override throughput considerations

## Stack

- Python 3.11
- PySpark 3.5 + Delta Lake 3.0
- pytest (36 tests, all passing)
- Docker + PostgreSQL for integration tests

## Run tests

```bash
cd standalone
uv sync --dev
uv run pytest tests/ -v
```

## Key finding

`CACHE TABLE` on a Delta-backed view does NOT freeze the snapshot. A new
commit to the Delta path invalidates the cached plan via the transaction-log
version tag. To pin a snapshot use `versionAsOf` or `.collect()` into Python.
