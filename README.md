# Day 18 Lab - Three-Way Format Comparison: Delta vs Hudi vs Iceberg

> :de: [Deutsche Version](README_DE.md)

## Objective

Evaluate open table formats (Delta, Hudi, Iceberg) by exercising Delta primitives, comparing their tradeoffs, and *justifying* a format recommendation against a measured workload. Bloom's level: **Analyze -> Create**.

Target completion time: **under 1 hour**. The lecture-demonstrated primitives are prefilled so you spend lab time on synthesis and evaluation, not on retyping the lecture.

## Learning Objectives

By the end of this lab you will be able to:

1. **Justify** a structured `comparison_report` and **explain** which input dominates a `recommend_format` decision.
2. **Implement** `delta_delete` and `delta_history` against a Delta table and observe transaction-log growth across write -> upsert -> delete commits.
3. **Critique** the recommendation rule order -- explain why the multi-engine check fires before the streaming/upsert check, and what would break if you swapped them.
4. **Synthesize** a measurement-grounded recommendation in `benchmark_workload` that cites the measured numbers, not just the rule table.
5. **Evaluate** the format choice for a named real-world case (MedTrack) by routing its workload profile through the rubric and surfacing the migration trade-off explicitly.

## What is provided

Prefilled in `pipeline.py` (lecture demonstrates these in full -- no point re-typing):

- `get_spark_session` -- Delta-enabled SparkSession factory
- `delta_write`, `delta_read`, `delta_upsert` -- the three Delta primitives with timing
- `recommend_format` -- the workload-to-format rubric
- `comparison_report` -- the static three-way comparison dict (defend the scores in your write-up)

You implement (these are the lab's Analyze / Evaluate / Create surface):

- `evaluate_format_tradeoffs`
- `delta_delete`, `delta_history`
- `format_recommendation`
- `benchmark_workload` -- **labelled synthesis task**
- `medtrack_recommendation`
- `run_pipeline`

Plus 36 TDD tests in `tests/test_format_comparison.py` covering Delta-on-disk semantics (transaction log v0/v1/v2, format-reader round-trip, history growth), the recommendation rubric, the comparison report (including numeric scores), the synthesis task, and the MedTrack scenario.

## Setup

```bash
cd standalone && uv sync --dev
uv run pytest tests/ -v
```

PySpark requires Java 17+. Set `JAVA_HOME` if it is not already on PATH.

---

## Tasks

### :green_circle: Basic

#### Task 1 -- Implement `evaluate_format_tradeoffs`

Wrap the prefilled `comparison_report()` so callers can pull a single
format's `{strengths, weaknesses, best_for}` triple by name.

- Return a dict with those three keys for `"delta"`, `"hudi"`, or `"iceberg"`.
- Raise `ValueError("Unknown format: ...")` for any other input (e.g. `"parquet"`).

This is the lab's bridge from *describing* the formats (lecture) to
*judging* them (lab): every weakness you list and every score you assign in
the comparison report is an evaluation claim you must defend in the
discussion.

#### Task 2 -- Implement `delta_delete` and `delta_history`

- `delta_delete` calls `DeltaTable.forPath(spark, path).delete(condition)`
  and returns `{format, remaining_rows, condition}`.
- `delta_history` calls `DeltaTable.forPath(spark, path).history().collect()`
  and casts each row via `row.asDict()`.

Acceptance: after write -> upsert -> delete, `_delta_log/` contains v0, v1, and v2 commit JSONs, and `delta_history` returns a list of length >= 3 with the most recent operation first.

### :blue_circle: Intermediate

#### Task 3 -- Implement `format_recommendation`

Apply the prefilled `recommend_format` rubric to four named scenarios and return a dict:

- `batch_single_engine` -> Delta
- `streaming_upserts` -> Hudi
- `multi_engine_batch` -> Iceberg
- `hybrid_multi_engine` -> Iceberg

#### Task 4 -- Critique the rule order (Analyze, ungraded)

Answer in your notes (no test asserts this, but the discussion will):

- Why does the `multi-engine` check fire **before** the streaming / upsert check?
- Construct a workload that would receive the *wrong* recommendation if you swapped the rule order. Which input did the swap let dominate?

### :purple_circle: Advanced

#### :red_circle: Task 5 -- Synthesis: measurement-grounded recommendation

> **This is the labelled synthesis task.** The lecture demonstrates each primitive in isolation; here you must combine three of them and let the *measurements* drive the prose.

Implement `benchmark_workload(spark, workload_profile, path)`:

- Run `delta_write`, `delta_read`, and `delta_upsert` against `path` and capture wall-clock timings for each.
- Apply `recommend_format` to the profile's `primary_pattern` / `engine_diversity` / `upsert_frequency` fields.
- Return a dict with `workload`, `write_seconds`, `read_seconds`, `upsert_seconds`, `recommended_format`, and a `justification` string.
- The `justification` **must cite at least one measured number** to two decimal places (e.g. `"upsert took 4.21s"`). A justification that only quotes `comparison_report()` strengths fails the synthesis test.
- Multi-engine profiles still route to Iceberg regardless of measured times -- engine diversity dominates throughput.

This recombines write benchmarking + read benchmarking + upsert benchmarking + the recommendation rubric into one decision the lecture does not directly demonstrate. The grader (`test_benchmark_workload_justification_cites_measured_numbers`) looks for a numeric token from your measurements inside the justification string.

#### Task 6 -- MedTrack scenario (Evaluate)

Apply the rubric to a named real-world case. Build `medtrack_recommendation()`
that constructs the MedTrack workload profile (multi-engine due to the new
Trino licences), routes it through `recommend_format`, and surfaces Delta's
weaknesses from `comparison_report` so the migration trade-off is visible.

Why this is Evaluate: you are not implementing a new primitive, you are
*applying* the rubric and *judging* the format choice for a specific
scenario. The grader checks the recommendation; the discussion grades
your `notes`.

#### Task 7 -- Wire `run_pipeline`

Plumb the prefilled and student-implemented pieces together: build a
SparkSession, run write -> read -> upsert -> delete -> history, log the
comparison report, and route one sample workload through
`benchmark_workload`. No graded behaviour beyond "doesn't raise".

---

## Running Tests

```bash
uv run pytest tests/ -v
```

All 36 tests should pass when your implementation is complete. Half of the test surface is format-specific (transaction log artefacts, MERGE/DELETE version bumps, history growth, format-reader round-trip); the rest exercises the recommendation rubric, the comparison report (including numeric scores), the synthesis task, and the MedTrack scenario.
