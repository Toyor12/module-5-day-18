"""Tests for Day 18 Lab — Three-Way Format Comparison (standalone).

The assertions here are deliberately format-specific. They check Delta-on-disk
artefacts (transaction log, commit versions) rather than seed-trivia such as
exact row counts, because the point of the lab is to evaluate the *format*,
not to read the test fixtures. A generic "row_count == 200" check would still
pass against a CSV writer; the assertions here would not.
"""

import json
import os
import shutil
import time

import pytest
from pyspark.sql import Row

from standalone.pipeline import (
    benchmark_workload,
    comparison_report,
    delta_delete,
    delta_history,
    delta_read,
    delta_upsert,
    delta_write,
    evaluate_format_tradeoffs,
    format_recommendation,
    get_spark_session,
    medtrack_recommendation,
    recommend_format,
    run_pipeline,
)


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("test-format-comparison")
    yield session
    session.stop()


@pytest.fixture
def tmp_delta_path(tmp_path):
    path = str(tmp_path / "delta_table")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


# ── Delta write tests ──


def test_delta_write_creates_v0_transaction_log(spark, tmp_delta_path):
    """A first delta_write emits the Delta v0 commit JSON in _delta_log/.

    Format-specific: a Parquet-only writer would not produce _delta_log/ at all,
    and the v0 file is the canonical signature of a fresh Delta table. This is
    what makes the table a *Delta* table rather than a directory of parquet.
    """
    delta_write(spark, tmp_delta_path)
    log_dir = os.path.join(tmp_delta_path, "_delta_log")
    assert os.path.isdir(log_dir), "Delta write must create _delta_log/"
    v0 = os.path.join(log_dir, "00000000000000000000.json")
    assert os.path.isfile(v0), "First commit must be 00000000000000000000.json"
    # Each commit JSON is line-delimited JSON; the first line is the protocol/
    # commitInfo entry. This will fail if the writer somehow wrote an empty file.
    with open(v0) as f:
        first = f.readline()
    assert first.strip(), "v0 commit must contain at least one JSON action"
    json.loads(first)  # raises if not valid JSON


def test_delta_write_returns_format_key(spark, tmp_delta_path):
    """Write result includes format identifier."""
    result = delta_write(spark, tmp_delta_path)
    assert result["format"] == "delta"


def test_delta_write_records_timing(spark, tmp_delta_path):
    """Write timing is a positive float."""
    result = delta_write(spark, tmp_delta_path)
    assert isinstance(result["write_time"], float)
    assert result["write_time"] > 0


def test_delta_write_round_trip_through_format_reader(spark, tmp_delta_path):
    """The path is readable specifically via the 'delta' format reader.

    Format-specific: spark.read.format('delta') will refuse to read a plain
    parquet directory. This assertion would fail if the implementation
    silently fell back to writing parquet without a transaction log.
    """
    delta_write(spark, tmp_delta_path)
    df = spark.read.format("delta").load(tmp_delta_path)
    # A successful Delta read returns a non-empty DataFrame with the
    # configured columns. We do NOT pin the row count here -- that would
    # be a seed-trivia check on the lab's chosen N.
    assert df.count() > 0
    assert "order_id" in df.columns


def test_delta_write_is_idempotent_via_overwrite(spark, tmp_delta_path):
    """Writing twice to the same path overwrites cleanly (no row growth).

    The point isn't the literal count -- it's that mode('overwrite') replaced
    the prior version rather than appending to it.
    """
    delta_write(spark, tmp_delta_path)
    first_count = spark.read.format("delta").load(tmp_delta_path).count()
    delta_write(spark, tmp_delta_path)
    second_count = spark.read.format("delta").load(tmp_delta_path).count()
    assert first_count == second_count, "overwrite must not duplicate rows"


# ── Delta read tests ──


def test_delta_read_matches_written_count(spark, tmp_delta_path):
    """Read returns the same number of rows that were just written.

    Setup sanity: pinning a literal seed value (e.g. 200) would test the
    fixture more than the format. We only check round-trip equality.
    """
    write_result = delta_write(spark, tmp_delta_path)
    result = delta_read(spark, tmp_delta_path)
    assert result["row_count"] == write_result["row_count"]


def test_delta_read_records_timing(spark, tmp_delta_path):
    """Read timing is a positive float."""
    delta_write(spark, tmp_delta_path)
    result = delta_read(spark, tmp_delta_path)
    assert isinstance(result["read_time"], float)
    assert result["read_time"] > 0


def test_delta_read_format_key(spark, tmp_delta_path):
    """Read result includes format identifier."""
    delta_write(spark, tmp_delta_path)
    result = delta_read(spark, tmp_delta_path)
    assert result["format"] == "delta"


# ── Delta upsert tests ──


def test_delta_upsert_grows_table(spark, tmp_delta_path):
    """MERGE inserts unmatched rows -- final count strictly greater than initial.

    Format-specific via behaviour: a non-MERGE writer (e.g. naive overwrite)
    would either replace the table (count drops) or append duplicates. A
    correct MERGE strictly grows the table by the number of unmatched source
    rows, no more, no less.
    """
    delta_write(spark, tmp_delta_path)
    initial = spark.read.format("delta").load(tmp_delta_path).count()
    result = delta_upsert(spark, tmp_delta_path)
    assert result["rows_after_upsert"] > initial, "MERGE must insert new rows"


def test_delta_upsert_creates_new_transaction_log_version(spark, tmp_delta_path):
    """MERGE produces a new commit JSON in _delta_log/ -- the Delta version
    monotonically increases.

    Format-specific: this is the contract of a Delta MERGE. A plain parquet
    overwrite or an in-place file mutation would not produce 00...001.json.
    The previous version's file count does not matter; what matters is that
    the log advanced.
    """
    delta_write(spark, tmp_delta_path)
    log_dir = os.path.join(tmp_delta_path, "_delta_log")
    versions_before = sorted(
        f for f in os.listdir(log_dir) if f.endswith(".json")
    )
    assert versions_before == ["00000000000000000000.json"], (
        f"After a fresh write, only v0 should exist; got {versions_before}"
    )

    delta_upsert(spark, tmp_delta_path)

    versions_after = sorted(
        f for f in os.listdir(log_dir) if f.endswith(".json")
    )
    # The MERGE must produce at least one new commit beyond v0.
    assert "00000000000000000001.json" in versions_after, (
        f"MERGE must create v1 commit JSON; log now contains {versions_after}"
    )
    assert len(versions_after) > len(versions_before), (
        "Delta version must increase after MERGE"
    )


def test_delta_upsert_updates_existing_rows(spark, tmp_delta_path):
    """MERGE updates matched rows to cancelled status."""
    delta_write(spark, tmp_delta_path)
    delta_upsert(spark, tmp_delta_path)
    df = spark.read.format("delta").load(tmp_delta_path)
    # The implementation chooses the update window. We don't pin the exact
    # cancelled count -- we just require that whenMatchedUpdateAll() did
    # produce some 'cancelled' rows AND that whenNotMatchedInsertAll()
    # produced some too. (If matched=0, the upsert was insert-only; if
    # unmatched=0, it was update-only.)
    cancelled = df.filter(df.status == "cancelled").count()
    non_cancelled = df.filter(df.status != "cancelled").count()
    assert cancelled > 0, "MERGE must produce some updated rows"
    assert non_cancelled > 0, "MERGE must preserve unmatched rows"


# ── Comparison report tests ──


def test_comparison_report_has_three_formats():
    """Report covers delta, hudi, and iceberg."""
    report = comparison_report()
    assert "delta" in report
    assert "hudi" in report
    assert "iceberg" in report


def test_comparison_report_has_strengths():
    """Each format entry includes a strengths list."""
    report = comparison_report()
    for fmt in ("delta", "hudi", "iceberg"):
        assert "strengths" in report[fmt]
        assert isinstance(report[fmt]["strengths"], list)
        assert len(report[fmt]["strengths"]) > 0


def test_comparison_report_has_best_for():
    """Each format entry includes a best_for description."""
    report = comparison_report()
    for fmt in ("delta", "hudi", "iceberg"):
        assert "best_for" in report[fmt]
        assert isinstance(report[fmt]["best_for"], str)


def test_comparison_report_has_weaknesses():
    """Each format entry includes weaknesses for balanced evaluation."""
    report = comparison_report()
    for fmt in ("delta", "hudi", "iceberg"):
        assert "weaknesses" in report[fmt]
        assert isinstance(report[fmt]["weaknesses"], list)


def test_comparison_report_all_support_acid():
    """All three formats support ACID transactions."""
    report = comparison_report()
    for fmt in ("delta", "hudi", "iceberg"):
        assert report[fmt]["acid"] is True


def test_comparison_report_has_scores():
    """Each format entry includes numeric scores (1-10) for evaluation."""
    report = comparison_report()
    for fmt in ("delta", "hudi", "iceberg"):
        scores = report[fmt]["scores"]
        for key in ("write_performance", "read_performance",
                    "schema_evolution", "multi_engine", "streaming"):
            assert key in scores, f"{fmt} missing score key {key}"
        assert all(isinstance(v, int) and 1 <= v <= 10 for v in scores.values())


# ── evaluate_format_tradeoffs tests ──


def test_evaluate_format_tradeoffs_valid():
    """Returns a dict with expected keys for a valid format."""
    result = evaluate_format_tradeoffs("delta")
    assert "strengths" in result
    assert "weaknesses" in result
    assert "best_for" in result


def test_evaluate_format_tradeoffs_invalid():
    """Raises ValueError for unknown format."""
    with pytest.raises(ValueError, match="Unknown format"):
        evaluate_format_tradeoffs("parquet")


# ── Recommendation tests ──


def test_recommend_batch_dominant():
    """Batch-dominant, single-engine workload recommends Delta."""
    assert recommend_format("batch", "single", "rare") == "delta"


def test_recommend_streaming_upserts():
    """Streaming with continuous upserts recommends Hudi."""
    assert recommend_format("streaming", "single", "continuous") == "hudi"


def test_recommend_multi_engine():
    """Multi-engine requirement recommends Iceberg regardless of pattern."""
    assert recommend_format("batch", "multi", "rare") == "iceberg"
    assert recommend_format("streaming", "multi", "continuous") == "iceberg"
    assert recommend_format("hybrid", "multi", "frequent") == "iceberg"


def test_recommend_hybrid_single_frequent():
    """Hybrid single-engine with frequent upserts recommends Delta."""
    assert recommend_format("hybrid", "single", "frequent") == "delta"


def test_recommend_batch_single_continuous():
    """Batch with continuous upserts recommends Hudi."""
    assert recommend_format("batch", "single", "continuous") == "hudi"


# ── format_recommendation tests ──


def test_format_recommendation_has_all_scenarios():
    """Recommendation function covers all four scenarios."""
    recs = format_recommendation()
    assert "batch_single_engine" in recs
    assert "streaming_upserts" in recs
    assert "multi_engine_batch" in recs
    assert "hybrid_multi_engine" in recs


def test_format_recommendation_values():
    """Each scenario maps to the correct format."""
    recs = format_recommendation()
    assert recs["batch_single_engine"] == "delta"
    assert recs["streaming_upserts"] == "hudi"
    assert recs["multi_engine_batch"] == "iceberg"
    assert recs["hybrid_multi_engine"] == "iceberg"


# ── Synthesis task: benchmark_workload ──
#
# These tests cover the labelled synthesis task. They are intentionally
# tougher than the rest of the file -- the student must combine three
# primitives (write timing, upsert timing, recommendation rubric) and
# justify the decision with measured numbers, not just a rule-table dump.


def test_benchmark_workload_returns_measured_timings(spark, tmp_delta_path):
    """benchmark_workload must report wall-clock measurements, not constants."""
    profile = {
        "kind": "write_heavy",
        "primary_pattern": "batch",
        "engine_diversity": "single",
        "upsert_frequency": "frequent",
    }
    result = benchmark_workload(spark, profile, tmp_delta_path)
    for key in ("write_seconds", "read_seconds", "upsert_seconds"):
        assert key in result, f"missing {key} in benchmark output"
        assert isinstance(result[key], float), f"{key} must be a float"
        assert result[key] > 0.0, f"{key} must be a measured positive duration"


def test_benchmark_workload_routes_to_iceberg_for_multi_engine(spark, tmp_delta_path):
    """Multi-engine profiles must route to Iceberg regardless of measured times.

    This is the rule-vs-measurement check: Iceberg's edge is engine diversity,
    not raw throughput. If the student uses measured times to override the
    multi-engine rule, this fails.
    """
    profile = {
        "kind": "mixed",
        "primary_pattern": "batch",
        "engine_diversity": "multi",
        "upsert_frequency": "rare",
    }
    result = benchmark_workload(spark, profile, tmp_delta_path)
    assert result["recommended_format"] == "iceberg"


def test_benchmark_workload_routes_to_hudi_for_continuous_upserts(spark, tmp_delta_path):
    """Continuous-upsert single-engine profiles must route to Hudi."""
    profile = {
        "kind": "write_heavy",
        "primary_pattern": "streaming",
        "engine_diversity": "single",
        "upsert_frequency": "continuous",
    }
    result = benchmark_workload(spark, profile, tmp_delta_path)
    assert result["recommended_format"] == "hudi"


def test_benchmark_workload_justification_cites_measured_numbers(spark, tmp_delta_path):
    """Justification string must reference at least one of the measured
    numbers. A generic strengths-list dump does not satisfy the contract.

    This is the synthesis check: the student must wire the *measurements*
    into the prose, not just hand back the rule table.
    """
    profile = {
        "kind": "write_heavy",
        "primary_pattern": "batch",
        "engine_diversity": "single",
        "upsert_frequency": "frequent",
    }
    result = benchmark_workload(spark, profile, tmp_delta_path)
    justification = result["justification"]
    assert isinstance(justification, str)
    assert len(justification) > 20

    # The justification must include a numeric token that matches one of
    # the measured durations to at least 2 decimal places. We accept any
    # of the three measured values rounded to 2dp.
    numeric_tokens_required = []
    for key in ("write_seconds", "read_seconds", "upsert_seconds"):
        numeric_tokens_required.append(f"{result[key]:.2f}")
    found = any(tok in justification for tok in numeric_tokens_required)
    assert found, (
        "justification must cite at least one measured number "
        f"(expected one of {numeric_tokens_required}) -- got: {justification!r}"
    )


def test_benchmark_workload_echoes_workload_profile(spark, tmp_delta_path):
    """The output echoes the input profile for downstream auditability."""
    profile = {
        "kind": "read_heavy",
        "primary_pattern": "batch",
        "engine_diversity": "single",
        "upsert_frequency": "rare",
    }
    result = benchmark_workload(spark, profile, tmp_delta_path)
    assert result["workload"] == profile


# ── delta_delete and delta_history tests ──


def test_delta_delete_reduces_row_count(spark, tmp_delta_path):
    """DELETE removes matching rows: remaining < initial."""
    delta_write(spark, tmp_delta_path)
    initial = spark.read.format("delta").load(tmp_delta_path).count()
    result = delta_delete(spark, tmp_delta_path, "status = 'pending'")
    assert result["remaining_rows"] < initial
    surviving_pending = (
        spark.read.format("delta").load(tmp_delta_path)
        .filter("status = 'pending'").count()
    )
    assert surviving_pending == 0


def test_delta_delete_creates_new_log_version(spark, tmp_delta_path):
    """A DELETE produces a new commit JSON beyond the initial write."""
    delta_write(spark, tmp_delta_path)
    log_dir = os.path.join(tmp_delta_path, "_delta_log")
    versions_before = sorted(f for f in os.listdir(log_dir) if f.endswith(".json"))
    delta_delete(spark, tmp_delta_path, "status = 'pending'")
    versions_after = sorted(f for f in os.listdir(log_dir) if f.endswith(".json"))
    assert len(versions_after) > len(versions_before)


def test_delta_history_grows_with_each_op(spark, tmp_delta_path):
    """History reflects write + delete as separate commits."""
    delta_write(spark, tmp_delta_path)
    history_after_write = delta_history(spark, tmp_delta_path)
    delta_delete(spark, tmp_delta_path, "status = 'pending'")
    history_after_delete = delta_history(spark, tmp_delta_path)
    assert len(history_after_delete) > len(history_after_write)
    assert isinstance(history_after_delete, list)
    assert isinstance(history_after_delete[0], dict)


# ── MedTrack scenario tests ──


def test_medtrack_recommendation_routes_to_iceberg():
    """MedTrack's Trino requirement makes it multi-engine -> Iceberg."""
    result = medtrack_recommendation()
    assert result["recommended_format"] == "iceberg"


def test_medtrack_recommendation_surfaces_delta_weaknesses():
    """Output cites Delta weaknesses from comparison_report (non-empty)."""
    result = medtrack_recommendation()
    assert isinstance(result["delta_weaknesses"], list)
    assert len(result["delta_weaknesses"]) > 0
    report = comparison_report()
    for w in result["delta_weaknesses"]:
        assert w in report["delta"]["weaknesses"]
