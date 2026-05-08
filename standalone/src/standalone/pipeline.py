"""Day 18 -- Three-Way Format Comparison: Delta vs Hudi vs Iceberg.

Plain Python pipeline (no orchestrator). Uses PySpark with Delta Lake to
demonstrate format operations, then compares strengths of all three formats.

The lecture-demonstrated primitives (``get_spark_session``, ``delta_write``,
``delta_read``, ``delta_upsert``, ``recommend_format``, ``comparison_report``)
are PREFILLED so you can spend lab time on synthesis, not on retyping the
lecture. Functions raising ``NotImplementedError`` are the ones you must
implement.
"""

import logging
import os
import shutil
import time

from pyspark.sql import Row, SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)

DELTA_PATH = "/tmp/shopflow_formats/delta_table"


# == Spark session (prefilled from lecture) ==


def get_spark_session(app_name: str = "shopflow-delta") -> SparkSession:
    """Build a Delta-enabled SparkSession.

    Mirrors the lecture's ``standalone/spark_config.py``. Wires the Delta SQL
    extension and the Delta catalog, then resolves Delta JARs via
    ``configure_spark_with_delta_pip``.
    """
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# == Recommendation rubric (prefilled from lecture) ==


def recommend_format(
    primary_pattern: str,
    engine_diversity: str,
    upsert_frequency: str,
) -> str:
    """Recommend an open table format based on workload characteristics.

    Rule order matters -- the multi-engine check fires before the
    streaming/upsert check because engine diversity dominates throughput
    concerns. The Task 4 critique asks you to defend this ordering.

    Args:
        primary_pattern: "batch", "streaming", or "hybrid"
        engine_diversity: "single" or "multi"
        upsert_frequency: "rare", "frequent", or "continuous"

    Returns:
        One of "delta", "hudi", or "iceberg".
    """
    if engine_diversity == "multi":
        return "iceberg"
    if upsert_frequency == "continuous" or primary_pattern == "streaming":
        return "hudi"
    return "delta"


def evaluate_format_tradeoffs(format_name: str) -> dict:
    """Return strengths, weaknesses, and best-fit scenario for a format.

    Lookup the format in ``comparison_report()`` and surface
    ``{strengths, weaknesses, best_for}``. Raise
    ``ValueError("Unknown format: ...")`` for any name that is not one of
    ``"delta"``, ``"hudi"``, or ``"iceberg"``.
    """
    raise NotImplementedError("Task 1: evaluate_format_tradeoffs")


# == Delta primitives (prefilled from lecture) ==


def delta_write(spark: SparkSession, path: str = DELTA_PATH) -> dict:
    """Write ~200 sample orders to Delta and record timing + file count.

    Prefilled from the lecture. Returns
    ``{format, write_time, row_count, file_count}``. The ``_delta_log/`` dir
    contains ``00000000000000000000.json`` after this call -- that v0 commit
    JSON is the format-specific signature of a Delta table.
    """
    if os.path.exists(path):
        shutil.rmtree(path)

    data = [
        Row(
            order_id=i,
            customer_id=i % 10,
            price=float((i + 1) * 5.99),
            status="delivered" if i % 2 == 0 else "pending",
        )
        for i in range(200)
    ]

    start = time.perf_counter()
    spark.createDataFrame(data).write.format("delta").mode("overwrite").save(path)
    write_time = time.perf_counter() - start

    file_count = sum(1 for f in os.listdir(path) if f.endswith(".parquet"))

    result = {
        "format": "delta",
        "write_time": float(write_time),
        "row_count": len(data),
        "file_count": file_count,
    }
    logger.info("Delta write: %s", result)
    return result


def delta_read(spark: SparkSession, path: str = DELTA_PATH) -> dict:
    """Read back from Delta and time the read.

    Prefilled from the lecture. Returns ``{format, read_time, row_count}``.
    """
    start = time.perf_counter()
    df = spark.read.format("delta").load(path)
    count = df.count()
    read_time = time.perf_counter() - start

    result = {
        "format": "delta",
        "read_time": float(read_time),
        "row_count": count,
    }
    logger.info("Delta read: %s", result)
    return result


def delta_upsert(spark: SparkSession, path: str = DELTA_PATH) -> dict:
    """MERGE upsert mixing existing and new ``order_id`` values.

    Prefilled from the lecture. Source rows 195-204 overlap (matched-update)
    and extend (unmatched-insert) the seed window. Returns
    ``{format, rows_after_upsert}``. Postcondition: a new
    ``_delta_log/00000000000000000001.json`` commit appears.
    """
    updates = [
        Row(order_id=i, customer_id=99, price=0.01, status="cancelled")
        for i in range(195, 205)
    ]
    updates_df = spark.createDataFrame(updates)

    dt = DeltaTable.forPath(spark, path)
    dt.alias("target").merge(
        updates_df.alias("source"),
        "target.order_id = source.order_id",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    final_count = spark.read.format("delta").load(path).count()
    result = {"format": "delta", "rows_after_upsert": final_count}
    logger.info("Delta upsert: %s", result)
    return result


def delta_delete(spark, path: str = DELTA_PATH, condition: str = "status = 'cancelled'") -> dict:
    """Delete rows matching ``condition`` from the Delta table.

    Use ``DeltaTable.forPath(spark, path).delete(condition)``. Return
    ``{format, remaining_rows, condition}``. Postcondition: a new commit
    JSON appears in ``_delta_log/`` (e.g. ``00000000000000000002.json``
    when called after write + upsert).
    """
    raise NotImplementedError("Task 2: delta_delete")


def delta_history(spark, path: str = DELTA_PATH) -> list[dict]:
    """Return the Delta transaction log history as a list of dicts.

    Use ``DeltaTable.forPath(spark, path).history().collect()`` and cast
    each row via ``row.asDict()``. The list grows by one entry per Delta
    commit: write -> 1 entry; write+upsert -> 2 entries; write+upsert+delete -> 3.
    """
    raise NotImplementedError("Task 2: delta_history")


# == Comparison report (prefilled from lecture) ==


def comparison_report() -> dict:
    """Structured comparison of Delta, Hudi, and Iceberg.

    Prefilled from the lecture's comparison table. Each entry exposes the
    canonical schema used by the rest of the lab:

        {
            "strengths":  list[str],
            "weaknesses": list[str],
            "best_for":   str,
            "scores": {
                "write_performance": int,   # 1-10
                "read_performance":  int,   # 1-10
                "schema_evolution":  int,   # 1-10
                "multi_engine":      int,   # 1-10
                "streaming":         int,   # 1-10
            },
            "acid": True,
        }
    """
    return {
        "delta": {
            "strengths": [
                "Spark-native performance",
                "Simple setup",
                "OPTIMIZE / Z-ORDER",
            ],
            "weaknesses": [
                "Weaker multi-engine support",
                "Limited partition evolution",
            ],
            "best_for": "Batch-dominant, Databricks ecosystem",
            "scores": {
                "write_performance": 9,
                "read_performance": 9,
                "schema_evolution": 6,
                "multi_engine": 4,
                "streaming": 7,
            },
            "acid": True,
        },
        "hudi": {
            "strengths": [
                "Streaming upserts",
                "CDC support",
                "MoR tables",
            ],
            "weaknesses": [
                "Higher complexity (CoW vs MoR)",
                "Smaller community",
            ],
            "best_for": "Continuous upserts, CDC pipelines",
            "scores": {
                "write_performance": 7,
                "read_performance": 7,
                "schema_evolution": 8,
                "multi_engine": 6,
                "streaming": 9,
            },
            "acid": True,
        },
        "iceberg": {
            "strengths": [
                "Multi-engine",
                "Partition evolution",
                "Vendor-neutral",
            ],
            "weaknesses": [
                "Less Spark-native optimization",
                "Manifest management overhead",
            ],
            "best_for": "Multi-engine environments, vendor neutrality",
            "scores": {
                "write_performance": 8,
                "read_performance": 8,
                "schema_evolution": 9,
                "multi_engine": 10,
                "streaming": 7,
            },
            "acid": True,
        },
    }


def format_recommendation() -> dict:
    """Apply ``recommend_format`` to four named scenarios.

    Return a dict with these four keys, each mapped to the format chosen by
    ``recommend_format`` for the matching workload profile:

        - "batch_single_engine"  -- batch / single / rare      -> "delta"
        - "streaming_upserts"    -- streaming / single / continuous -> "hudi"
        - "multi_engine_batch"   -- batch / multi / rare       -> "iceberg"
        - "hybrid_multi_engine"  -- hybrid / multi / frequent  -> "iceberg"
    """
    raise NotImplementedError("Task 3: format_recommendation")


# == Synthesis task ==


def benchmark_workload(spark: SparkSession, workload_profile: dict, path: str = DELTA_PATH) -> dict:
    """Benchmark a workload profile and produce a measurement-grounded recommendation.

    SYNTHESIS TASK: This recombines write benchmarking (delta_write), read
    benchmarking (delta_read), upsert benchmarking (delta_upsert), and the
    recommendation rubric (recommend_format) into a single decision-making
    routine.

    Args:
        workload_profile: dict with at minimum the keys
            - "kind": one of "read_heavy", "write_heavy", or "mixed"
            - "primary_pattern": "batch" | "streaming" | "hybrid"
            - "engine_diversity": "single" | "multi"
            - "upsert_frequency": "rare" | "frequent" | "continuous"
        path: Delta table path to use for the live measurements.

    Returns:
        dict with the following keys:
            - "workload": the input profile (echoed)
            - "write_seconds": measured wall-clock time for delta_write
            - "read_seconds": measured wall-clock time for delta_read
            - "upsert_seconds": measured wall-clock time for delta_upsert
            - "recommended_format": result of recommend_format(...) on the
              profile's pattern/engine/upsert fields
            - "justification": a short prose string that CITES at least one of
              the measured numbers above to two decimal places
              (e.g. ``"upsert took 4.21s, which dominates ..."``). The grader
              looks for a numeric reference -- a generic rule dump from
              ``comparison_report()`` does NOT satisfy this contract.

    Why this is synthesis (not rote): the lecture demonstrates each primitive
    in isolation. Here you must drive all three and let the *measured* numbers,
    not just the rule table, justify the recommendation. A correct answer for
    a write-heavy + continuous-upsert profile should mention the upsert time
    and conclude Hudi; a multi-engine profile should conclude Iceberg
    regardless of the timings (engine diversity dominates).
    """
    raise NotImplementedError("Task 5 (synthesis): benchmark_workload")


def medtrack_recommendation() -> dict:
    """Apply the recommendation rubric to the MedTrack case from the lecture.

    MedTrack profile (from the Day 18 lecture discussion):
        - 40 data engineers
        - 8K CDC events/sec from 12 hospital PostgreSQL DBs
        - 15 TB and growing 1.5 TB/month
        - Spark for batch (existing) + Trino licences just purchased (new)
        - Currently entirely on Delta Lake in Databricks

    The new Trino requirement makes the workload genuinely multi-engine.
    Build a workload profile dict and pass it to ``recommend_format``.
    Surface Delta's weaknesses from ``comparison_report`` so the migration
    cost trade-off is explicit.

    Returns:
        dict with:
            - profile: the workload profile dict you constructed
            - recommended_format: result of recommend_format on the profile
            - delta_weaknesses: comparison_report()['delta']['weaknesses']
            - notes: free-form one-paragraph rationale (string)
    """
    raise NotImplementedError("Task 6 (case study): medtrack_recommendation")


def run_pipeline() -> None:
    """Drive the full comparison end-to-end (write -> read -> upsert -> report).

    Wire the prefilled and student-implemented pieces together: build a
    SparkSession, run the Delta primitives against ``DELTA_PATH``, log the
    comparison report, and route a sample workload through
    ``benchmark_workload``.
    """
    raise NotImplementedError("Task 7: run_pipeline")
