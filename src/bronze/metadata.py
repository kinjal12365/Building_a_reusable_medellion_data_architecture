"""
metadata.py
-----------
Attaches the 8 mandatory Bronze metadata columns to any Spark DataFrame.

Columns added:
    _batch_id            : Groups all files in one pipeline run
    _run_id              : Unique ID for this execution
    _source_file_name    : Name of the source file
    _source_file_hash    : SHA-256 hash of the source file
    _ingestion_timestamp : Exact moment of ingestion
    _ingestion_date      : Date only — used for partitioning
    _record_seq          : Row number within the file
    _source_row_uuid     : Globally unique ID per row
"""

from __future__ import annotations
import uuid
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def attach_metadata(
    df: DataFrame,
    batch_id: str,
    run_id: str,
    source_file_name: str,
    source_file_hash: str,
) -> DataFrame:
    """
    Attach 8 Bronze metadata columns to a DataFrame.

    Args:
        df:                 Any Spark DataFrame (all columns already STRING)
        batch_id:           Batch ID for this pipeline run
        run_id:             Unique run ID for this execution
        source_file_name:   Filename of the source file
        source_file_hash:   SHA-256 hex digest of the source file

    Returns:
        DataFrame with 8 additional metadata columns.
    """

    return (
        df
        # Static values — same for every row in this file
        .withColumn("_batch_id",             F.lit(batch_id))
        .withColumn("_run_id",               F.lit(run_id))
        .withColumn("_source_file_name",     F.lit(source_file_name))
        .withColumn("_source_file_hash",     F.lit(source_file_hash))

        # Timestamps — same for every row in this file
        .withColumn("_ingestion_timestamp",  F.current_timestamp())
        .withColumn("_ingestion_date",       F.current_date())

        # Row-level — unique per row
        .withColumn("_record_seq",           F.monotonically_increasing_id())
        .withColumn("_source_row_uuid",      F.expr("uuid()"))
    )


def generate_run_id() -> str:
    """Generate a unique run ID for this pipeline execution."""
    return str(uuid.uuid4())


def generate_batch_id(job_id: str, run_id: str) -> str:
    """
    Generate a batch ID — combines job_id and run_id for traceability.
    Format: <job_id>__<first 8 chars of run_id>
    """
    return f"{job_id}__{run_id[:8]}"