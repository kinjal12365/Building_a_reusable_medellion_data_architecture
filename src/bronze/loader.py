"""
loader.py
---------
Bronze loader — orchestrates the full source-to-bronze pipeline for one job.

Flow per file:
  1. Discover files under source URI
  2. Skip already-processed files (watermark check)
  3. Detect format
  4. Read via source-specific reader (ADLS/S3/GCS)
  5. Attach 8 metadata columns
  6. Append clean rows to bronze_<dataset>
  7. Append corrupt rows to bronze_corrupt
  8. Update watermark to bronze_loaded
"""

from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bronze.config.models import JobConfig
from bronze.discovery.file_lister import list_files, DiscoveredFile
from bronze.discovery.format_detector import detect_format
from bronze.metadata import attach_metadata, generate_run_id, generate_batch_id
from bronze.watermark import load_watermark, update_watermark, is_already_processed


# ---------------------------------------------------------------------------
# URI scheme router — picks the right reader
# ---------------------------------------------------------------------------

def _get_reader(scheme: str):
    if scheme == "abfss":
        from bronze.readers.adls_reader import read_from_adls
        return read_from_adls
    elif scheme == "s3a":
        from bronze.readers.s3_reader import read_from_s3
        return read_from_s3
    elif scheme == "gs":
        from bronze.readers.gcs_reader import read_from_gcs
        return read_from_gcs
    elif scheme == "file":
        from bronze.readers.adls_reader import read_from_adls
        return read_from_adls  # local uses same Spark reader
    else:
        raise ValueError(f"Unsupported URI scheme: {scheme}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_bronze_job(
    spark: SparkSession,
    job: JobConfig,
    watermark_location: str,
) -> dict:
    """
    Run the full Bronze ingestion for one job.

    Args:
        spark:               Active SparkSession
        job:                 JobConfig for this dataset
        watermark_location:  Path where the watermark Parquet table lives

    Returns:
        dict with run stats: files_found, files_skipped, files_loaded,
                             rows_loaded, corrupt_rows
    """

    run_id   = generate_run_id()
    batch_id = generate_batch_id(job.job_id, run_id)

    print(f"[Bronze] Starting job: {job.job_id}")
    print(f"[Bronze] run_id:   {run_id}")
    print(f"[Bronze] batch_id: {batch_id}")

    # ------------------------------------------------------------------
    # Step 1 — Discover files
    # ------------------------------------------------------------------
    discovered = list_files(job.source.uri, pattern=job.source.pattern)
    print(f"[Bronze] Files discovered: {len(discovered)}")

    # ------------------------------------------------------------------
    # Step 2 — Load watermark (already processed files)
    # ------------------------------------------------------------------
    watermark_df = load_watermark(spark, watermark_location)

    stats = {
        "files_found"  : len(discovered),
        "files_skipped": 0,
        "files_loaded" : 0,
        "rows_loaded"  : 0,
        "corrupt_rows" : 0,
    }

    # ------------------------------------------------------------------
    # Step 3 — Process each file
    # ------------------------------------------------------------------
    for file in discovered:

        # Skip if already processed
        if is_already_processed(watermark_df, file.content_hash):
            print(f"[Bronze] Skipping (already loaded): {file.file_name}")
            stats["files_skipped"] += 1
            continue

        print(f"[Bronze] Processing: {file.file_name}")

        try:
            # Detect format and get right reader
            fmt    = detect_format(file.uri, config_override=job.source.format)
            reader = _get_reader(file.scheme)

            # Read file → clean + corrupt DataFrames
            clean_df, corrupt_df = reader(spark, file.uri, job)

            # Attach metadata to clean rows
            clean_df = attach_metadata(
                clean_df,
                batch_id        = batch_id,
                run_id          = run_id,
                source_file_name= file.file_name,
                source_file_hash= file.content_hash,
            )

            # Add source info to corrupt rows
            if corrupt_df.count() > 0:
                corrupt_df = (
                    corrupt_df
                    .withColumn("_source_file_name", F.lit(file.file_name))
                    .withColumn("_source_file_hash", F.lit(file.content_hash))
                    .withColumn("_batch_id",         F.lit(batch_id))
                    .withColumn("_run_id",            F.lit(run_id))
                    .withColumn("_ingestion_timestamp", F.current_timestamp())
                )

            # Write clean rows to bronze_<dataset>
            clean_count = clean_df.count()
            (
                clean_df.write
                .mode("append")
                .partitionBy("_ingestion_date")
                .parquet(f"{job.bronze.location}")
            )

            # Write corrupt rows to bronze_corrupt
            corrupt_count = corrupt_df.count()
            if corrupt_count > 0:
                corrupt_location = job.bronze.location.rsplit("/", 1)[0] + "/bronze_corrupt/"
                (
                    corrupt_df.write
                    .mode("append")
                    .parquet(corrupt_location)
                )

            # Update watermark
            update_watermark(
                spark,
                watermark_location,
                file.content_hash,
                file.file_name,
                status="bronze_loaded",
            )

            stats["files_loaded"]  += 1
            stats["rows_loaded"]   += clean_count
            stats["corrupt_rows"]  += corrupt_count

            print(f"[Bronze] Loaded: {file.file_name} — {clean_count} rows, {corrupt_count} corrupt")

        except Exception as exc:
            # Bronze never fails on data content
            # Only infra errors bubble up
            print(f"[Bronze] ERROR on {file.file_name}: {exc}")
            raise

    print(f"[Bronze] Job complete: {stats}")
    return stats