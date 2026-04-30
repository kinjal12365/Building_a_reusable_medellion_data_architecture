from __future__ import annotations
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from bronze.config.models import JobConfig
from bronze.discovery.file_lister import list_files
from bronze.discovery.format_detector import detect_format
from bronze.metadata import attach_metadata, generate_run_id, generate_batch_id
from bronze.watermark import load_watermark, update_watermark, is_already_processed


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
        return read_from_adls
    else:
        raise ValueError(f"Unsupported URI scheme: {scheme}")


def _delete_partial_rows(
    spark: SparkSession,
    bronze_location: str,
    content_hash: str,
) -> int:
    """
    If a previous crashed run left partial rows for this file hash,
    delete them before reloading.
    Returns count of deleted rows (0 if none found).
    """
    try:
        existing_df = spark.read.parquet(bronze_location)

        partial_rows = existing_df.filter(
            F.col("_source_file_hash") == content_hash
        ).count()

        if partial_rows > 0:
            print(f"[Bronze] Found {partial_rows} partial rows from previous crashed run — cleaning up...")

            # Keep all rows EXCEPT the partial ones from this file
            clean_df = existing_df.filter(
                F.col("_source_file_hash") != content_hash
            )

            # Overwrite the bronze table with cleaned data
            (
                clean_df.write
                .mode("overwrite")
                .partitionBy("_ingestion_date")
                .parquet(bronze_location)
            )

            print(f"[Bronze] Deleted {partial_rows} partial rows. Bronze table is clean.")
            return partial_rows

    except Exception:
        # Table doesn't exist yet — first run, nothing to clean
        pass

    return 0


def _check_partial_write(
    spark: SparkSession,
    bronze_location: str,
    content_hash: str,
) -> bool:
    """
    Check if this file hash has any rows already in bronze.
    Returns True if partial rows exist (crashed previous run).
    """
    try:
        existing_df = spark.read.parquet(bronze_location)
        count = existing_df.filter(
            F.col("_source_file_hash") == content_hash
        ).count()
        return count > 0
    except Exception:
        return False


def run_bronze_job(
    spark: SparkSession,
    job: JobConfig,
    watermark_location: str,
) -> dict:
    run_id   = generate_run_id()
    batch_id = generate_batch_id(job.job_id, run_id)

    print(f"[Bronze] Starting job: {job.job_id}")
    print(f"[Bronze] run_id:   {run_id}")
    print(f"[Bronze] batch_id: {batch_id}")

    discovered = list_files(job.source.uri, pattern=job.source.pattern)
    print(f"[Bronze] Files discovered: {len(discovered)}")

    watermark_df = load_watermark(spark, watermark_location)

    stats = {
        "files_found"    : len(discovered),
        "files_skipped"  : 0,
        "files_loaded"   : 0,
        "files_recovered": 0,
        "rows_loaded"    : 0,
        "corrupt_rows"   : 0,
    }

    for file in discovered:

        # Step 1 — Check watermark first
        if is_already_processed(watermark_df, file.content_hash):
            print(f"[Bronze] Skipping (already loaded): {file.file_name}")
            stats["files_skipped"] += 1
            continue

        # Step 2 — Check for partial rows from a previous crashed run
        has_partial = _check_partial_write(
            spark,
            job.bronze.location,
            file.content_hash
        )

        if has_partial:
            print(f"[Bronze] Detected partial write for {file.file_name} — recovering...")
            deleted = _delete_partial_rows(
                spark,
                job.bronze.location,
                file.content_hash
            )
            stats["files_recovered"] += 1

        print(f"[Bronze] Processing: {file.file_name}")

        try:
            fmt    = detect_format(file.uri, config_override=job.source.format)
            reader = _get_reader(file.scheme)
            clean_df, corrupt_df = reader(spark, file.uri, job)

            clean_df = attach_metadata(
                clean_df,
                batch_id        = batch_id,
                run_id          = run_id,
                source_file_name= file.file_name,
                source_file_hash= file.content_hash,
            )

            if corrupt_df.count() > 0:
                corrupt_df = (
                    corrupt_df
                    .withColumn("_source_file_name",    F.lit(file.file_name))
                    .withColumn("_source_file_hash",    F.lit(file.content_hash))
                    .withColumn("_batch_id",            F.lit(batch_id))
                    .withColumn("_run_id",              F.lit(run_id))
                    .withColumn("_ingestion_timestamp", F.current_timestamp())
                )

            clean_count = clean_df.count()
            (
                clean_df.write
                .mode("append")
                .partitionBy("_ingestion_date")
                .parquet(job.bronze.location)
            )

            corrupt_count = corrupt_df.count()
            if corrupt_count > 0:
                corrupt_location = job.bronze.location.rsplit("/", 1)[0] + "/bronze_corrupt/"
                corrupt_df.write.mode("append").parquet(corrupt_location)

            # Step 3 — Only update watermark AFTER successful write
            update_watermark(
                spark,
                watermark_location,
                file.content_hash,
                file.file_name,
                status="bronze_loaded",
            )

            stats["files_loaded"] += 1
            stats["rows_loaded"]  += clean_count
            stats["corrupt_rows"] += corrupt_count

            print(f"[Bronze] Loaded: {file.file_name} — {clean_count} rows, {corrupt_count} corrupt")

        except Exception as exc:
            print(f"[Bronze] ERROR on {file.file_name}: {exc}")
            raise

    print(f"[Bronze] Job complete: {stats}")
    return stats