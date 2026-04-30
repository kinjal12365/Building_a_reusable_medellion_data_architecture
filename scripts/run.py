"""
run.py
------
CLI entry point for the Bronze pipeline.

Usage:
    python scripts/run.py --config configs/example.yaml --job-id customers_raw

Arguments:
    --config     Path to the YAML config file
    --job-id     Job ID to run (must exist in the config file)
    --watermark  Optional. Watermark table location.
                 Defaults to <bronze_location>/watermark/
"""

import argparse
import os
import sys

def parse_args():
    parser = argparse.ArgumentParser(
        description="Bronze Pipeline — source to bronze ingestion"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the YAML config file e.g. configs/example.yaml"
    )
    parser.add_argument(
        "--job-id",
        required=True,
        help="Job ID to run e.g. customers_raw"
    )
    parser.add_argument(
        "--watermark",
        required=False,
        default=None,
        help="Watermark table location. Defaults to <bronze_location>/../watermark/"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Add src to path so bronze package is importable
    # Works both as CLI script and via exec() in Databricks
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)) if '__file__' in dir() else '/Workspace/Users/kinjal.kanjilal.aiml25@heritageit.edu.in/Building_a_reusable_medellion_data_architecture', 'src')
    sys.path.insert(0, src_path)

    # Imports after path setup
    from pyspark.sql import SparkSession
    from bronze.config import YamlConfigStore
    from bronze.loader import run_bronze_job

    print(f"[CLI] Config:  {args.config}")
    print(f"[CLI] Job ID:  {args.job_id}")

    # Load config
    store = YamlConfigStore(args.config)
    job = store.get_job(args.job_id)

    # Default watermark location
    watermark_location = args.watermark
    if watermark_location is None:
        # Place watermark one level up from bronze table
        base = job.bronze.location.rstrip("/").rsplit("/", 1)[0]
        watermark_location = f"{base}/watermark/"

    print(f"[CLI] Watermark: {watermark_location}")

    # Build SparkSession
    spark = (
        SparkSession.builder
        .appName(f"bronze_{args.job_id}")
        .getOrCreate()
    )

    # Run the job
    stats = run_bronze_job(spark, job, watermark_location)

    print(f"[CLI] Done! Stats: {stats}")
    return stats


if __name__ == "__main__":
    main()