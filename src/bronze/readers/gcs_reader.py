"""
gcs_reader.py
-------------
Reads files from Google Cloud Storage into Bronze-ready DataFrames.
- Auto-detects format (CSV, JSON, Parquet) via format_detector
- All columns cast to STRING
- PERMISSIVE mode — malformed rows separated, never dropped
"""

from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType

from bronze.config.models import JobConfig
from bronze.discovery.format_detector import detect_format


def read_from_gcs(
    spark: SparkSession,
    uri: str,
    job: JobConfig,
) -> tuple[DataFrame, DataFrame]:
    """
    Read a file from Google Cloud Storage.

    Args:
        spark: Active SparkSession
        uri:   Full gs:// URI to the file
        job:   JobConfig for this dataset

    Returns:
        (clean_df, corrupt_df)
    """

    fmt = detect_format(uri, config_override=job.source.format)

    if fmt == "csv":
        return _read_csv(spark, uri, job)
    elif fmt == "json":
        return _read_json(spark, uri, job)
    elif fmt == "parquet":
        return _read_parquet(spark, uri)
    else:
        raise ValueError(f"Unsupported format '{fmt}' for URI: {uri}")


def _read_csv(
    spark: SparkSession,
    uri: str,
    job: JobConfig,
) -> tuple[DataFrame, DataFrame]:

    opts = job.options.csv

    raw_df = (
        spark.read
        .option("header", str(opts.header).lower())
        .option("delimiter", opts.delimiter)
        .option("encoding", opts.encoding)
        .option("multiLine", str(opts.multiline).lower())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(uri)
    )

    return _split_and_cast(raw_df)


def _read_json(
    spark: SparkSession,
    uri: str,
    job: JobConfig,
) -> tuple[DataFrame, DataFrame]:

    opts = job.options.json_opts

    raw_df = (
        spark.read
        .option("multiLine", str(opts.multiline).lower())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(uri)
    )

    return _split_and_cast(raw_df)


def _read_parquet(
    spark: SparkSession,
    uri: str,
) -> tuple[DataFrame, DataFrame]:

    raw_df = spark.read.parquet(uri)
    clean_df = _cast_all_to_string(raw_df)
    corrupt_df = spark.createDataFrame([], schema="corrupt_record STRING")

    return clean_df, corrupt_df


def _split_and_cast(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    corrupt_df = raw_df.filter(F.col("_corrupt_record").isNotNull())
    clean_df = (
        raw_df
        .filter(F.col("_corrupt_record").isNull())
        .drop("_corrupt_record")
    )
    clean_df = _cast_all_to_string(clean_df)
    return clean_df, corrupt_df


def _cast_all_to_string(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))
        else:
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
    return df