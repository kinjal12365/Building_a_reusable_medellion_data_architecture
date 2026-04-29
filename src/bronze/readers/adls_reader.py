from __future__ import annotations
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, MapType, StringType
from bronze.config.models import JobConfig
from bronze.discovery.format_detector import detect_format


def read_from_adls(
    spark: SparkSession,
    uri: str,
    job: JobConfig,
) -> tuple[DataFrame, DataFrame]:
    fmt = detect_format(uri, config_override=job.source.format)
    if fmt == "csv":
        return _read_csv(spark, uri, job)
    elif fmt == "json":
        return _read_json(spark, uri, job)
    elif fmt == "parquet":
        return _read_parquet(spark, uri)
    else:
        raise ValueError(f"Unsupported format {fmt} for URI: {uri}")


def _read_csv(spark, uri, job):
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
    return _split_and_cast(spark, raw_df)


def _read_json(spark, uri, job):
    opts = job.options.json_opts
    raw_df = (
        spark.read
        .option("multiLine", str(opts.multiline).lower())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(uri)
    )
    return _split_and_cast(spark, raw_df)


def _read_parquet(spark, uri):
    raw_df = spark.read.parquet(uri)
    clean_df = _cast_all_to_string(raw_df)
    corrupt_df = spark.createDataFrame([], schema="corrupt_record STRING")
    return clean_df, corrupt_df


def _split_and_cast(spark, raw_df):
    if "_corrupt_record" in raw_df.columns:
        corrupt_df = raw_df.filter(F.col("_corrupt_record").isNotNull())
        clean_df = (
            raw_df
            .filter(F.col("_corrupt_record").isNull())
            .drop("_corrupt_record")
        )
    else:
        clean_df = raw_df
        corrupt_df = spark.createDataFrame([], schema="corrupt_record STRING")

    clean_df = _cast_all_to_string(clean_df)
    return clean_df, corrupt_df


def _cast_all_to_string(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            df = df.withColumn(field.name, F.to_json(F.col(field.name)))
        else:
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
    return df