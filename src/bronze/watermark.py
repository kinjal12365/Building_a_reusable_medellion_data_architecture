from __future__ import annotations
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

WATERMARK_SCHEMA = StructType([
    StructField("content_hash",        StringType(),    False),
    StructField("file_name",           StringType(),    False),
    StructField("status",              StringType(),    False),
    StructField("ingestion_timestamp", TimestampType(), True),
])

def load_watermark(spark: SparkSession, location: str) -> DataFrame:
    try:
        return spark.read.parquet(location)
    except Exception:
        return spark.createDataFrame([], schema=WATERMARK_SCHEMA)

def is_already_processed(watermark_df: DataFrame, content_hash: str) -> bool:
    if watermark_df.rdd.isEmpty():
        return False
    match = watermark_df.filter(
        (F.col("content_hash") == content_hash) &
        (F.col("status") == "bronze_loaded")
    )
    return match.count() > 0

def update_watermark(
    spark: SparkSession,
    location: str,
    content_hash: str,
    file_name: str,
    status: str = "bronze_loaded",
) -> None:
    new_row = spark.createDataFrame(
        [(content_hash, file_name, status, None)],
        schema=WATERMARK_SCHEMA,
    ).withColumn("ingestion_timestamp", F.current_timestamp())
    new_row.write.mode("append").parquet(location)