# Reusable Bronze Ingestion Engine

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery


# --------------------------------------------------
# 1. Build Auto Loader Stream
# --------------------------------------------------

def build_bronze_stream(
    spark: SparkSession,
    source_path: str,
    schema: StructType,
    run_id: str,
    date_format: str = "yyyy-MM-dd",
    timestamp_format: str = "yyyy-MM-dd HH:mm:ss",
) -> DataFrame:
    """Build an Auto Loader stream with source lineage and audit metadata."""

    stream_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .option("dateFormat", date_format)
        .option("timestampFormat", timestamp_format)
        .schema(schema)
        .load(source_path)
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )
        .withColumn(
            "_source_modified_at",
            F.col("_metadata.file_modification_time")
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp()
        )
        .withColumn(
            "_ingestion_run_id",
            F.lit(run_id)
        )
    )

    return stream_df


# --------------------------------------------------
# 2. Execute Bronze Ingestion
# --------------------------------------------------

def execute_bronze_ingestion(
    stream_df: DataFrame,
    target_table: str,
    checkpoint_path: str,
    query_name: str,
) -> StreamingQuery:
    """Execute incremental Bronze ingestion into a Delta table."""

    query = (
        stream_df.writeStream
        .queryName(query_name)
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    # Wait for ingestion to finish
    query.awaitTermination()

    # Validate query completion
    if query.exception() is not None:
        raise RuntimeError(
            f"Bronze ingestion failed: {query_name}"
        )

    return query