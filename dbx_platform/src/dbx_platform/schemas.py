"""Explicit source schemas used by Bronze ingestion."""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("booking_date", DateType(), True),
        StructField("route_code", StringType(), True),
        StructField("amount", DecimalType(12, 2), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


ROUTE_SCHEMA = StructType(
    [
        StructField("route_code", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("distance_km", IntegerType(), True),
        StructField("region", StringType(), True),
    ]
)


DATASET_SCHEMAS = {
    "transactions": TRANSACTION_SCHEMA,
    "routes": ROUTE_SCHEMA,
}


def get_dataset_schema(dataset: str) -> StructType:
    """Return the explicit source schema for a supported dataset."""
    normalized_dataset = dataset.strip().lower()

    if normalized_dataset not in DATASET_SCHEMAS:
        supported = ", ".join(sorted(DATASET_SCHEMAS))
        raise ValueError(
            f"Unsupported dataset '{dataset}'. Supported datasets: {supported}"
        )

    return DATASET_SCHEMAS[normalized_dataset]