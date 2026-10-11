# Source Configuration

DATASETS = {
    "transactions": {
        "landing_folder": "transactions",
        "checkpoint_name": "bronze_transactions",
        "table_name": "transactions",
    },
    "routes": {
        "landing_folder": "routes",
        "checkpoint_name": "bronze_routes",
        "table_name": "routes",
    },
}


def get_bronze_config(
    catalog: str,
    platform_schema: str,
    bronze_schema: str,
) -> dict:
    """Build Bronze ingestion configurations for all datasets."""

    volume_root = f"/Volumes/{catalog}/{platform_schema}"

    return {
        dataset_name: {
            "source_path": (
                f"{volume_root}/landing/{settings['landing_folder']}"
            ),
            "checkpoint_path": (
                f"{volume_root}/checkpoints/{settings['checkpoint_name']}"
            ),
            "target_table": (
                f"{catalog}.{bronze_schema}.{settings['table_name']}"
            ),
        }
        for dataset_name, settings in DATASETS.items()
    }