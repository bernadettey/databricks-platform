# Test metadata-driven workflow generation

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dbx_platform.config import DATASETS
from scripts.generate_bronze_workflow import generate_bronze_workflow


def test_workflow_contains_registered_datasets():
    """Every registered dataset should have its own ingestion task."""

    workflow_yaml = generate_bronze_workflow()

    for dataset_name in DATASETS:
        expected_task = f"ingest_{dataset_name}_to_bronze"

        assert expected_task in workflow_yaml