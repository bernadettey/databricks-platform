# Generate Bronze Ingestion Workflow from Dataset Registry

import sys
from pathlib import Path
from textwrap import dedent

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dbx_platform.config import DATASETS


def generate_bronze_workflow() -> str:
    """Generate one Databricks Job task per registered dataset."""

    if not DATASETS:
        raise ValueError("No datasets registered in config.py")

    tasks = []

    for dataset_name in sorted(DATASETS):
        task = dedent(f"""\
              - task_key: ingest_{dataset_name}_to_bronze
                notebook_task:
                  notebook_path: ../notebooks/01_bronze_ingestion.ipynb
                  base_parameters:
                    dataset_name: {dataset_name}
        """)
        tasks.append(task)

        header = dedent("""\
        # Auto-generated from src/dbx_platform/config.py
        # Do not edit manually.

        resources:
          jobs:
            bronze_ingestion_job:
              name: dbx_platform_bronze_ingestion

              queue:
                enabled: true

              max_concurrent_runs: 1

              schedule:
                quartz_cron_expression: "0 0 9 * * ?"
                timezone_id: Australia/Brisbane
                pause_status: PAUSED

              tasks:
    """)

    indented_tasks = "\n".join(
        "\n".join("      " + line for line in task.rstrip().splitlines())
        for task in tasks
    )

    return header + indented_tasks + "\n"


if __name__ == "__main__":
    output_path = PROJECT_ROOT / "resources" / "bronze_ingestion.job.yml"

    workflow_yaml = generate_bronze_workflow()
    output_path.write_text(workflow_yaml, encoding="utf-8")

    print(f"Generated workflow: {output_path}")
    print(f"Dataset tasks: {', '.join(sorted(DATASETS))}")