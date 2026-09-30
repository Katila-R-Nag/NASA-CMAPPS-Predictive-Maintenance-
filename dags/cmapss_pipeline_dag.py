"""
Orchestrates the C-MAPSS pipeline: extract & load raw files into Postgres,
then run and test the dbt project that builds the health-indicator and
fleet-summary models.

Written for Airflow 3.x: DAG/task decorators come from airflow.sdk, and
operators (BashOperator) come from the separate apache-airflow-providers-
standard package -- both moved out of core Airflow in the 3.0 restructure.

C-MAPSS is a static, one-time dataset -- it never actually gets a "new batch"
tomorrow. This DAG is still written on a daily schedule deliberately, to
demonstrate how the same pipeline would run against a real, continuously
arriving sensor feed. That's a design decision worth stating explicitly in
your project README, not something to gloss over.
"""

import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

# This matches the volume mount in docker-compose.yml: the whole project
# folder is mounted read-only at /opt/airflow/project inside the container.
PROJECT_ROOT = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt/cmapss_dbt"
RAW_DATA_DIR = f"{PROJECT_ROOT}/data/raw"

default_args = {
    "owner": "analytics",
    "retries": 2,
}


@dag(
    dag_id="cmapss_pipeline",
    description="Extract, load, transform, and test C-MAPSS engine sensor data",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["cmapss", "predictive-maintenance", "analytics-engineering"],
)
def cmapss_pipeline():

    @task
    def extract_and_load(dataset_id: str = "FD001"):
        import subprocess

        subprocess.run(
            [
                "python",
                f"{PROJECT_ROOT}/scripts/load_raw_data.py",
                "--file",
                f"{RAW_DATA_DIR}/train_{dataset_id}.txt",
                "--dataset-id",
                dataset_id,
            ],
            check=True,
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    extract_and_load() >> dbt_run >> dbt_test


cmapss_pipeline()
