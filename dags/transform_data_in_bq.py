"""
## Run transformations in BigQuery using dbt with Cosmos

This DAG creates a task group based on a dbt Core project using
Astronomer Cosmos. It runs dbt transformations in BigQuery
to create enriched tables about cheese sales.
"""

import os
from functools import reduce
from pathlib import Path

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from pendulum import datetime, duration

# GCP variables
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_conn")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
_BQ_DATASET = os.getenv("BQ_DATASET", "cheese_store")

_LIST_OF_BASE_TABLES = ["users", "cheeses", "sales", "utms"]
_LIST_OF_BASE_TABLES_DATASETS = [
    Dataset(f"{_PROJECT_ID}:{_BQ_DATASET}.{table}") for table in _LIST_OF_BASE_TABLES
]

# Cosmos variables

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# profile config for Cosmos using the Airflow connection to connect to GCP
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id=_GCP_CONN_ID,
        profile_args={"project": _PROJECT_ID, "dataset": _BQ_DATASET},
    ),
)


# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🛠️ Transform data in BigQuery",  # The name of the DAG displayed in the Airflow UI
    start_date=datetime(2024, 10, 18),  # date after which the DAG can be scheduled
    schedule=reduce(
        lambda x, y: x & y, _LIST_OF_BASE_TABLES_DATASETS
    ),  # this DAG uses a Dataset schedule, see https://www.astronomer.io/docs/learn/airflow-datasets
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Data team",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(minutes=1),  # tasks wait 1 minute in between retries
    },
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    description="Load",  # description next to the DAG name in the UI
    tags=["L", "GCS", "BigQuery"],  # add tags in the UI
)
def transform_data_in_bq():

    @task(inlets=[_LIST_OF_BASE_TABLES_DATASETS])
    def define_inlets():
        return "Inlets defined"

    # use Cosmos to create a task group based on the models in a dbt project
    tg = DbtTaskGroup(
        project_config=ProjectConfig(
            DBT_ROOT_PATH / "transform_cheese_sales",
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,
            "full_refresh": True,
        },
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        ),
        render_config=RenderConfig(
            source_rendering_behavior="all",
        ),
    )

    chain(
        define_inlets(),
        tg,
    )


transform_data_in_bq()
