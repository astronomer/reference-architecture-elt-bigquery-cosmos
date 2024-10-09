from airflow.datasets import Dataset
from pendulum import datetime
from functools import reduce
import os
from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task

from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.constants import SourceRenderingBehavior
from airflow.models.baseoperator import chain

# GCP
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_default")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
_BQ_DATASET = os.getenv("BQ_DATASET", "cheese_store")
_LIST_OF_BASE_TABLES = ["users", "cheeses", "sales", "utms"]
_LIST_OF_BASE_TABLES_DATASETS = [
    Dataset(f"{_PROJECT_ID}:{_BQ_DATASET}.{table}") for table in _LIST_OF_BASE_TABLES
]

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id=_GCP_CONN_ID,
        profile_args={"project": _PROJECT_ID, "dataset": _BQ_DATASET},
    ),
)


@dag(
    dag_display_name="🛠️ Transform data in BigQuery",
    start_date=datetime(2024, 10, 1),
    schedule=reduce(lambda x, y: x & y, _LIST_OF_BASE_TABLES_DATASETS),
    catchup=False,
    tags=["ELT"],
)
def transform_data_in_bq():

    @task(
        inlets=[_LIST_OF_BASE_TABLES_DATASETS]
    )
    def define_inlets():
        return "Inlets defined"

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
