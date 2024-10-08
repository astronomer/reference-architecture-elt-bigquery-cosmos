"""
## Extract data from an API and load it to GCS

To use a different remote storage option replace the GCSCreateBucketOperator,
as well as change the OBJECT_STORAGE_DST, CONN_ID_DST and KEY_DST
parameters.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models.param import Param
from pendulum import datetime, duration
import pandas as pd
import logging
import os

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# GCP
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_default")
_GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "my-bucket")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "cheese-sales-ingest")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")


# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="📊 Extract data from the internal API and load it to GCS",
    start_date=datetime(2024, 8, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    params={
        "num_sales": Param(
            100,
            description="The number of sales to fetch from the API.",
            type="number",
        ),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["ETL", "GCS", "Internal API"],
)
def api_to_GCS():

    # ---------------- #
    # Task Definitions #
    # ---------------- #
    # the @task decorator turns any Python function into an Airflow task
    # any @task decorated function that is called inside the @dag decorated
    # function is automatically added to the DAG.
    # if one exists for your use case you can still use traditional Airflow operators
    # and mix them with @task decorators. Checkout registry.astronomer.io for available operators
    # see: https://www.astronomer.io/docs/learn/airflow-decorators for information about @task
    # see: https://www.astronomer.io/docs/learn/what-is-an-operator for information about traditional operators

    # create the GCS bucket if it does not exist yet
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        gcp_conn_id=_GCP_CONN_ID,
        bucket_name=_GCS_BUCKET_NAME,
        project_id=_PROJECT_ID,
    )

    @task
    def get_new_sales_from_api(**context) -> list[pd.DataFrame]:
        """
        Get new sales data from an internal API.
        Args:
            num_sales (int): The number of sales to fetch.
        Returns:
            list[pd.DataFrame]: A list of DataFrames containing data relating
            to the newest sales.
        """
        num_sales = context["params"]["num_sales"]
        date = context["ts"]
        from include.api_functions import get_new_sales_from_internal_api

        sales_df, users_df, cheeses_df, utm_df = get_new_sales_from_internal_api(
            num_sales, date
        )

        t_log.info(f"Fetching {num_sales} new sales from the internal API.")
        t_log.info(f"Head of the new sales data: {sales_df.head()}")
        t_log.info(f"Head of the new users data: {users_df.head()}")
        t_log.info(f"Head of the new cheese data: {cheeses_df.head()}")
        t_log.info(f"Head of the new utm data: {utm_df.head()}")

        return [
            {"name": "sales", "data": sales_df},
            {"name": "users", "data": users_df},
            {"name": "cheeses", "data": cheeses_df},
            {"name": "utms", "data": utm_df},
        ]

    get_new_sales_from_api_obj = get_new_sales_from_api()

    # TODO: add transform step here

    @task(
        map_index_template="{{ my_custom_map_index }}",
    )
    def write_to_gcs(data_to_write: pd.DataFrame, **context):
        """
        Write the data to an GCS bucket.
        Args:
            data_to_write (pd.DataFrame): The data to write to GCS.
            base_dst (ObjectStoragePath): The base path to write the data to.
        """
        import io

        data = data_to_write["data"]
        name = data_to_write["name"]
        dag_run_id = context["dag_run"].run_id

        csv_buffer = io.BytesIO()
        data.to_csv(csv_buffer, index=False)

        csv_bytes = csv_buffer.getvalue()

        hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)

        hook.upload(
            bucket_name=_GCS_BUCKET_NAME,
            object_name=f"{_INGEST_FOLDER_NAME}/{name}/{dag_run_id}.csv",
            data=csv_bytes,
        )

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Wrote new {name} data to GCS."

    write_to_gcs_obj = write_to_gcs.expand(data_to_write=get_new_sales_from_api_obj)

    @task(outlets=[Dataset(f"gs://{_GCS_BUCKET_NAME}/{_INGEST_FOLDER_NAME}/*")])
    def update_dataset():
        return "Cheese data ready in GCS!"

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(create_bucket, get_new_sales_from_api_obj, write_to_gcs_obj, update_dataset())


api_to_GCS()
