"""
## Load data from GCS to BigQuery

This DAG loads data from GCS to BigQuery using the BigQuery Data Transfer Service.
"""

import os
from typing import cast

from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor,
)
from pendulum import datetime, duration

# GCP variables
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_conn")
_GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "my-bucket")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "cheese-sales-ingest")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
_BQ_DATASET = os.getenv("BQ_DATASET", "cheese_store")

_LIST_OF_BASE_TABLES = ["users", "cheeses", "sales", "utms"]


# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🐘 Load data from GCS to BigQuery",  # The name of the DAG displayed in the Airflow UI
    start_date=datetime(2024, 10, 18),  # date after which the DAG can be scheduled
    schedule=[
        Dataset(f"gs://{_GCS_BUCKET_NAME}/{_INGEST_FOLDER_NAME}/*")
    ],  # this DAG uses a Dataset schedule, see https://www.astronomer.io/docs/learn/airflow-datasets
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
def load_to_bigquery():

    @task(inlets=[Dataset(f"gs://{_GCS_BUCKET_NAME}/{_INGEST_FOLDER_NAME}/*")])
    def define_inlets() -> str:
        return "Inlets defined"

    # create the BQ dataset if it doesn't exist yet
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", gcp_conn_id=_GCP_CONN_ID, dataset_id=_BQ_DATASET
    )

    @task
    def create_table_creation_configs() -> list[dict]:
        """
        Create a list of configurations for creating tables in BigQuery.
        Returns:
            list[dict]: List of configurations for creating tables in BigQuery.
        """
        list_of_configs = []

        for table in _LIST_OF_BASE_TABLES:
            with open(f"include/table_creation_queries/{table}.sql") as f:
                sql = f.read()
                sql = sql.replace("{dataset}", _BQ_DATASET)

                configuration = {
                    "query": {
                        "query": sql,
                        "useLegacySql": False,
                    }
                }

                list_of_configs.append(configuration)

        return list_of_configs

    create_table = BigQueryInsertJobOperator.partial(
        task_id="create_table",
        gcp_conn_id=_GCP_CONN_ID,
        location="US",
        map_index_template="Creating table: {{ task.configuration['query']['query'][27:47] }}",
    ).expand(configuration=create_table_creation_configs())

    # create a task group to transfer data for each table
    @task_group
    def transfer_data(table):

        @task
        def create_transfer_config(table, **context) -> dict:
            """
            Create a transfer configuration for the given table.
            Args:
                table (str): The name of the table to transfer.
            Returns:
                dict: The transfer configuration.
            """

            ts = context["ts"]

            transfer_config = {
                "destination_dataset_id": _BQ_DATASET,
                "display_name": f"Data transfer for {table} - {ts}",
                "data_source_id": "google_cloud_storage",
                "schedule_options": {"disable_auto_scheduling": True},
                "params": {
                    "field_delimiter": ",",
                    "max_bad_records": "0",
                    "skip_leading_rows": "1",
                    "data_path_template": "gs://"
                    + _GCS_BUCKET_NAME
                    + "/"
                    + _INGEST_FOLDER_NAME
                    + "/"
                    + f"{table}/*.csv",
                    "destination_table_name_template": f"{table}",
                    "file_format": "CSV",
                },
            }

            return transfer_config

        create_transfer_config_obj = create_transfer_config(table)

        gcp_bigquery_create_transfer = BigQueryCreateDataTransferOperator(
            transfer_config=create_transfer_config_obj,
            gcp_conn_id=_GCP_CONN_ID,
            project_id=_PROJECT_ID,
            task_id="gcp_bigquery_create_transfer",
            map_index_template="Creating transfer config for table: {{task.transfer_config['params']['destination_table_name_template']}}",
        )

        # get the transfer config id from the XCom of the previous task
        transfer_config_id = cast(
            str, XComArg(gcp_bigquery_create_transfer, key="transfer_config_id")
        )

        gcp_bigquery_start_transfer = BigQueryDataTransferServiceStartTransferRunsOperator(
            task_id="gcp_bigquery_start_transfer",
            project_id=_PROJECT_ID,
            gcp_conn_id=_GCP_CONN_ID,
            transfer_config_id=transfer_config_id,
            requested_run_time={"seconds": 10},
            map_index_template="Starting transfer for config: {{task.transfer_config_id}}",
        )

        gcp_run_sensor = BigQueryDataTransferServiceTransferRunSensor(
            task_id="gcp_run_sensor",
            transfer_config_id=transfer_config_id,
            project_id=_PROJECT_ID,
            gcp_conn_id=_GCP_CONN_ID,
            run_id=cast(str, XComArg(gcp_bigquery_start_transfer, key="run_id")),
            expected_statuses={"SUCCEEDED"},
            map_index_template="Waiting for transfer run: {{task.run_id}}",
        )

        gcp_bigquery_delete_transfer = BigQueryDeleteDataTransferConfigOperator(
            transfer_config_id=transfer_config_id,
            gcp_conn_id=_GCP_CONN_ID,
            task_id="gcp_bigquery_delete_transfer",
        )

        @task(
            outlets=[DatasetAlias("bigquery_table_alias")],
            map_index_template="{{ my_custom_map_index }}",
        )
        def update_dataset(table):
            """
            Update the Airflow dataset of table that was created in BigQuery.
            Args:
                table (str): The table that was created in BigQuery.
            """
            table_uri = f"{_PROJECT_ID}:{_BQ_DATASET}.{table}"
            yield Metadata(
                Dataset(f"{table_uri}"),
                extra={"successful_bigquery_transfer": True},
                alias="bigquery_table_alias",
            )

            # get the current context and define the custom map index variable
            from airflow.operators.python import get_current_context

            context = get_current_context()
            context["my_custom_map_index"] = f"Updating Dataset for {table}"

        # set dependencies of tasks inside the task group
        chain(
            create_transfer_config_obj,
            gcp_bigquery_create_transfer,
            gcp_bigquery_start_transfer,
            gcp_run_sensor,
            gcp_bigquery_delete_transfer,
            update_dataset(table),
        )

    # set dependencies of tasks and task the task group
    chain(
        define_inlets(),
        create_dataset,
        create_table,
        transfer_data.expand(table=_LIST_OF_BASE_TABLES),
    )


load_to_bigquery()
