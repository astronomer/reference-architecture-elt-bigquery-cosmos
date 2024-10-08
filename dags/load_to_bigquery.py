import os

from airflow.decorators import dag, task_group, task
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)
from airflow.providers.google.cloud.sensors.bigquery_dts import (
    BigQueryDataTransferServiceTransferRunSensor,
)
from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from typing import cast
from pendulum import datetime
from airflow.models.xcom_arg import XComArg
import time

# GCP
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_default")
_GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "my-bucket")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "cheese-sales-ingest")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
_BQ_DATASET = os.getenv("BQ_DATASET", "cheese_store")

_LIST_OF_BASE_TABLES = ["users", "cheeses", "sales", "utms"]


@dag(
    dag_display_name="🐘 Load data from GCS to BigQuery",
    start_date=datetime(2024, 10, 1),
    schedule=[Dataset(f"gs://{_GCS_BUCKET_NAME}/{_INGEST_FOLDER_NAME}/*")],
    catchup=False,
    tags=["ELT"],
)
def load_to_bigquery():

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", gcp_conn_id=_GCP_CONN_ID, dataset_id=_BQ_DATASET
    )

    @task
    def create_table_creation_configs():
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

    @task_group
    def transfer_data(table):

        @task
        def create_transfer_config(table, **context):

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

        chain(
            create_transfer_config_obj,
            gcp_bigquery_create_transfer,
            gcp_bigquery_start_transfer,
            gcp_run_sensor,
            gcp_bigquery_delete_transfer,
            update_dataset(table),
        )

    chain(
        create_dataset,
        create_table,
        transfer_data.expand(table=_LIST_OF_BASE_TABLES),
    )


load_to_bigquery()
