"""
## Create reports for the top cheese enthusiasts

This DAG creates a report of the top users by spending in the cheese store. 
The report is then sent to a Slack channel.
"""

import os

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator
from pendulum import datetime, duration

# GCP variables
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_conn")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
_BQ_DATASET = os.getenv("BQ_DATASET", "cheese_store")

# -------------- #
# DAG definition #
# -------------- #


@dag(
    dag_display_name="🧀 Top users by spending report",  # The name of the DAG displayed in the Airflow UI
    start_date=datetime(2024, 10, 18),  # date after which the DAG can be scheduled
    schedule=[
        Dataset(f"bigquery/{_PROJECT_ID}.{_BQ_DATASET}.top_users_by_spending")
    ],  # this DAG uses a Dataset schedule, see https://www.astronomer.io/docs/learn/airflow-datasets
    catchup=False,  # see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_consecutive_failed_dag_runs=10,  # auto-pauses the DAG after 10 consecutive failed runs, experimental
    default_args={
        "owner": "Product team",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(minutes=1),  # tasks wait 1 minute in between retries
    },
    doc_md=__doc__,  # add DAG Docs in the UI, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    description="Reporting",  # description next to the DAG name in the UI
    tags=["Reporting", "Slack", "BigQuery"],  # add tags in the UI
)
def top_users_report():

    @task(
        inlets=[Dataset(f"bigquery/{_PROJECT_ID}.{_BQ_DATASET}.top_users_by_spending")]
    )
    def define_inlets():
        return "Inlets defined"

    # Query the top users by spending in the cheese store
    query_data = BigQueryGetDataOperator(
        task_id="query_data",
        gcp_conn_id=_GCP_CONN_ID,
        dataset_id=_BQ_DATASET,
        table_id="top_users_by_spending",
        max_results=10,
    )

    @task
    def post_top_cheese_enthusiasts_to_slack(top_cheese_eaters: str) -> None:
        """
        Send info about top cheese enthusiasts to a Slack channel.
        Args:
            top_cheese_eaters (str): The top cheese eaters in the store.
        """
        from airflow.providers.slack.hooks.slack import SlackHook

        top_cheese_eaters = "\n".join(
            [
                f"{i+1}. {info[1]} spent ${round(float(info[2]),2)} on cheese!"
                for i, info in enumerate(top_cheese_eaters)
            ]
        )

        SLACK_MESSAGE = (
            f"------------------------------------------------\n\n"
            f"Hey there, I have the names of our top customers for you. 🧀\n"
            f"Here they are:\n\n"
            f"{top_cheese_eaters}\n\n"
            f"------------------------------------------------\n\n"
        )
        SLACK_CONNECTION_ID = "slack_conn"
        SLACK_CHANNEL = "alerts"

        hook = SlackHook(slack_conn_id=SLACK_CONNECTION_ID)

        hook.call(
            api_method="chat.postMessage",
            json={
                "channel": SLACK_CHANNEL,
                "text": SLACK_MESSAGE,
            },
        )

    chain(define_inlets(), post_top_cheese_enthusiasts_to_slack(query_data.output))


top_users_report()
