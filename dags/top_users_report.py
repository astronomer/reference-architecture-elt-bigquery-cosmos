from airflow.decorators import dag, task
from pendulum import datetime
from airflow.datasets import Dataset
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
)
from airflow.models.baseoperator import chain
import os

# GCP
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "gcp_default")
_PROJECT_ID = os.getenv("PROJECT_ID", "my-project")
_BQ_DATASET = os.getenv("BQ_DATASET", "cheese_store")


@dag(
    dag_display_name="🧀 Top users by spending report",
    start_date=datetime(2024, 10, 1),
    schedule=[Dataset(f"bigquery/{_PROJECT_ID}.{_BQ_DATASET}.top_users_by_spending")],
    catchup=False,
)
def top_users_report():

    @task(
        inlets=[Dataset(f"bigquery/{_PROJECT_ID}.{_BQ_DATASET}.top_users_by_spending")]
    )
    def define_inlets():
        return "Inlets defined"

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
        Send the executive summary to a Slack channel.
        Args:
            insight (str): Executive summary.
        """
        from airflow.providers.slack.hooks.slack import SlackHook

        # format the message top_cheese_eaters is a list of lists with the [1] element being the name
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
