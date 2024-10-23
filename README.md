## ELT with BigQuery, dbt and Apache Airflow® - Reference architecture

Welcome! 🚀

This project is an end-to-end pipeline showing how to implement an ELT pattern with [Google BigQuery](https://cloud.google.com/bigquery), [dbt Core](https://github.com/dbt-labs/dbt-core) and [Apache Airflow®](https://airflow.apache.org/). The pipeline extracts data from a mocked internal API about an online cheese store, loads the raw data into [Google Cloud Storage S3](https://cloud.google.com/storage), transfers the data to BigQuery where a dbt Core project is used to create enriched reporting tables in several transformation steps. Finally the pipeline sends information about the top customers to a [Slack](https://slack.com/) channel.

You can use this project as a starting point to build your own pipelines for similar use cases. 

> [!TIP]
> If you are new to Airflow, we recommend checking out our get started resources: [DAG writing for data engineers and data scientists](https://www.astronomer.io/events/webinars/dag-writing-for-data-engineers-and-data-scientists-video/) before diving into this project.

## Tools used

- [Apache Airflow®](https://airflow.apache.org/docs/apache-airflow/stable/index.html) running on [Astro](https://www.astronomer.io/product/). A [free trial](http://qrco.de/bfHv2Q) is available.
- [Google Cloud Storage](https://cloud.google.com/storage) for storing raw data.
- [Google BigQuery](https://cloud.google.com/bigquery) for storing and querying transformed data.
- [dbt Core](https://github.com/dbt-labs/dbt-core) to define transformations.

Optional:

The last DAG posts a notification to Slack about the top cheese enthusiasts. If you don't want to use Slack, remove the `top_users_report` DAG from the `dags` folder.

- A [Slack](https://slack.com/) workspace with permissions to add a new app is needed for the Slack notification tasks.

## How to setup the demo environment

Follow the steps below to set up the demo for yourself.

1. Install Astronomer's open-source local Airflow development tool, the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview).
2. Log into your Google Cloud Platform account and create a set of [credentials](https://cloud.google.com/iam/docs/authentication) for Airflow with permissions to create, write to and read from a GCS bucket, as well as to create, write to and query BigQuery tables and use the [BigQuery Data Transfer Service](https://cloud.google.com/bigquery/docs/dts-introduction).
3. Fork this repository and clone the code locally.
4. (Optional) Create a new Slack app and install it in your workspace. You can follow the instructions in the [Slack API documentation](https://api.slack.com/start) to retrieve an API token for the app.

### Run the project locally

1. Create a new file called `.env` in the root of the cloned repository and copy the contents of [.env_example](.env_example) into it. Fill out the placeholders with your own credentials for GCP, and optionally Slack.

It is also possible to set the connections in the Airflow UI. For GCP select the `google_cloud_platform` connection type, provide your `Project Id` and paste your JSON credentials into the `Keyfile JSON` field. For Slack, select the `slack` connection type and paste your Slack API token into the `Password` field.

Note that in a production use case you may want to enable the [Custom XCom backend](https://www.astronomer.io/docs/learn/xcom-backend-tutorial) using the commented environment variables in the `.env` file. 

2. In the root of the repository, run `astro dev start` to start up the following Docker containers. This is your local development environment.

    - Postgres: Airflow's Metadata Database.
    - Webserver: The Airflow component responsible for rendering the Airflow UI. Accessible on port `localhost:8080`.
    - Scheduler: The Airflow component responsible for monitoring and triggering tasks
    - Triggerer: The Airflow component responsible for triggering deferred tasks

    Note that after any changes to `.env` you will need to run `astro dev restart` for new environment variables to be picked up.

3. Access the Airflow UI at `localhost:8080` and follow the DAG running instructions in the [Running the DAGs](#running-the-dags) section of this README.

### Run the project in the cloud

1. Sign up to [Astro](https://www.astronomer.io/try-astro/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=free-trial) for free and follow the onboarding flow to create a deployment with default configurations.
2. Deploy the project to Astro using `astro deploy`. See [Deploy code to Astro](https://www.astronomer.io/docs/astro/deploy-code).
3. Set up your GCP and, optionally, Slack connection, as well as all other environment variables listed in [`.env_example](.env_example) on Astro. For instructions see [Manage Airflow connections and variables](https://www.astronomer.io/docs/astro/manage-connections-variables) and [Manage environment variables on Astro](https://www.astronomer.io/docs/astro/manage-env-vars).
4. Open the Airflow UI of your Astro deployment and follow the steps in [Running the DAGs](#running-the-dags).

## Running the DAGs

1. Unpause all DAGs in the Airflow UI by clicking the toggle to the left of the DAG name.
2. The `Extract data from the internal API and load it to GCS` DAG will start its first run automatically. All other DAGs are scheduled based on Datasets to run as soon as the required data is available.

## Next steps

If you'd like to build your own ELT pipeline with BigQuery and dbt, feel free adapt this repository to your use case. We recommend to deploy the Airflow pipelines using a [free trial](https://www.astronomer.io/try-astro/?utm_source=learn-docs-reference-architectures&utm_medium=web&utm_campaign=free-trial) of Astro.