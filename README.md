Orchestrate ML in Snowpark with Apache Airflow
==============================================

This repository contains the DAG code used in the [Orchestrate ML in Snowpark with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-snowpark). 

The DAG in this repository uses the following package:

- Airflow Snowpark provider beta version.

> Note that the decorators from this beta version of the Snowpark provider will be available in the [Snowflake Airflow provider](https://registry.astronomer.io/providers/apache-airflow-providers-snowflake/versions/latest) in a future release.

# How to use this repository

This section explains how to run this repository with Airflow. Note that you will need to copy the contents of the `.env_example` file to a newly created `.env` file. No external connections are necessary to run this repository locally, but you can add your own credentials in the file if you wish to connect to your tools. 

Download the [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) to run Airflow locally in Docker. `astro` is the only package you will need to install locally.

1. Run `git clone https://github.com/astronomer/use-case-mlflow.git` on your computer to create a local clone of this repository.
2. Install the Astro CLI by following the steps in the [Astro CLI documentation](https://docs.astronomer.io/astro/cli/install-cli). Docker Desktop/Docker Engine is a prerequisite, but you don't need in-depth Docker knowledge to run Airflow with the Astro CLI.
3. Run `astro dev start` in your cloned repository.
4. After your Astro project has started. View the Airflow UI at `localhost:8080`.

In this project `astro dev start` spins up 4 Docker containers:

- The Airflow webserver, which runs the Airflow UI and can be accessed at `https://localhost:8080/`.
- The Airflow scheduler, which is responsible for monitoring and triggering tasks.
- The Airflow triggerer, which is an Airflow component used to run deferrable operators.
- The Airflow metadata database, which is a Postgres database that runs on port 5432.

## Resources

- [Orchestrate ML in Snowpark with Apache Airflow tutorial](https://docs.astronomer.io/learn/airflow-snowpark).
- [Snowpark API documentation](https://docs.snowflake.com/en/developer-guide/snowpark/python/index).
- [Snowpark ML documentation](https://docs.snowflake.com/en/developer-guide/snowpark-ml/index).
- [Airflow Snowpark ML demo](https://github.com/astronomer/airflow-snowparkml-demo) this demo shows a more complex example of how to use Snowpark ML in Airflow.