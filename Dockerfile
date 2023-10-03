# syntax=quay.io/astronomer/airflow-extensions:latest

FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.9-base

# copy the wheel file to the image
COPY include/astro_provider_snowflake-0.0.0-py3-none-any.whl /tmp

# Create the virtual environment
PYENV 3.8 snowpark requirements-snowpark.txt

# install
COPY requirements-snowpark.txt /tmp
RUN python3.8 -m pip install -r /tmp/requirements-snowpark.txt