FROM apache/airflow:2.9.1

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt-snowflake and project dependencies
RUN pip install --no-cache-dir \
    dbt-snowflake==1.8.0 \
    snowflake-connector-python[pandas] \
    boto3 \
    pyarrow \
    python-dotenv