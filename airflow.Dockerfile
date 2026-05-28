ARG AIRFLOW_VERSION=3.2.1
ARG AIRFLOW_PYTHON_VERSION=3.12

FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON_VERSION

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git lftp zip wget p7zip-full gcc g++

USER airflow

ENV PATH="/home/airflow/.local/bin:$PATH"
RUN pip install --no-cache-dir --user \
    "apache-airflow[postgres,statsd]==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-fab" \
    "sentry-sdk" \
    -c "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt"

COPY ./requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir --user -r /opt/airflow/requirements.txt
