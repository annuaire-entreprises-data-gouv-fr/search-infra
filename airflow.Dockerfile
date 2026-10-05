# This Dockerfile is used in production by l'Annuaire des Entreprises
# Any modification should be thoroughly tested
# Keep the Python and Airflow versions up to date with pyproject.toml
ARG AIRFLOW_VERSION=3.2.1
ARG AIRFLOW_PYTHON_VERSION=3.12

FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${AIRFLOW_PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG AIRFLOW_PYTHON_VERSION

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git lftp zip wget p7zip-full pigz gcc g++

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow[postgres,statsd]==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-fab" \
    -c https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${AIRFLOW_PYTHON_VERSION}.txt

COPY ./pyproject.toml ./uv.lock /opt/airflow/
RUN uv export --project /opt/airflow --locked --only-group airflow --no-hashes \
    | grep -x "apache-airflow==${AIRFLOW_VERSION}" \
    || { echo "AIRFLOW_VERSION=${AIRFLOW_VERSION} differs from the \"airflow\" group in pyproject.toml: align them, then run uv lock" >&2; exit 1; }
# Installs uv.lock into the image's environment without the "airflow" group already provided by the base image.
# --inexact keeps the packages absent from uv.lock, otherwise uv sync would uninstall them.
# --python fails the build if the image's Python is different than requires-python
RUN uv sync --project /opt/airflow --python "$VIRTUAL_ENV/bin/python" --locked --active --inexact --no-default-groups --no-install-project --no-cache \
    && uv pip check
