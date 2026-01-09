import logging
import os
import shutil
from datetime import datetime, timedelta

import pendulum
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import (
    EMAIL_LIST,
)


# Define the Python function to delete old logs and directories
@task
def delete_logs(**kwargs):
    log_dir = "/opt/airflow/logs"
    cutoff_date = datetime.now() - timedelta(days=14)

    # Check if the directory exists
    if os.path.exists(log_dir):
        for root, dirs, files in os.walk(log_dir, topdown=False):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                # Check if the directory modification time is older than the cutoff date
                if os.path.getmtime(dir_path) < cutoff_date.timestamp():
                    # Recursively remove directory and its contents
                    shutil.rmtree(dir_path)
                    logging.info(f"Deleted directory: {dir_path}")
            for file in files:
                file_path = os.path.join(root, file)
                # Check if the file modification time is older than the cutoff date
                if os.path.getmtime(file_path) < cutoff_date.timestamp():
                    os.remove(file_path)
                    logging.info(f"Deleted file: {file_path}")
    else:
        logging.error(f"Log directory not found: {log_dir}")


# Define default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": EMAIL_LIST,
    "email_on_failure": True,
}


@dag(
    tags=["maintenance"],
    default_args=default_args,
    description="Delete Airflow logs and runs older than 14 days",
    schedule="0 16 * * 1",
    start_date=pendulum.today("UTC").add(days=-8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    max_active_runs=1,
)
def delete_airflow_logs_and_runs():
    return delete_logs()


delete_airflow_logs_and_runs()
