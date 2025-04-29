import logging
import os
import shutil
from datetime import datetime, timedelta, timezone

from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.settings import Session

from dag_datalake_sirene.config import (
    EMAIL_LIST,
)


# Define the Python function to delete old logs and directories
def delete_old_logs_and_directories(**kwargs):
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


def delete_old_runs(**kwargs):
    """
    Query and delete runs older than the threshold date (2 weeks ago).
    """
    oldest_run_date = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(
        days=14
    )

    # Create a session to interact with the metadata database
    session = Session()

    try:
        runs_to_delete = (
            session.query(DagRun).filter(DagRun.execution_date <= oldest_run_date).all()
        )
        for run in runs_to_delete:
            logging.info(
                f"Deleting run: dag_id={run.dag_id}, "
                f"execution_date={run.execution_date}"
            )
            session.delete(run)
        session.commit()
    except Exception as e:
        logging.error(f"Error deleting old runs: {str(e)}")
        session.rollback()
    finally:
        session.close()


# Define default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": EMAIL_LIST,
    "email_on_failure": True,
}

with DAG(
    "delete_airflow_logs_and_runs",
    default_args=default_args,
    description="Delete Airflow logs and runs older than 14 days",
    schedule="0 16 * * 1",  # run every Monday at 4:00 PM (UTC)
    dagrun_timeout=timedelta(minutes=30),
    start_date=datetime(2023, 12, 28),
    catchup=False,  # False to ignore past runs
    max_active_runs=1,  # Allow only one execution at a time
) as dag:
    delete_old_logs_task = PythonOperator(
        task_id="delete_logs",
        python_callable=delete_old_logs_and_directories,
        dag=dag,
    )

    delete_old_runs_task = PythonOperator(
        task_id="delete_old_runs", python_callable=delete_old_runs, dag=dag
    )

    # Set the task dependency
    delete_old_runs_task.set_upstream(delete_old_logs_task)
