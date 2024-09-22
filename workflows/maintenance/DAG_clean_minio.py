import logging
from airflow.operators.python import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta, timezone
from helpers.minio_helpers import minio_client
from helpers.settings import Settings


def delete_old_files(
    prefix,
    keep_latest: int = 2,
    retention_days: int = 14,
):
    """
    Delete old files from MinIO, keeping the specified number of latest files.

    Args:
        prefix (str): Prefix of the files to delete.
        keep_latest (int, optional): Number of latest files to retain. Defaults to 2.
        retention_days (int, optional): Number of days to retain files. Defaults to 14.
    """
    file_info_list = minio_client.get_files_and_last_modified(prefix)

    file_info_list.sort(key=lambda x: x[1], reverse=True)

    for i, (file_name, last_modified) in enumerate(file_info_list):
        # Ensure both datetime objects are offset-aware
        last_modified = last_modified.replace(tzinfo=timezone.utc)
        current_time = datetime.utcnow().replace(tzinfo=timezone.utc)

        age = current_time - last_modified

        if i < keep_latest or age < timedelta(days=retention_days):
            continue
        logging.info(f"***** Deleting file: {file_name}")
        minio_client.delete_file(file_name)


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": Settings.EMAIL_LIST,
    "email_on_failure": True,
}

# This DAG delete outdated RNE and SIRENE databases from MinIO if they are older than
# 3 days, while retaining a specified number of the most recent files.
with DAG(
    "delete_old_minio_file",
    default_args=default_args,
    description="Delete old MinIO files",
    schedule_interval="0 12 * * *",  # run every day at 12:00 PM (UTC)
    dagrun_timeout=timedelta(minutes=30),
    start_date=datetime(2023, 12, 28),
    catchup=False,  # False to ignore past runs
    max_active_runs=1,  # Allow only one execution at a time
) as dag:
    delete_old_rne_databases = PythonOperator(
        task_id="delete_old_rne_databases",
        python_callable=delete_old_files,
        provide_context=True,
        op_kwargs={
            "prefix": f"ae/{Settings.AIRFLOW_ENV}/rne/database/",
            "keep_latest": 5,
            "retention_days": 3,
        },
        dag=dag,
    )

    delete_old_sirene_databases = PythonOperator(
        task_id="delete_old_sirene_databases",
        python_callable=delete_old_files,
        provide_context=True,
        op_kwargs={
            "prefix": f"ae/{Settings.AIRFLOW_ENV}/sirene/database/",
            "keep_latest": 2,
            "retention_days": 3,
        },
        dag=dag,
    )

    delete_old_sirene_databases.set_upstream(delete_old_rne_databases)
