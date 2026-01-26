import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task

from data_pipelines_annuaire.config import (
    AIRFLOW_ENV,
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient


@task
def delete_old_files(
    prefix,
    keep_latest: int = 2,
    retention_days: int = 14,
):
    """
    Delete old files from the object storage, keeping the specified number of latest files.

    Args:
        prefix (str): Prefix of the files to delete.
        keep_latest (int, optional): Number of latest files to retain. Defaults to 2.
        retention_days (int, optional): Number of days to retain files. Defaults to 14.
    """
    object_storage_client = ObjectStorageClient()
    file_info_list = object_storage_client.get_files_and_last_modified(prefix)

    file_info_list.sort(key=lambda x: x[1], reverse=True)

    for i, (file_name, last_modified) in enumerate(file_info_list):
        # Ensure both datetime objects are offset-aware
        last_modified = last_modified.replace(tzinfo=timezone.utc)
        current_time = datetime.now(timezone.utc)

        age = current_time - last_modified

        if i < keep_latest or age < timedelta(days=retention_days):
            continue
        logging.info(f"***** Deleting file: {file_name}")
        object_storage_client.delete_file(file_name)


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email": EMAIL_LIST,
    "email_on_failure": True,
}


# This DAG delete outdated RNE and SIRENE databases from object storage if they are older than
# 3 days, while retaining a specified number of the most recent files.
@dag(
    tags=["maintenance", "flush cache and execute queries"],
    description="Delete old object storage files",
    default_args=default_args,
    schedule="0 12 * * *",  # run every day at 12:00 PM (UTC)
    start_date=datetime(2023, 12, 28),
    dagrun_timeout=timedelta(minutes=30),
    catchup=False,
    max_active_runs=1,  # Allow only one execution at a time
)
def delete_old_object_storage_file():
    rne = delete_old_files(
        prefix=f"ae/{AIRFLOW_ENV}/rne/database/",
        keep_latest=5,
        retention_days=3,
    )

    sirene = delete_old_files(
        prefix=f"ae/{AIRFLOW_ENV}/sirene/database/",
        keep_latest=2,
        retention_days=3,
    )

    return rne >> sirene


delete_old_object_storage_file()
