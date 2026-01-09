from datetime import datetime, timedelta

from airflow.sdk import dag, task

from data_pipelines_annuaire.config import (
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers.minio_helpers import MinIOClient

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task
def rename_old_rne_folders():
    MinIOClient().rename_folder("rne/flux/data-2023", "rne/flux/data")


@dag(
    tags=["maintenance", "rename", "rne", "files"],
    description="Delete old MinIO files",
    default_args=default_args,
    schedule="0 12 * * *",  # run every day at 12:00 PM (UTC)
    start_date=datetime(2023, 10, 5),
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    catchup=False,
    max_active_runs=1,  # Allow only one execution at a time
)
def rename_rne_folders():
    return rename_old_rne_folders()


rename_rne_folders()
