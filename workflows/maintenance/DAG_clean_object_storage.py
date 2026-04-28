import logging
import re
from collections import defaultdict
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
    """Delete old files from object storage, keeping the latest N files per filename series.

    Files are grouped by their name with the date part ignored (YYYY, YYYY-MM or
    YYYY-MM-DD) so that each distinct file type is managed separately. For example,
    flux_unite_legale_2026-04.csv.gz and flux_unite_legale_2026-03.csv.gz belong
    to the same file group and only the most recent `keep_latest` files in that group
    are retained.
    The `retention_days` protects any file that was uploaded recently,
    even if it means ending up with more than `keep_latest` files.

    Args:
        prefix (str): Prefix of the files to delete.
        keep_latest (int): Number of most-recent files to keep. Defaults to 2.
        retention_days (int): Number of days to retain files whatever the `keep_latest` value is. Defaults to 14.
    """
    object_storage_client = ObjectStorageClient()
    file_info_list = object_storage_client.get_files_and_last_modified(prefix)

    groups: dict[str, list] = defaultdict(list)
    for file_name, last_modified in file_info_list:
        base_name = re.sub(r"\d{4}(-\d{2}(-\d{2})?)?", "YYYY-MM-DD", file_name)
        groups[base_name].append((file_name, last_modified))

    current_time = datetime.now(timezone.utc)
    for files in groups.values():
        files.sort(key=lambda x: x[1], reverse=True)
        for i, (file_name, last_modified) in enumerate(files):
            # Ensure both datetime objects are offset-aware
            last_modified = last_modified.replace(tzinfo=timezone.utc)
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
    start_date=datetime(2026, 1, 1),
    dagrun_timeout=timedelta(minutes=30),
    catchup=False,
    max_active_runs=1,  # Allow only one execution at a time
)
def delete_old_object_storage_file():
    delete_old_files.override(task_id="rne_database")(
        prefix=f"ae/{AIRFLOW_ENV}/rne/database/",
        keep_latest=4,
        retention_days=3,
    )

    delete_old_files.override(task_id="sirene_database")(
        prefix=f"ae/{AIRFLOW_ENV}/sirene/database/",
        keep_latest=3,
        retention_days=3,
    )

    delete_old_files.override(task_id="sirene_flux")(
        prefix=f"ae/{AIRFLOW_ENV}/insee/flux/",
        keep_latest=3,
        retention_days=30,
    )

    delete_old_files.override(task_id="sirene_stock")(
        prefix=f"ae/{AIRFLOW_ENV}/insee/sirene_stock/",
        keep_latest=3,
        retention_days=30,
    )


delete_old_object_storage_file()
