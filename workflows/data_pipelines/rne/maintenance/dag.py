from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@task
def rename_old_rne_folders():
    ObjectStorageClient().rename_folder("rne/flux/data-2023", "rne/flux/data")


@dag(
    tags=["maintenance", "rename", "rne", "files"],
    description="Delete old object storage files",
    default_args=default_args,
    schedule="0 12 * * *",  # run every day at 12:00 PM (UTC)
    start_date=datetime(2026, 1, 1),
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    catchup=False,
    on_failure_callback=[Notification(), SmtpNotifier(to=EMAIL_LIST)],
    on_success_callback=Notification(),
    max_active_runs=1,  # Allow only one execution at a time
)
def rename_rne_folders():
    return rename_old_rne_folders()


rename_rne_folders()
