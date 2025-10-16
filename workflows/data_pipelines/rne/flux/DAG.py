from datetime import datetime, timedelta

from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST, RNE_FLUX_TMP_FOLDER
from data_pipelines_annuaire.workflows.data_pipelines.rne.flux.flux_tasks import (
    get_every_day_flux,
    send_notification_failure_mattermost,
    send_notification_success_mattermost,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    tags=["rne", "flux"],
    default_args=default_args,
    schedule="0 1 * * *",  # Run every day at 1 AM
    start_date=datetime(2023, 10, 18),
    max_active_runs=1,
    dagrun_timeout=timedelta(days=30),
    params={},
    catchup=False,
    on_failure_callback=send_notification_failure_mattermost,
)
def get_flux_rne():
    @task.bash
    def clean_outputs():
        return (f"rm -rf {RNE_FLUX_TMP_FOLDER} && mkdir -p {RNE_FLUX_TMP_FOLDER}",)

    return (
        clean_outputs()
        >> get_every_day_flux()
        >> clean_outputs()
        >> send_notification_success_mattermost()
    )


get_flux_rne()
