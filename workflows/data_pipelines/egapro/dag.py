from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG
from data_pipelines_annuaire.workflows.data_pipelines.egapro.processor import (
    EgaproProcessor,
)

egapro_processor = EgaproProcessor()

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["egapro"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=pendulum.today("UTC").add(days=-8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_egapro():
    @task.bash
    def clean_previous_outputs():
        return (
            f"rm -rf {EGAPRO_CONFIG.tmp_folder} && mkdir -p {EGAPRO_CONFIG.tmp_folder}"
        )

    @task
    def preprocess_egapro():
        return egapro_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return egapro_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return egapro_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return egapro_processor.compare_files_minio()

    return (
        clean_previous_outputs()
        >> preprocess_egapro()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


# Instantiate the DAG
data_processing_egapro()
