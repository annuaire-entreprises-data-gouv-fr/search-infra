from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.alim_confiance.processor import (
    AlimConfianceProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["alim_confiance", "label"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=pendulum.today("UTC").add(days=-8),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_alim_confiance():
    alim_confiance_processor = AlimConfianceProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {alim_confiance_processor.config.tmp_folder} && mkdir -p {alim_confiance_processor.config.tmp_folder}"

    @task
    def download_data():
        return alim_confiance_processor.download_data()

    @task
    def preprocess_data():
        return alim_confiance_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return alim_confiance_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return alim_confiance_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return alim_confiance_processor.compare_files_object_storage()

    @task.bash
    def clean_up():
        return f"rm -rf {alim_confiance_processor.config.tmp_folder}"

    return (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
        >> clean_up()
    )


data_processing_alim_confiance()
