from datetime import timedelta

from airflow.decorators import dag, task
import pendulum

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.bilans_financiers.processor import (
    BilansFinanciersProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["bilans financiers", "signaux faibles"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=pendulum.today('UTC').add(days=-8),
    dagrun_timeout=timedelta(minutes=60 * 2),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_bilans_financiers():
    bilans_financiers_processor = BilansFinanciersProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {bilans_financiers_processor.config.tmp_folder} && mkdir -p {bilans_financiers_processor.config.tmp_folder}"

    @task
    def download_data():
        return bilans_financiers_processor.download_data()

    @task
    def preprocess_data():
        return bilans_financiers_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return bilans_financiers_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return bilans_financiers_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return bilans_financiers_processor.compare_files_object_storage()

    (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
    )


data_processing_bilans_financiers()
