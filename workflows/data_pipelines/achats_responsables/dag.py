from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.achats_responsables.processor import (
    AchatsResponsablesProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["achats_responsables", "label"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_achats_responsables():
    achats_responsables_processor = AchatsResponsablesProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {achats_responsables_processor.config.tmp_folder} && mkdir -p {achats_responsables_processor.config.tmp_folder}"

    @task
    def download_data():
        return achats_responsables_processor.download_data()

    @task
    def preprocess_data():
        return achats_responsables_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return achats_responsables_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return achats_responsables_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return achats_responsables_processor.compare_files_minio()

    @task.bash
    def clean_up():
        return f"rm -rf {achats_responsables_processor.config.tmp_folder}"

    (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
        >> clean_up()
    )


data_processing_achats_responsables()
