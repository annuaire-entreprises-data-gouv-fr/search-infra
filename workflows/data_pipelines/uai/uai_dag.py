from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.uai.uai_config import (
    UAI_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.uai.uai_processor import (
    UAIProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["uai", "scolaire"],
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=15),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
)
def data_processing_uai():
    uai_processor = UAIProcessor()

    @task.bash
    def uai_clean_previous_outputs():
        return f"rm -rf {UAI_CONFIG.tmp_folder} && mkdir -p {UAI_CONFIG.tmp_folder}"

    @task
    def uai_download_data():
        return uai_processor.download_data()

    @task
    def uai_preprocess_data():
        return uai_processor.preprocess_data()

    @task
    def uai_save_date_last_modified():
        return uai_processor.save_date_last_modified()

    @task
    def uai_send_file_to_minio():
        return uai_processor.send_file_to_minio()

    @task
    def uai_compare_files_minio():
        return uai_processor.compare_files_minio()

    (
        uai_clean_previous_outputs()
        >> uai_download_data()
        >> uai_preprocess_data()
        >> uai_save_date_last_modified()
        >> uai_send_file_to_minio()
        >> uai_compare_files_minio()
    )


data_processing_uai()
