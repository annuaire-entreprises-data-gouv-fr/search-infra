from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.bilan_ges.config import (
    BILAN_GES_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.bilan_ges.processor import (
    BilanGesProcessor,
)

bilan_ges_processor = BilanGesProcessor()

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["bilan_ges"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_bilan_ges():
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {BILAN_GES_CONFIG.tmp_folder} && mkdir -p {BILAN_GES_CONFIG.tmp_folder}"

    @task
    def preprocess_bilan_ges():
        return bilan_ges_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return bilan_ges_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return bilan_ges_processor.send_file_to_minio()

    @task
    def compare_files_minio():
        return bilan_ges_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> preprocess_bilan_ges()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


# Instantiate the DAG
data_processing_bilan_ges()
