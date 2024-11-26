from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datalake_sirene.config import (
    EGAPRO_TMP_FOLDER,
    EMAIL_LIST,
)
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.egapro.egapro_processor import (
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
    dag_id="data_processing_egapro",
    tags=["egapro"],
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
)
def data_processing_egapro_dag():
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {EGAPRO_TMP_FOLDER} && mkdir -p {EGAPRO_TMP_FOLDER}"

    @task
    def process_egapro():
        return egapro_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return egapro_processor.save_date_last_modified()

    @task
    def send_file_to_minio():
        return egapro_processor.send_file_to_minio()

    @task.short_circuit
    def compare_files_minio():
        return egapro_processor.compare_files_minio()

    (
        clean_previous_outputs()
        >> process_egapro()
        >> save_date_last_modified()
        >> send_file_to_minio()
        >> compare_files_minio()
    )


# Instantiate the DAG
data_processing_egapro_dag()
