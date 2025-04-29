from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import ShortCircuitOperator

from dag_datalake_sirene.config import EMAIL_LIST, METADATA_CC_TMP_FOLDER
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.metadata.cc.task_functions import (
    create_metadata_convention_collective_json,
    is_metadata_not_updated,
    upload_json_to_minio,
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
    default_args=default_args,
    start_date=datetime(2023, 11, 23),
    schedule="0 11 2-31/3 * *",  # At 11:00 on every 3rd day-of-month from 2nd through 31st
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 100)),
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
    tags=["api", "metadata", "cc"],
)
def get_metadata_cc():
    @task.bash
    def clean_previous_outputs() -> str:
        return f"rm -rf {METADATA_CC_TMP_FOLDER} && mkdir -p {METADATA_CC_TMP_FOLDER}"

    is_metadata_updated_task = ShortCircuitOperator(
        task_id="is_metadata_not_updated",
        python_callable=is_metadata_not_updated,
    )

    @task()
    def create_metadata_convention_collective_json_task():
        create_metadata_convention_collective_json()

    @task()
    def upload_json_to_minio_task():
        upload_json_to_minio()

    (
        clean_previous_outputs()
        >> is_metadata_updated_task
        >> create_metadata_convention_collective_json_task()
        >> upload_json_to_minio_task()
    )


get_metadata_cc()
