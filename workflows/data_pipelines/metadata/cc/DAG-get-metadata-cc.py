from datetime import datetime, timedelta

from airflow.decorators import dag, task

from dag_datalake_sirene.config import EMAIL_LIST, METADATA_CC_TMP_FOLDER
from dag_datalake_sirene.workflows.data_pipelines.metadata.cc.task_functions import (
    create_metadata_concollective_json,
    upload_json_to_minio,
)
from dag_datalake_sirene.helpers import Notification


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="get_metadata_cc",
    default_args=default_args,
    start_date=datetime(2023, 11, 23),
    schedule_interval="0 11 2 * *",  # Run every 2nd day of the month at 11 am
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 100)),
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
    tags=["api", "metadata", "cc"],
)
def get_metadata_cc():
    @task.bash
    def clean_previous_outputs() -> str:
        return f"rm -rf {METADATA_CC_TMP_FOLDER} && mkdir -p {METADATA_CC_TMP_FOLDER}"

    (
        clean_previous_outputs()
        >> create_metadata_concollective_json()
        >> upload_json_to_minio()
    )


get_metadata_cc()
