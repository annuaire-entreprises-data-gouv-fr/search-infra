from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.config import EMAIL_LIST, METADATA_CC_TMP_FOLDER
from dag_datalake_sirene.data_pipelines.metadata.cc.task_functions import (
    create_metadata_concollective_json,
    upload_json_file_to_minio,
    send_notification_failure_tchap,
    send_notification_success_tchap,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="get_metadata_cc",
    default_args=default_args,
    start_date=datetime(2023, 11, 23),
    schedule_interval="0 11 * * *",  # Run every day at 11 am
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 100)),
    on_failure_callback=send_notification_failure_tchap,
    tags=["api", "metadata", "cc"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {METADATA_CC_TMP_FOLDER} "
        f"&& mkdir -p {METADATA_CC_TMP_FOLDER}",
    )

    create_metadata_concollective_json = PythonOperator(
        task_id="create_metadata_cc_json",
        python_callable=create_metadata_concollective_json,
    )

    upload_json_to_minio = PythonOperator(
        task_id="upload_json_to_minio", python_callable=upload_json_file_to_minio
    )

    send_notification_success_tchap = PythonOperator(
        task_id="send_notification_success_tchap",
        python_callable=send_notification_success_tchap,
    )

    create_metadata_concollective_json.set_upstream(clean_previous_outputs)
    upload_json_to_minio.set_upstream(create_metadata_concollective_json)
    send_notification_success_tchap.set_upstream(upload_json_to_minio)
