from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datalake_sirene.config import (
    EGAPRO_TMP_FOLDER,
    EMAIL_LIST,
)
from dag_datalake_sirene.workflows.data_pipelines.egapro.task_functions import (
    preprocess_egapro_data,
    send_file_to_minio,
    compare_files_minio,
    send_notification,
    save_date_last_modified,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}

with DAG(
    dag_id="data_processing_egapro",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    tags=["egapro"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(f"rm -rf {EGAPRO_TMP_FOLDER} && mkdir -p {EGAPRO_TMP_FOLDER}"),
    )

    process_egapro = PythonOperator(
        task_id="process_egapro", python_callable=preprocess_egapro_data
    )
    save_date_last_modified = PythonOperator(
        task_id="save_date_last_modified", python_callable=save_date_last_modified
    )

    send_file_to_minio = PythonOperator(
        task_id="send_file_to_minio", python_callable=send_file_to_minio
    )

    compare_files_minio = ShortCircuitOperator(
        task_id="compare_files_minio", python_callable=compare_files_minio
    )

    send_notification = PythonOperator(
        task_id="send_notification", python_callable=send_notification
    )

    process_egapro.set_upstream(clean_previous_outputs)
    save_date_last_modified.set_upstream(process_egapro)
    send_file_to_minio.set_upstream(save_date_last_modified)
    compare_files_minio.set_upstream(send_file_to_minio)
    send_notification.set_upstream(compare_files_minio)
