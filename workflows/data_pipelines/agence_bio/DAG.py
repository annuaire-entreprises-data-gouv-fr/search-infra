from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datalake_sirene.config import (
    AGENCE_BIO_TMP_FOLDER,
    EMAIL_LIST,
)
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.task_functions import (
    process_agence_bio,
    send_file_to_minio,
    compare_files_minio,
    send_notification,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}

with DAG(
    dag_id="data_processing_agence_bio",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    tags=["agence bio", "certifications"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {AGENCE_BIO_TMP_FOLDER} && mkdir -p {AGENCE_BIO_TMP_FOLDER}"
        ),
    )

    process_agence_bio = PythonOperator(
        task_id="process_agence_bio", python_callable=process_agence_bio
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

    process_agence_bio.set_upstream(clean_previous_outputs)
    send_file_to_minio.set_upstream(process_agence_bio)
    compare_files_minio.set_upstream(send_file_to_minio)
    send_notification.set_upstream(compare_files_minio)
