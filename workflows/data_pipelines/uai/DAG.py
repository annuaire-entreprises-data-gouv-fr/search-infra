from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from config import (
    UAI_TMP_FOLDER,
)
from workflows.data_pipelines.uai.task_functions import (
    download_latest_data,
    process_uai,
    send_file_to_minio,
    compare_files_minio,
    send_notification,
)

with DAG(
    dag_id="data_processing_uai",
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=15),
    tags=["uai", "scolaire"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {UAI_TMP_FOLDER} && mkdir -p {UAI_TMP_FOLDER}",
    )

    download_latest_data = PythonOperator(
        task_id="download_latest_data", python_callable=download_latest_data
    )

    process_uai = PythonOperator(task_id="process_uai", python_callable=process_uai)

    send_file_to_minio = PythonOperator(
        task_id="send_file_to_minio", python_callable=send_file_to_minio
    )

    compare_files_minio = ShortCircuitOperator(
        task_id="compare_files_minio", python_callable=compare_files_minio
    )

    send_notification = PythonOperator(
        task_id="send_notification", python_callable=send_notification
    )

    download_latest_data.set_upstream(clean_previous_outputs)
    process_uai.set_upstream(download_latest_data)
    send_file_to_minio.set_upstream(process_uai)
    compare_files_minio.set_upstream(send_file_to_minio)
    send_notification.set_upstream(compare_files_minio)
