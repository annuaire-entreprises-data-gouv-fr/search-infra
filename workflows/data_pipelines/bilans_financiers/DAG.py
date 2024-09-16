from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from config import (
    BILANS_FINANCIERS_TMP_FOLDER,
    EMAIL_LIST,
)
# fmt: off
from workflows.data_pipelines.bilans_financiers.task_functions \
    import (
    download_bilans_financiers,
    process_bilans_financiers,
    send_file_to_minio,
    compare_files_minio,
    send_notification,
)
# fmt: on

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


with DAG(
    dag_id="data_processing_bilans_financiers",
    default_args=default_args,
    schedule_interval="0 16 * * *",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    tags=["bilans financiers", "entreprises", "signaux faibles"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {BILANS_FINANCIERS_TMP_FOLDER} &&"
            f"mkdir -p {BILANS_FINANCIERS_TMP_FOLDER}"
        ),
    )

    download_bilans_financiers = PythonOperator(
        task_id="download_bilans_financiers", python_callable=download_bilans_financiers
    )

    process_bilans_financiers = PythonOperator(
        task_id="process_bilans_financiers", python_callable=process_bilans_financiers
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
    download_bilans_financiers.set_upstream(clean_previous_outputs)
    process_bilans_financiers.set_upstream(download_bilans_financiers)
    send_file_to_minio.set_upstream(process_bilans_financiers)
    compare_files_minio.set_upstream(send_file_to_minio)
    send_notification.set_upstream(compare_files_minio)
