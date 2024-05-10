from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datalake_sirene.config import (
    INSEE_TMP_FOLDER,
    EMAIL_LIST,
)
from dag_datalake_sirene.workflows.data_pipelines.sirene.etab.task_functions import (
    download_historique_etab,
    download_stock_etab,
    send_historique_file_to_minio,
    send_stock_file_to_minio,
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
    dag_id="data_processing_sirene_stock_etab",
    default_args=default_args,
    schedule_interval="0 4 * * MON",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    tags=["data-processing", "sirene", "backup", "historique", "etablissement"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {INSEE_TMP_FOLDER}etab/ && mkdir -p {INSEE_TMP_FOLDER}etab/"
        ),
    )

    download_historique_etab = PythonOperator(
        task_id="download_historique_etab", python_callable=download_historique_etab
    )

    download_stock_etab = PythonOperator(
        task_id="download_stock_etab", python_callable=download_stock_etab
    )

    send_historique_file_to_minio = ShortCircuitOperator(
        task_id="send_historique_file_to_minio",
        python_callable=send_historique_file_to_minio,
    )

    send_stock_file_to_minio = PythonOperator(
        task_id="send_stock_file_to_minio", python_callable=send_stock_file_to_minio
    )

    send_notification = PythonOperator(
        task_id="send_notification", python_callable=send_notification
    )

    download_historique_etab.set_upstream(clean_previous_outputs)
    send_historique_file_to_minio.set_upstream(download_historique_etab)
    download_stock_etab.set_upstream(clean_previous_outputs)
    send_stock_file_to_minio.set_upstream(download_stock_etab)
    send_notification.set_upstream(send_stock_file_to_minio)
    send_notification.set_upstream(send_historique_file_to_minio)
