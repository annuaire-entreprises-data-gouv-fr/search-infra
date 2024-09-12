from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_HOME,
    EMAIL_LIST,
    RNE_DAG_FOLDER,
    RNE_FTP_URL,
    RNE_STOCK_TMP_FOLDER,
)
from dag_datalake_sirene.workflows.data_pipelines.rne.stock.task_functions import (
    unzip_files_and_upload_minio,
    send_notification_failure_tchap,
    send_notification_success_tchap,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="get_stock_rne",
    default_args=default_args,
    start_date=datetime(2023, 10, 5),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 18)),
    on_failure_callback=send_notification_failure_tchap,
    tags=["download", "rne", "stock"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {RNE_STOCK_TMP_FOLDER} && mkdir -p {RNE_STOCK_TMP_FOLDER}"
        ),
    )

    get_rne_latest_stock = BashOperator(
        task_id="get_latest_stock",
        bash_command=(
            f"{AIRFLOW_DAG_HOME}{RNE_DAG_FOLDER}rne/stock/get_stock.sh "
            f"{RNE_STOCK_TMP_FOLDER} {RNE_FTP_URL} "
        ),
    )

    unzip_files_and_upload_minio = PythonOperator(
        task_id="unzip_files_and_upload_minio",
        python_callable=unzip_files_and_upload_minio,
    )

    clean_outputs = BashOperator(
        task_id="clean_outputs",
        bash_command=f"rm -rf {RNE_STOCK_TMP_FOLDER}",
    )

    send_notification_tchap = PythonOperator(
        task_id="send_notification_tchap",
        python_callable=send_notification_success_tchap,
    )

    get_rne_latest_stock.set_upstream(clean_previous_outputs)
    unzip_files_and_upload_minio.set_upstream(get_rne_latest_stock)
    clean_outputs.set_upstream(unzip_files_and_upload_minio)
    send_notification_tchap.set_upstream(clean_outputs)
