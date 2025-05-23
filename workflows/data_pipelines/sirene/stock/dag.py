from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.sirene.stock.processor import (
    SireneStockProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["sirene", "stock", "historique", "unité légale", "établissement"],
    default_args=default_args,
    schedule="0 0 * * *",  # Run everyday
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_sirene_stock():
    sirene_stock_processor = SireneStockProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {sirene_stock_processor.config.tmp_folder} && mkdir -p {sirene_stock_processor.config.tmp_folder}"

    @task()
    def download_stock():
        return sirene_stock_processor.download_data()

    @task()
    def send_stock_file_to_minio():
        return sirene_stock_processor.send_stock_to_minio()

    @task.bash
    def clean_up() -> str:
        return f"rm -rf {sirene_stock_processor.config.tmp_folder}"

    (
        clean_previous_outputs()
        >> download_stock()
        >> send_stock_file_to_minio()
        >> clean_up()
    )


# Instantiate the DAG
data_processing_sirene_stock()
