from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import (
    EMAIL_LIST,
)
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.rne.stock.processor import (
    RneStockProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
    "max_active_run": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    tags=["rne", "stock", "download"],
    default_args=default_args,
    schedule_interval=None,  # <- No automatic scheduling
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60 * 18),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def get_rne_stock():
    rne_stock_processor = RneStockProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {rne_stock_processor.config.tmp_folder} && mkdir -p {rne_stock_processor.config.tmp_folder}"

    @task
    def get_rne_latest_stock():
        rne_stock_processor.download_stock(
            rne_stock_processor.config.files_to_download["ftp"]["url"]
        )

    @task
    def unzip_files_and_upload_minio():
        rne_stock_processor.send_stock_to_minio()

    (
        clean_previous_outputs()
        >> get_rne_latest_stock()
        >> unzip_files_and_upload_minio()
    )


# Instantiate the DAG
get_rne_stock()
