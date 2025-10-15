from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import (
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.rne.stock.processor import (
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
    schedule=None,  # <- No automatic scheduling
    start_date=pendulum.today("UTC").add(days=-8),
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
    def unzip_files_and_upload_object_storage():
        rne_stock_processor.send_stock_to_object_storage()

    return (
        clean_previous_outputs()
        >> get_rne_latest_stock()
        >> unzip_files_and_upload_object_storage()
    )


# Instantiate the DAG
get_rne_stock()
