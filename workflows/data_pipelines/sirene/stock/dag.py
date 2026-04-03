from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.sirene.stock.processor import (
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
    start_date=pendulum.today("UTC").add(days=-1),
    dagrun_timeout=timedelta(minutes=60 * 2),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification(),
    on_success_callback=Notification(),
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
    def convert_etablissement_coordinates():
        return sirene_stock_processor.convert_stock_etablissement_coordinates()

    @task()
    def send_file_to_object_storage():
        return sirene_stock_processor.send_stock_to_object_storage()

    @task.bash
    def clean_up() -> str:
        return f"rm -rf {sirene_stock_processor.config.tmp_folder}"

    return (
        clean_previous_outputs()
        >> download_stock()
        >> convert_etablissement_coordinates()
        >> send_file_to_object_storage()
        >> clean_up()
    )


data_processing_sirene_stock()
