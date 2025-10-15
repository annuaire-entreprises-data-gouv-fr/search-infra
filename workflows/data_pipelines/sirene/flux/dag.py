from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from data_pipelines_annuaire.config import (
    AIRFLOW_ETL_DAG_NAME,
    EMAIL_LIST,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.sirene.flux.processor import (
    SireneFluxProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


@dag(
    tags=["sirene", "flux"],
    default_args=default_args,
    schedule="30 6 * * *",  # Daily at 6:30 AM
    start_date=datetime(2025, 8, 20),  # more naive than days_ago()
    dagrun_timeout=timedelta(minutes=60 * 12),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def data_processing_sirene_flux():
    sirene_flux_processor = SireneFluxProcessor()

    @task
    def check_updates_availability():
        return sirene_flux_processor.check_updates_availability()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {sirene_flux_processor.config.tmp_folder} && mkdir -p {sirene_flux_processor.config.tmp_folder}"

    @task
    def get_flux_unites_legales():
        return sirene_flux_processor.get_current_flux_unite_legale()

    @task
    def get_flux_etablissements():
        return sirene_flux_processor.get_current_flux_etablissement()

    @task
    def save_date_last_modified():
        return sirene_flux_processor.save_date_last_modified()

    @task
    def send_flux_to_object_storage():
        return sirene_flux_processor.send_flux_to_object_storage()

    @task.bash
    def clean_up():
        return f"rm -rf {sirene_flux_processor.config.tmp_folder}"

    trigger_etl_dag = TriggerDagRunOperator(
        task_id="trigger_indexing_dag",
        trigger_dag_id=AIRFLOW_ETL_DAG_NAME,
        wait_for_completion=False,
        deferrable=False,
    )

    return (
        check_updates_availability()
        >> clean_previous_outputs()
        >> get_flux_unites_legales()
        >> get_flux_etablissements()
        >> save_date_last_modified()
        >> send_flux_to_object_storage()
        >> clean_up()
        >> trigger_etl_dag
    )


# Instantiate the DAG
data_processing_sirene_flux()
