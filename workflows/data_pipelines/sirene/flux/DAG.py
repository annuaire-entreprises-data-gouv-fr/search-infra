from datetime import timedelta

from airflow.utils.dates import days_ago

from airflow.decorators import dag, task
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.insee_processor import (
    InseeFluxProcessor,
)

from dag_datalake_sirene.workflows.data_pipelines.sirene.flux.config import (
    FLUX_SIRENE_CONFIG,
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
    schedule_interval="0 4 2-31 * *",  # Daily at 4 AM except the 1st of every month
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification.send_notification_tchap,
    on_success_callback=Notification.send_notification_tchap,
)
def data_processing_sirene_flux():
    insee_processor = InseeFluxProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {FLUX_SIRENE_CONFIG.tmp_folder} && mkdir -p {FLUX_SIRENE_CONFIG.tmp_folder}"

    @task
    def get_flux_unites_legales():
        return insee_processor.get_current_flux_unite_legale()

    @task
    def get_flux_etablissements():
        return insee_processor.get_current_flux_etablissement()

    @task
    def save_date_last_modified():
        return insee_processor.save_date_last_modified()

    @task
    def send_flux_to_minio():
        return insee_processor.send_flux_to_minio()

    (
        clean_previous_outputs()
        >> get_flux_unites_legales()
        >> get_flux_etablissements()
        >> save_date_last_modified()
        >> send_flux_to_minio()
    )


# Instantiate the DAG
data_processing_sirene_flux()
