from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.helpers.utils import check_if_prod
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.demarche_numerique.processor import (
    DemarcheNumeriqueProcessor,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    tags=["siren_protege", "diffusion", "demarche_numerique"],
    default_args=default_args,
    # Scheduled to run every hour in production only.
    # It can still be triggered by hand in other environments if needed.
    schedule="0 * * * *" if check_if_prod() else None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    dagrun_timeout=timedelta(minutes=30),
    catchup=False,
    on_failure_callback=[Notification(), EmailNotification(to=EMAIL_LIST)],
    max_active_runs=1,
)
def data_processing_siren_protege_demarche_numerique():
    @task
    def process_accepted_dossiers():
        return DemarcheNumeriqueProcessor().process_accepted_dossiers()

    @task
    def notify_dossiers_with_issues(result):
        return DemarcheNumeriqueProcessor().notify_dossiers_with_issues(result)

    notify_dossiers_with_issues(process_accepted_dossiers())


data_processing_siren_protege_demarche_numerique()
