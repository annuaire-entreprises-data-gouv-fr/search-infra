from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.helpers.utils import check_if_prod
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.demarche_numerique.processor import (
    DemarcheNumeriqueProcessor,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.object_storage.config import (
    SIREN_PROTEGE_DAG_NAME,
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

    # Run the data_processing_siren_protege DAG even in case of failure to update the file
    # on the object storage with any processed dossier
    trigger_data_processing_siren_protege_dag = TriggerDagRunOperator(
        task_id="trigger_siren_protege_dag",
        trigger_dag_id=SIREN_PROTEGE_DAG_NAME,
        logical_date=None,
        wait_for_completion=False,
        deferrable=False,
        trigger_rule="all_done",
    )

    result = process_accepted_dossiers()
    (
        result
        >> trigger_data_processing_siren_protege_dag
        >> notify_dossiers_with_issues(result)
    )


data_processing_siren_protege_demarche_numerique()
