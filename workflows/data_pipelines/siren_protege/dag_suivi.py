from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.helpers.utils import check_if_prod
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege import suivi


@dag(
    tags=["siren_protege", "diffusion", "demarche_numerique", "suivi"],
    default_args={"depends_on_past": False, "retries": 1},
    schedule="0 10 * * 1" if check_if_prod() else None,
    start_date=datetime(2026, 10, 1, tzinfo=ZoneInfo("Europe/Paris")),
    dagrun_timeout=timedelta(minutes=10),
    catchup=False,
    on_failure_callback=[Notification(), EmailNotification(to=EMAIL_LIST)],
    max_active_runs=1,
)
def suivi_siren_protege():
    @task
    def notify_names_not_verified():
        return suivi.notify_names_not_verified()

    @task
    def notify_missing_fields():
        return suivi.notify_missing_fields()

    notify_names_not_verified()
    notify_missing_fields()


suivi_siren_protege()
