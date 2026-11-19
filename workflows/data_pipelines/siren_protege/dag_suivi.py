from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.helpers.utils import check_if_prod
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.demarche_numerique import (
    DemarcheNumeriqueProcessor,
)


@dag(
    tags=["siren_protege", "diffusion", "demarche_numerique"],
    default_args={"depends_on_past": False, "retries": 1},
    # Every Monday at 10:00 Paris time in production only
    # It can still be triggered by hand in other environments
    schedule="0 10 * * 1" if check_if_prod() else None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Paris"),
    dagrun_timeout=timedelta(minutes=10),
    catchup=False,
    on_failure_callback=[Notification(), EmailNotification(to=EMAIL_LIST)],
    max_active_runs=1,
)
def suivi_siren_protege():
    @task
    def notify_names_not_confirmed():
        return DemarcheNumeriqueProcessor().notify_names_not_confirmed()

    notify_names_not_confirmed()


suivi_siren_protege()
