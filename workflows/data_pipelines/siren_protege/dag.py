from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, setup, task, teardown

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.helpers.utils import check_if_prod
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.demarche_numerique import (
    DemarcheNumeriqueProcessor,
)
from data_pipelines_annuaire.workflows.data_pipelines.siren_protege.processor import (
    SirenProtegeProcessor,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    tags=["siren_protege", "diffusion", "demarche_numerique"],
    default_args=default_args,
    # Every hour in production only
    # It can still be triggered by hand in other environments
    schedule="0 * * * *" if check_if_prod() else None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    dagrun_timeout=timedelta(minutes=30),
    catchup=False,
    on_failure_callback=[Notification(), EmailNotification(to=EMAIL_LIST)],
    max_active_runs=1,
)
def data_processing_siren_protege():
    siren_protege_processor = SirenProtegeProcessor()

    @setup
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {siren_protege_processor.config.tmp_folder} && mkdir -p {siren_protege_processor.config.tmp_folder}"

    @task
    def process_accepted_dossiers():
        return DemarcheNumeriqueProcessor().process_accepted_dossiers()

    @task
    def notify_dossiers_with_issues(result):
        return DemarcheNumeriqueProcessor().notify_dossiers_with_issues(result)

    # Publish the list even if some dossiers could not be processed
    # so the dossiers already copied and the manual edits in Grist are not delayed
    @task(trigger_rule="all_done")
    def preprocess_data():
        return siren_protege_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return siren_protege_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return siren_protege_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return siren_protege_processor.compare_files_object_storage()

    @task
    def check_last_dossier_insertion_date():
        return siren_protege_processor.check_last_dossier_insertion_date()

    @teardown
    @task.bash
    def clean_up():
        return f"rm -rf {siren_protege_processor.config.tmp_folder}"

    result = process_accepted_dossiers()
    notify_dossiers_with_issues(result)
    (
        clean_previous_outputs()
        >> result
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
        >> check_last_dossier_insertion_date()
        >> clean_up()
    )


data_processing_siren_protege()
