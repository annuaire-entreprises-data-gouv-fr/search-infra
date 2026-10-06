from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, setup, task, teardown

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import EmailNotification, Notification
from data_pipelines_annuaire.workflows.adc.communes.config import (
    ADC_COMMUNES_DAG_NAME,
    ADC_COMMUNES_DATA_DIR,
)
from data_pipelines_annuaire.workflows.adc.communes.processor import (
    check_and_delete_stale_json_files,
    generate_json_files,
    get_latest_sirene_database,
    load_communes,
    load_effectifs,
    upload_json_files,
)

default_args = {
    "depends_on_past": False,
    "retries": 1,
}


@dag(
    dag_id=ADC_COMMUNES_DAG_NAME,
    tags=["adc", "communes", "export"],
    default_args=default_args,
    schedule="0 21 * * 6",  # Saturday evening
    start_date=datetime(2026, 10, 1, tzinfo=UTC),
    dagrun_timeout=timedelta(hours=4),
    catchup=False,
    max_active_runs=1,
    on_failure_callback=[Notification(), EmailNotification(to=EMAIL_LIST)],
    on_success_callback=Notification(),
)
def adc_communes():
    @setup
    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {ADC_COMMUNES_DATA_DIR} && mkdir -p {ADC_COMMUNES_DATA_DIR}"

    @teardown
    @task.bash
    def clean_outputs():
        return f"rm -rf {ADC_COMMUNES_DATA_DIR}"

    sirene_date = get_latest_sirene_database()
    communes = load_communes()
    effectifs_date = load_effectifs()

    clean_previous_outputs() >> [sirene_date, communes]
    communes >> effectifs_date

    return (
        generate_json_files(sirene_date, effectifs_date)
        >> upload_json_files()
        >> check_and_delete_stale_json_files()
        >> clean_outputs()
    )


adc_communes()
