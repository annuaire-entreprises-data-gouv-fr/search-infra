from datetime import timedelta

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    EMAIL_LIST,
)
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.helpers.utils import check_if_prod
from dag_datalake_sirene.workflows.data_pipelines.data_gouv.processor import (
    DataGouvProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    tags=["publication", "data.gouv"],
    default_args=default_args,
    schedule="0 17 * * *",  # Executes daily at 5 PM
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60 * 3),
    params={},
    catchup=False,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
    max_active_runs=1,
)
def publish_files_in_data_gouv():
    data_gouv_processor = DataGouvProcessor()

    @task.short_circuit
    def check_if_prod_env():
        return check_if_prod()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {AIRFLOW_DAG_TMP}publish_data_gouv && mkdir -p {AIRFLOW_DAG_TMP}publish_data_gouv"

    @task
    def get_latest_sqlite_db():
        return data_gouv_processor.get_latest_database()

    @task
    def fill_unite_legale_file():
        return data_gouv_processor.fill_ul_file()

    @task
    def fill_liste_administration_file():
        return data_gouv_processor.process_administration_list()

    @task
    def upload_unite_legale_and_administration_files_to_minio():
        return data_gouv_processor.upload_ul_and_administration_to_minio()

    @task
    def fill_etablissement_file():
        return data_gouv_processor.fill_etab_file()

    @task
    def upload_etablissement_file_to_minio():
        return data_gouv_processor.upload_etab_to_minio()

    @task
    def send_files_to_data_gouv():
        return data_gouv_processor.publish_to_datagouv()

    @task.bash
    def clean_outputs():
        return f"rm -rf {AIRFLOW_DAG_TMP}publish_data_gouv"

    (
        check_if_prod_env()
        >> clean_previous_outputs()
        >> get_latest_sqlite_db()
        >> fill_unite_legale_file()
        >> fill_liste_administration_file()
        >> upload_unite_legale_and_administration_files_to_minio()
        >> fill_etablissement_file()
        >> upload_etablissement_file_to_minio()
        >> send_files_to_data_gouv()
        >> clean_outputs()
    )


# Instantiate the DAG
publish_files_in_data_gouv()
