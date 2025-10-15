from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

from data_pipelines_annuaire.config import EMAIL_LIST
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.workflows.data_pipelines.colter.config import (
    COLTER_CONFIG,
    ELUS_CONFIG,
)
from data_pipelines_annuaire.workflows.data_pipelines.colter.processor import (
    ColterProcessor,
    ElusProcessor,
)

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}

dataset_colter = Asset(COLTER_CONFIG.name)


@dag(
    tags=["collectivités", "communes", "régions", "départements"],
    default_args=default_args,
    schedule="0 16 * * *",
    start_date=pendulum.today("UTC").add(days=-8),
    dagrun_timeout=timedelta(minutes=60),
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
    params={},
    catchup=False,
)
def data_processing_collectivite_territoriale():
    colter_processor = ColterProcessor()

    @task.bash
    def clean_previous_outputs():
        return (
            f"rm -rf {COLTER_CONFIG.tmp_folder} && mkdir -p {COLTER_CONFIG.tmp_folder}"
        )

    @task()
    def download_data():
        return colter_processor.download_data()

    @task()
    def preprocess_data():
        return colter_processor.preprocess_data()

    @task()
    def data_validation():
        return colter_processor.data_validation()

    @task
    def save_date_last_modified():
        return colter_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return colter_processor.send_file_to_object_storage()

    @task(outlets=[dataset_colter])
    def compare_files_object_storage():
        return colter_processor.compare_files_object_storage()

    return (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> data_validation()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
    )


@dag(
    tags=["collectivités", "élus", "conseillers", "epci"],
    default_args=default_args,
    schedule=[dataset_colter],
    start_date=pendulum.today("UTC").add(days=-8),
    dagrun_timeout=timedelta(minutes=60),
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
    params={},
    catchup=False,
)
def data_processing_collectivite_territoriale_elus():
    elus_processor = ElusProcessor()

    @task.bash
    def clean_previous_outputs():
        return f"rm -rf {ELUS_CONFIG.tmp_folder} && mkdir -p {ELUS_CONFIG.tmp_folder}"

    @task()
    def download_data():
        return elus_processor.download_data()

    @task
    def preprocess_data():
        return elus_processor.preprocess_data()

    @task
    def save_date_last_modified():
        return elus_processor.save_date_last_modified()

    @task
    def send_file_to_object_storage():
        return elus_processor.send_file_to_object_storage()

    @task
    def compare_files_object_storage():
        return elus_processor.compare_files_object_storage()

    return (
        clean_previous_outputs()
        >> download_data()
        >> preprocess_data()
        >> save_date_last_modified()
        >> send_file_to_object_storage()
        >> compare_files_object_storage()
    )


data_processing_collectivite_territoriale()
data_processing_collectivite_territoriale_elus()
