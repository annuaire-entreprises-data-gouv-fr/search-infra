import os
import shutil
from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task

from data_pipelines_annuaire.config import (
    AIRFLOW_DAG_FOLDER,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ELK_DAG_NAME,
    AIRFLOW_SNAPSHOT_DAG_NAME,
    API_IS_REMOTE,
    EMAIL_LIST,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.helpers.flush_cache import flush_redis_cache
from data_pipelines_annuaire.tests.e2e_tests.run_tests import run_e2e_tests
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.fetch_db import (
    get_latest_database,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.index import (
    check_elastic_index,
    create_elastic_index,
    delete_previous_elastic_indices,
    fill_elastic_siren_index,
    get_next_index_name,
    update_elastic_alias,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.sitemap import (
    create_sitemap,
    update_sitemap,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.source_updates import (
    sync_data_source_updates_file,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id=AIRFLOW_ELK_DAG_NAME,
    default_args=default_args,
    schedule=None,  # Triggered by database etl
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 12),
    tags=["index", "elasticsearch"],
    catchup=False,
    max_active_runs=1,
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
)
def index_elasticsearch():
    @task
    def clean_folder():
        folder_path = f"{AIRFLOW_DAG_TMP}{AIRFLOW_DAG_FOLDER}{AIRFLOW_ELK_DAG_NAME}"
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)

    elastic_alias_updated = (
        get_next_index_name()
        >> clean_folder()
        >> get_latest_database()
        >> delete_previous_elastic_indices()
        >> create_elastic_index()
        >> fill_elastic_siren_index()
        >> check_elastic_index()
        >> update_elastic_alias()
    )

    sitemap_updated = elastic_alias_updated >> create_sitemap() >> update_sitemap()

    if API_IS_REMOTE:
        trigger_snapshot_dag = TriggerDagRunOperator(
            task_id="trigger_snapshot_dag",
            trigger_dag_id=AIRFLOW_SNAPSHOT_DAG_NAME,
            wait_for_completion=True,
            deferrable=False,
        )
        tests_successful = (
            elastic_alias_updated
            >> trigger_snapshot_dag
            >> sync_data_source_updates_file()
            >> run_e2e_tests()
        )
    else:
        tests_successful = (
            elastic_alias_updated
            >> sync_data_source_updates_file()
            >> run_e2e_tests()
            >> flush_redis_cache(REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD)
        )

    return [sitemap_updated, tests_successful] >> clean_folder()


index_elasticsearch()
