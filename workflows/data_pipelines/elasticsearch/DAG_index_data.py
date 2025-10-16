import os
import shutil
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task

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
from data_pipelines_annuaire.helpers.flush_cache import flush_cache

# fmt: on
from data_pipelines_annuaire.tests.e2e_tests.run_tests import run_e2e_tests

# fmt: off
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.\
    index import (
    check_elastic_index,
    create_elastic_index,
    delete_previous_elastic_indices,
    fill_elastic_siren_index,
    get_next_index_name,
    update_elastic_alias,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.\
    send_notification import (
    send_notification_failure_mattermost,
    send_notification_success_mattermost,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.\
    sitemap import (
    create_sitemap,
    update_sitemap,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.\
    source_updates import (
    sync_data_source_updates,
)
from data_pipelines_annuaire.workflows.data_pipelines.elasticsearch.task_functions.fetch_db import (
    get_latest_database,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def clean_folders(folder_path):
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        shutil.rmtree(folder_path)

with DAG(
    dag_id=AIRFLOW_ELK_DAG_NAME,
    default_args=default_args,
    schedule=None,  # Triggered by database etl
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 12),
    tags=["index", "elasticsearch"],
    catchup=False,  # False to ignore past runs
    on_failure_callback=send_notification_failure_mattermost,
    max_active_runs=1,
) as dag:
    get_next_index_name = PythonOperator(
        task_id="get_next_index_name",

        python_callable=get_next_index_name,
    )

    @task
    def clean_folder():
        clean_folders(f"{AIRFLOW_DAG_TMP}{AIRFLOW_DAG_FOLDER}{AIRFLOW_ELK_DAG_NAME}")

    get_latest_sqlite_database = create_sqlite_database = PythonOperator(
        task_id="get_latest_sqlite_db",

        python_callable=get_latest_database,
    )

    delete_previous_elastic_indices = PythonOperator(
        task_id="delete_previous_elastic_indices",

        python_callable=delete_previous_elastic_indices,
    )

    create_elastic_index = PythonOperator(
        task_id="create_elastic_index",

        python_callable=create_elastic_index,
    )

    fill_elastic_siren_index = PythonOperator(
        task_id="fill_elastic_siren_index",

        python_callable=fill_elastic_siren_index,
    )

    check_elastic_index = PythonOperator(
        task_id="check_elastic_index",

        python_callable=check_elastic_index,
    )

    update_elastic_alias = PythonOperator(
        task_id="update_elastic_alias",

        python_callable=update_elastic_alias,
    )

    create_sitemap = PythonOperator(
        task_id="create_sitemap",

        python_callable=create_sitemap,
    )

    update_sitemap = PythonOperator(
        task_id="update_sitemap",

        python_callable=update_sitemap,
    )

    test_api = PythonOperator(
        task_id="test_api",

        python_callable=run_e2e_tests,
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_success_mattermost,
    )

    sync_data_source_updates = PythonOperator(
        task_id="sync_data_source_updates_file",
        python_callable=sync_data_source_updates,
    )

    get_next_index_name >> clean_folder() >> get_latest_sqlite_database

    delete_previous_elastic_indices.set_upstream(get_latest_sqlite_database)
    create_elastic_index.set_upstream(delete_previous_elastic_indices)
    fill_elastic_siren_index.set_upstream(create_elastic_index)
    check_elastic_index.set_upstream(fill_elastic_siren_index)
    update_elastic_alias.set_upstream(check_elastic_index)

    create_sitemap.set_upstream(update_elastic_alias)
    update_sitemap.set_upstream(create_sitemap)

    if API_IS_REMOTE:
        trigger_snapshot_dag = TriggerDagRunOperator(
            task_id="trigger_snapshot_dag",
            trigger_dag_id=AIRFLOW_SNAPSHOT_DAG_NAME,
            wait_for_completion=True,
            deferrable=False,
        )

        trigger_snapshot_dag.set_upstream(update_elastic_alias)
        sync_data_source_updates.set_upstream(trigger_snapshot_dag)
        test_api.set_upstream(trigger_snapshot_dag)

        [test_api, update_sitemap] >> clean_folder() >> send_notification_mattermost
        send_notification_mattermost.set_upstream(update_sitemap)
    else:
        flush_cache = PythonOperator(
            task_id="flush_cache",

            python_callable=flush_cache,
            op_args=(
                REDIS_HOST,
                REDIS_PORT,
                REDIS_DB,
                REDIS_PASSWORD,
            ),
        )
        sync_data_source_updates.set_upstream(update_elastic_alias)
        test_api.set_upstream(sync_data_source_updates)
        [test_api, update_sitemap] >> clean_folder() >> flush_cache
