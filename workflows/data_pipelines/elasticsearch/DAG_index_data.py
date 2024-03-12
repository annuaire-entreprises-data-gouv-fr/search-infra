from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.contrib.operators.ssh_operator import SSHOperator

from dag_datalake_sirene.helpers.flush_cache import flush_cache

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    index import (
    get_next_index_name,
    check_elastic_index,
    create_elastic_index,
    update_elastic_alias,
    fill_elastic_siren_index,
    delete_previous_elastic_indices,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    sitemap import (
    create_sitemap,
    update_sitemap,
)
from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    fetch_db import get_latest_database

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.task_functions.\
    send_notification import (
    send_notification_success_tchap,
    send_notification_failure_tchap,
)
# fmt: on
from dag_datalake_sirene.tests.e2e_tests.run_tests import run_e2e_tests
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ELK_DAG_NAME,
    AIRFLOW_SNAPSHOT_DAG_NAME,
    AIRFLOW_DAG_FOLDER,
    EMAIL_LIST,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
    API_IS_REMOTE,
    PATH_AIO,
)
from airflow.operators.clean_folder import CleanFolderOperator


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=AIRFLOW_ELK_DAG_NAME,
    default_args=default_args,
    schedule_interval=None,  # Triggered by database etl
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 12),
    tags=["index", "elasticsearch"],
    catchup=False,  # False to ignore past runs
    on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
) as dag:
    get_next_index_name = PythonOperator(
        task_id="get_next_index_name",
        provide_context=True,
        python_callable=get_next_index_name,
    )

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{AIRFLOW_DAG_TMP}+{AIRFLOW_DAG_FOLDER}+{AIRFLOW_ELK_DAG_NAME}",
    )

    get_latest_sqlite_database = create_sqlite_database = PythonOperator(
        task_id="get_latest_sqlite_db",
        provide_context=True,
        python_callable=get_latest_database,
    )

    delete_previous_elastic_indices = PythonOperator(
        task_id="delete_previous_elastic_indices",
        provide_context=True,
        python_callable=delete_previous_elastic_indices,
    )

    create_elastic_index = PythonOperator(
        task_id="create_elastic_index",
        provide_context=True,
        python_callable=create_elastic_index,
    )

    fill_elastic_siren_index = PythonOperator(
        task_id="fill_elastic_siren_index",
        provide_context=True,
        python_callable=fill_elastic_siren_index,
    )

    check_elastic_index = PythonOperator(
        task_id="check_elastic_index",
        provide_context=True,
        python_callable=check_elastic_index,
    )

    update_elastic_alias = PythonOperator(
        task_id="update_elastic_alias",
        provide_context=True,
        python_callable=update_elastic_alias,
    )

    create_sitemap = PythonOperator(
        task_id="create_sitemap",
        provide_context=True,
        python_callable=create_sitemap,
    )

    update_sitemap = PythonOperator(
        task_id="update_sitemap",
        provide_context=True,
        python_callable=update_sitemap,
    )

    test_api = PythonOperator(
        task_id="test_api",
        provide_context=True,
        python_callable=run_e2e_tests,
    )

    send_notification_tchap = PythonOperator(
        task_id="send_notification_tchap",
        python_callable=send_notification_success_tchap,
    )

    clean_previous_folder.set_upstream(get_next_index_name)
    get_latest_sqlite_database.set_upstream(clean_previous_folder)

    create_elastic_index.set_upstream(get_latest_sqlite_database)
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
        test_api.set_upstream(trigger_snapshot_dag)
        send_notification_tchap.set_upstream([test_api, update_sitemap])
    else:
        execute_aio_container = SSHOperator(
            ssh_conn_id="SERVER",
            task_id="execute_aio_container",
            command=f"cd {PATH_AIO} "
            f"&& docker-compose -f docker-compose-aio.yml up --build -d --force",
            cmd_timeout=60,
            dag=dag,
        )
        flush_cache = PythonOperator(
            task_id="flush_cache",
            provide_context=True,
            python_callable=flush_cache,
            op_args=(
                REDIS_HOST,
                REDIS_PORT,
                REDIS_DB,
                REDIS_PASSWORD,
            ),
        )
        execute_aio_container.set_upstream(update_elastic_alias)
        test_api.set_upstream(execute_aio_container)
        flush_cache.set_upstream(test_api)
