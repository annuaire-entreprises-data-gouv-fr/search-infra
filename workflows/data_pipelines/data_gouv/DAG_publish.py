from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.workflows.data_pipelines.data_gouv.task_functions import (
    get_latest_database,
    fill_ul_file,
    fill_etab_file,
)

from dag_datalake_sirene.workflows.data_pipelines.data_gouv.task_functions import (
    send_notification_failure_tchap,
)

from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_FOLDER,
    EMAIL_LIST,
    AIRFLOW_PUBLISH_DAG_NAME,
)
from operators.clean_folder import CleanFolderOperator


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=AIRFLOW_PUBLISH_DAG_NAME,
    default_args=default_args,
    schedule_interval=None,  # Triggered by database etl
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 12),
    tags=["publish", "datagouv"],
    catchup=False,  # False to ignore past runs
    on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
) as dag:

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{AIRFLOW_DAG_TMP}{AIRFLOW_DAG_FOLDER}{AIRFLOW_PUBLISH_DAG_NAME}",
    )

    get_latest_sqlite_database = create_sqlite_database = PythonOperator(
        task_id="get_latest_sqlite_db",
        provide_context=True,
        python_callable=get_latest_database,
    )

    fill_ul_file = PythonOperator(
        task_id="fill_ul_file",
        python_callable=fill_ul_file,
    )

    fill_etab_file = PythonOperator(
        task_id="fill_etab_file",
        python_callable=fill_etab_file,
    )

    get_latest_sqlite_database.set_upstream(clean_previous_folder)
    fill_ul_file.set_upstream(get_latest_sqlite_database)
    fill_etab_file.set_upstream(fill_ul_file)
