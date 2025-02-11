from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from dag_datalake_sirene.workflows.data_pipelines.data_gouv.task_functions import (
    get_latest_database,
    fill_administration_file,
    fill_ul_file,
    upload_ul_and_admin_to_minio,
    fill_etab_file,
    upload_etab_to_minio,
    publish_data,
    notification_tchap,
)

from dag_datalake_sirene.workflows.data_pipelines.data_gouv.task_functions import (
    send_notification_failure_tchap,
)
from dag_datalake_sirene.helpers.utils import check_if_prod

from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
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
    schedule_interval="0 17 * * *",  # Executes daily at 5 PM
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 3),
    tags=["publish", "datagouv"],
    catchup=False,  # False to ignore past runs
    on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
) as dag:
    check_if_prod = ShortCircuitOperator(
        task_id="check_if_prod", python_callable=check_if_prod
    )

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{AIRFLOW_DAG_TMP}{AIRFLOW_PUBLISH_DAG_NAME}",
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

    fill_administration_file = PythonOperator(
        task_id="fill_administration_file",
        python_callable=fill_administration_file,
    )

    upload_ul_to_minio = PythonOperator(
        task_id="upload_ul_to_minio",
        python_callable=upload_ul_and_admin_to_minio,
    )
    fill_etab_file = PythonOperator(
        task_id="fill_etab_file",
        python_callable=fill_etab_file,
    )

    upload_etab_to_minio = PythonOperator(
        task_id="upload_etab_to_minio",
        python_callable=upload_etab_to_minio,
    )

    publish_files = PythonOperator(
        task_id="publish_data",
        python_callable=publish_data,
    )
    clean_outputs = BashOperator(
        task_id="clean_outputs",
        bash_command=f"rm -rf {AIRFLOW_DAG_TMP}{AIRFLOW_PUBLISH_DAG_NAME}",
    )
    notification_tchap = PythonOperator(
        task_id="notification_tchap", python_callable=notification_tchap
    )

    clean_previous_folder.set_upstream(check_if_prod)
    get_latest_sqlite_database.set_upstream(clean_previous_folder)
    fill_ul_file.set_upstream(get_latest_sqlite_database)
    fill_administration_file.set_upstream(fill_ul_file)
    upload_ul_to_minio.set_upstream(fill_administration_file)
    fill_etab_file.set_upstream(upload_ul_to_minio)
    upload_etab_to_minio.set_upstream(fill_etab_file)
    publish_files.set_upstream(upload_etab_to_minio)
    clean_outputs.set_upstream(publish_files)
    notification_tchap.set_upstream(clean_outputs)
