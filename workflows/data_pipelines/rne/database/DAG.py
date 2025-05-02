from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dag_datalake_sirene.config import EMAIL_LIST, RNE_DB_TMP_FOLDER
from dag_datalake_sirene.workflows.data_pipelines.rne.database.task_functions import (
    check_db_count,
    create_db,
    get_latest_db,
    get_start_date_minio,
    notification_mattermost,
    process_flux_json_files,
    process_stock_json_files,
    remove_duplicates,
    upload_db_to_minio,
    upload_latest_date_rne_minio,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fill_rne_database",
    default_args=default_args,
    start_date=datetime(2023, 10, 11),
    schedule="0 2 * * *",  # Run daily at 2 am
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=(60 * 200)),
    tags=["rne", "database"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {RNE_DB_TMP_FOLDER} && mkdir -p {RNE_DB_TMP_FOLDER}",
    )

    get_start_date = PythonOperator(
        task_id="get_start_date", python_callable=get_start_date_minio
    )
    create_db = PythonOperator(task_id="create_db", python_callable=create_db)
    get_latest_db = PythonOperator(
        task_id="get_latest_db", python_callable=get_latest_db
    )
    process_stock_json_files = PythonOperator(
        task_id="process_stock_json_files", python_callable=process_stock_json_files
    )
    process_flux_json_files = PythonOperator(
        task_id="process_flux_json_files", python_callable=process_flux_json_files
    )

    remove_duplicates = PythonOperator(
        task_id="remove_duplicates", python_callable=remove_duplicates
    )
    check_db_count = PythonOperator(
        task_id="check_db_count", python_callable=check_db_count
    )
    upload_db_to_minio = PythonOperator(
        task_id="upload_db_to_minio", python_callable=upload_db_to_minio
    )
    upload_latest_date_rne_minio = PythonOperator(
        task_id="upload_latest_date_rne_minio",
        python_callable=upload_latest_date_rne_minio,
    )

    clean_outputs = BashOperator(
        task_id="clean_outputs",
        bash_command=f"rm -rf {RNE_DB_TMP_FOLDER}",
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost", python_callable=notification_mattermost
    )

    get_start_date.set_upstream(clean_previous_outputs)
    create_db.set_upstream(get_start_date)
    get_latest_db.set_upstream(create_db)
    process_stock_json_files.set_upstream(get_latest_db)
    process_flux_json_files.set_upstream(process_stock_json_files)
    remove_duplicates.set_upstream(process_flux_json_files)
    check_db_count.set_upstream(remove_duplicates)
    upload_db_to_minio.set_upstream(check_db_count)
    upload_latest_date_rne_minio.set_upstream(upload_db_to_minio)
    clean_outputs.set_upstream(upload_latest_date_rne_minio)
    send_notification_mattermost.set_upstream(clean_outputs)
