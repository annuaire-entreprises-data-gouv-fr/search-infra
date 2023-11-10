from airflow.models import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.data_pipelines.rne.database.vars import TMP_FOLDER
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.data_pipelines.rne.database.task_functions import (
    get_start_date_minio,
    get_latest_db,
    check_db_count,
    create_db,
    process_flux_json_files,
    process_stock_json_files,
    upload_db_to_minio,
    upload_latest_date_rne_minio,
    notification_tchap,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fill_rne_dirigeants_database",
    default_args=default_args,
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=(60 * 20)),
    tags=["data_processing", "rne", "dirigeants"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
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
    notification_tchap = PythonOperator(
        task_id="notification_tchap", python_callable=notification_tchap
    )

    get_start_date.set_upstream(clean_previous_outputs)
    create_db.set_upstream(get_start_date)
    get_latest_db.set_upstream(create_db)
    process_stock_json_files.set_upstream(get_latest_db)
    process_flux_json_files.set_upstream(process_stock_json_files)
    check_db_count.set_upstream(process_flux_json_files)
    upload_db_to_minio.set_upstream(check_db_count)
    upload_latest_date_rne_minio.set_upstream(upload_db_to_minio)
    notification_tchap.set_upstream(upload_latest_date_rne_minio)
