from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.config import EMAIL_LIST
from dag_datalake_sirene.workflows.data_pipelines.demarches_simplifiees.ds_api import (
    get_latest_db,
    fetch_and_save_df_data,
    send_to_minio,
)
from dag_datalake_sirene.config import DS_TMP_FOLDER

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="get_dossiers_demarches_simplifiees",
    default_args=default_args,
    start_date=datetime(2023, 10, 18),
    # schedule_interval="0 1 * * *",  # Run every day at 1 AM
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(days=30),
    tags=["demarches_simplifiees", "dossiers"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DS_TMP_FOLDER} && mkdir -p {DS_TMP_FOLDER}",
    )

    get_latest_db = PythonOperator(
        task_id="get_latest_db",
        python_callable=get_latest_db,
    )

    fetch_and_save_df_data = PythonOperator(
        task_id="fetch_and_save_df_data",
        python_callable=fetch_and_save_df_data,
    )

    send_to_minio = PythonOperator(
        task_id="send_to_minio",
        python_callable=send_to_minio,
    )

    get_latest_db.set_upstream(clean_previous_outputs)
    fetch_and_save_df_data.set_upstream(get_latest_db)
    send_to_minio.set_upstream(fetch_and_save_df_data)
