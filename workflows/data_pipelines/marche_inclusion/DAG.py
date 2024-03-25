from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.marche_inclusion.task_functions\
    import (
    get_structures_siae,
    send_file_minio,
)
# fmt: on
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    EMAIL_LIST,
    MARCHE_INCLUSION_TMP_FOLDER,
)

DAG_NAME = "data_processing_marche_inclusion"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}marche_inclusion/"

default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": EMAIL_LIST,
    "retries": 1,
}


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 1 * * *",  # Run daily at 1 AM
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["marche inclusion"],
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {MARCHE_INCLUSION_TMP_FOLDER} && "
            f"mkdir -p {MARCHE_INCLUSION_TMP_FOLDER}"
        ),
    )

    get_structures_siae = PythonOperator(
        task_id="get_structures_siae",
        python_callable=get_structures_siae,
    )
    send_file_minio = PythonOperator(
        task_id="send_file_minio",
        python_callable=send_file_minio,
    )

    get_structures_siae.set_upstream(clean_previous_outputs)
    send_file_minio.set_upstream(get_structures_siae)
