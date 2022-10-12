from datetime import timedelta

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.external_data.task_functions import (
    preprocess_nondiff_data,
    publish_mattermost,
    update_es,
)
from dag_datalake_sirene.task_functions import get_colors, put_object_minio
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "update-elk-sirene"
TMP_FOLDER = "/tmp/"
EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")
PATH_AIO = Variable.get("PATH_AIO")

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 22 5,10,15,20,25 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["update", "sirene"],
) as dag:
    get_colors = PythonOperator(
        task_id="get_colors",
        provide_context=True,
        python_callable=get_colors,
    )

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{TMP_FOLDER}+{DAG_FOLDER}+{DAG_NAME}",
    )

    preprocess_nondiff_data = PythonOperator(
        task_id="preprocess_nondiff_data",
        python_callable=preprocess_nondiff_data,
        op_args=(TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",),
    )

    update_es_nondiff = PythonOperator(
        task_id="update_es_nondiff",
        python_callable=update_es,
        op_args=(
            "nondiff",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/nondiff-new.csv",
            "nondiff-errors.txt",
            "current",
        ),
    )

    put_nondiff_file_error_to_minio = PythonOperator(
        task_id="put_nondiff_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "nondiff-errors.txt",
            "ae/external_data/" + ENV + "/nondiff/nondiff-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    put_nondiff_file_latest_to_minio = PythonOperator(
        task_id="put_nondiff_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "nondiff-new.csv",
            "ae/external_data/" + ENV + "/nondiff/nondiff-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_args=(
            "Infos Annuaire : Données des entreprises non-diffusibles "
            "mises à jour sur l'API !",
        ),
    )

    clean_previous_folder.set_upstream(get_colors)
    preprocess_nondiff_data.set_upstream(clean_previous_folder)
    update_es_nondiff.set_upstream(preprocess_nondiff_data)
    put_nondiff_file_error_to_minio.set_upstream(update_es_nondiff)
    put_nondiff_file_latest_to_minio.set_upstream(update_es_nondiff)
    publish_mattermost.set_upstream(put_nondiff_file_error_to_minio)
    publish_mattermost.set_upstream(put_nondiff_file_latest_to_minio)
