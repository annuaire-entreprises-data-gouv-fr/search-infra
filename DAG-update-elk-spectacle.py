from datetime import timedelta

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.data_aggregation.entrepreneur_spectacle import (
    preprocess_spectacle_data,
)
from dag_datalake_sirene.data_aggregation.update_elasticsearch import (
    update_elasticsearch_with_new_data,
)
from dag_datalake_sirene.helpers.utils import compare_versions_file, publish_mattermost
from dag_datalake_sirene.task_functions import (
    get_colors,
    get_object_minio,
    put_object_minio,
)
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "update-elk-entrepreneur-spectacle"
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
    schedule_interval="0 22 4,5,10,15,20,25 * *",
    start_date=days_ago(6),
    dagrun_timeout=timedelta(minutes=60),
    tags=["spectacle"],
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

    preprocess_spectacle_data = PythonOperator(
        task_id="preprocess_spectacle_data",
        python_callable=preprocess_spectacle_data,
        op_args=(f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",),
    )

    get_latest_spectacle_data = PythonOperator(
        task_id="get_latest_spectacle_data",
        python_callable=get_object_minio,
        op_args=(
            "spectacle-latest.csv",
            f"ae/data_aggregation/{ENV}/spectacle/",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/spectacle-latest.csv",
        ),
    )

    compare_versions_file = ShortCircuitOperator(
        task_id="compare_versions_file",
        python_callable=compare_versions_file,
        op_args=(
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/spectacle-latest.csv",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/spectacle-new.csv",
        ),
    )

    update_es = PythonOperator(
        task_id="update_es",
        python_callable=update_elasticsearch_with_new_data,
        op_args=(
            "spectacle",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/spectacle-new.csv",
            "spectacle-errors.txt",
            "current",
        ),
    )

    put_file_error_to_minio = PythonOperator(
        task_id="put_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "spectacle-errors.txt",
            f"ae/data_aggregation/{ENV}/spectacle/spectacle-errors.txt",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",
        ),
    )

    put_file_latest_to_minio = PythonOperator(
        task_id="put_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "spectacle-new.csv",
            f"ae/data_aggregation/{ENV}/spectacle/spectacle-latest.csv",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",
        ),
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_args=(
            "Infos Annuaire : Données des entrepreneurs du spectacle vivant"
            " mises à jour sur l'API !",
        ),
    )

    clean_previous_folder.set_upstream(get_colors)
    preprocess_spectacle_data.set_upstream(clean_previous_folder)
    get_latest_spectacle_data.set_upstream(preprocess_spectacle_data)
    compare_versions_file.set_upstream(get_latest_spectacle_data)
    update_es.set_upstream(compare_versions_file)
    put_file_error_to_minio.set_upstream(update_es)
    put_file_latest_to_minio.set_upstream(update_es)
    publish_mattermost.set_upstream(put_file_error_to_minio)
    publish_mattermost.set_upstream(put_file_latest_to_minio)
