from datetime import timedelta

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.external_data.task_functions import (
    compare_versions_file,
    preprocess_convcollective_data,
    publish_mattermost,
    update_es,
)
from dag_datalake_sirene.task_functions import (
    get_colors,
    get_object_minio,
    put_object_minio,
)
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "update-elk-convention-collective"
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
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["convcollective"],
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

    preprocess_convcollective_data = PythonOperator(
        task_id="preprocess_convcollective_data",
        python_callable=preprocess_convcollective_data,
        op_args=(TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",),
    )

    get_latest_convcollective_data = PythonOperator(
        task_id="get_latest_convcollective_data",
        python_callable=get_object_minio,
        op_args=(
            "convcollective-latest.csv",
            "ae/external_data/" + ENV + "/convcollective/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/convcollective-latest.csv",
        ),
    )

    compare_versions_file = ShortCircuitOperator(
        task_id="compare_versions_file",
        python_callable=compare_versions_file,
        op_args=(
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/convcollective-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/convcollective-new.csv",
        ),
    )

    update_es = PythonOperator(
        task_id="update_es",
        python_callable=update_es,
        op_args=(
            "convcollective",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/convcollective-new.csv",
            "convcollective-errors.txt",
            "current",
        ),
    )

    put_file_error_to_minio = PythonOperator(
        task_id="put_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "convcollective-errors.txt",
            "ae/external_data/" + ENV + "/convcollective/convcollective-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    put_file_latest_to_minio = PythonOperator(
        task_id="put_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "convcollective-new.csv",
            "ae/external_data/" + ENV + "/convcollective/convcollective-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_args=(
            "Infos Annuaire : Données des Conventions Collectives"
            " mises à jour sur l'API !",
        ),
    )

    clean_previous_folder.set_upstream(get_colors)
    preprocess_convcollective_data.set_upstream(clean_previous_folder)
    get_latest_convcollective_data.set_upstream(preprocess_convcollective_data)
    compare_versions_file.set_upstream(get_latest_convcollective_data)
    update_es.set_upstream(compare_versions_file)
    put_file_error_to_minio.set_upstream(update_es)
    put_file_latest_to_minio.set_upstream(update_es)
    publish_mattermost.set_upstream(put_file_error_to_minio)
    publish_mattermost.set_upstream(put_file_latest_to_minio)
