from datetime import timedelta

from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.data_aggregation.collectivite_territoriale import (
    preprocess_colter_data,
    preprocess_elu_data,
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
DAG_NAME = "update-elk-colter"
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
    tags=["colter"],
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

    preprocess_colter_data = PythonOperator(
        task_id="preprocess_colter_data",
        python_callable=preprocess_colter_data,
        op_args=(f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",),
    )

    get_latest_colter_data = PythonOperator(
        task_id="get_latest_colter_data",
        python_callable=get_object_minio,
        op_args=(
            "colter-latest.csv",
            f"ae/data_aggregation/{ENV}/colter/",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/colter-latest.csv",
        ),
    )

    compare_versions_file_colter = ShortCircuitOperator(
        task_id="compare_versions_file_colter",
        python_callable=compare_versions_file,
        op_args=(
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/colter-latest.csv",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/colter-new.csv",
        ),
    )

    update_es_colter = PythonOperator(
        task_id="update_es_colter",
        python_callable=update_elasticsearch_with_new_data,
        op_args=(
            "colter",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/colter-new.csv",
            "colter-errors.txt",
            "current",
        ),
    )

    put_colter_file_error_to_minio = PythonOperator(
        task_id="put_colter_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "colter-errors.txt",
            f"ae/data_aggregation/{ENV}/colter/colter-errors.txt",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",
        ),
    )

    put_colter_file_latest_to_minio = PythonOperator(
        task_id="put_colter_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "colter-new.csv",
            "ae/data_aggregation/" + ENV + "/colter/colter-latest.csv",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    preprocess_elu_data = PythonOperator(
        task_id="preprocess_elu_data",
        python_callable=preprocess_elu_data,
        op_args=(f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",),
    )

    get_latest_elu_data = PythonOperator(
        task_id="get_latest_elu_data",
        python_callable=get_object_minio,
        op_args=(
            "elu-latest.csv",
            f"ae/data_aggregation/{ENV}/colter/",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/elu-latest.csv",
        ),
    )

    compare_versions_file_elu = ShortCircuitOperator(
        task_id="compare_versions_file_elu",
        python_callable=compare_versions_file,
        op_args=(
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/elu-latest.csv",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/elu-new.csv",
        ),
    )

    update_es_elu = PythonOperator(
        task_id="update_es_elu",
        python_callable=update_elasticsearch_with_new_data,
        op_args=(
            "elu",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/elu-new.csv",
            "elu-errors.txt",
            "current",
        ),
    )

    put_elu_file_error_to_minio = PythonOperator(
        task_id="put_elu_file_error_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "elu-errors.txt",
            "ae/data_aggregation/{ENV}/colter/elu-errors.txt",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",
        ),
    )

    put_elu_file_latest_to_minio = PythonOperator(
        task_id="put_elu_file_latest_to_minio",
        python_callable=put_object_minio,
        op_args=(
            "elu-new.csv",
            f"ae/data_aggregation/{ENV}/colter/elu-latest.csv",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/",
        ),
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_args=(
            "Infos Annuaire : Données des Colter (+Elus) mises à jour sur l'API !",
        ),
    )

    clean_previous_folder.set_upstream(get_colors)
    preprocess_colter_data.set_upstream(clean_previous_folder)
    get_latest_colter_data.set_upstream(preprocess_colter_data)
    compare_versions_file_colter.set_upstream(get_latest_colter_data)
    update_es_colter.set_upstream(compare_versions_file_colter)
    put_colter_file_error_to_minio.set_upstream(update_es_colter)
    put_colter_file_latest_to_minio.set_upstream(update_es_colter)

    preprocess_elu_data.set_upstream(put_colter_file_error_to_minio)
    preprocess_elu_data.set_upstream(put_colter_file_latest_to_minio)
    get_latest_elu_data.set_upstream(preprocess_elu_data)
    compare_versions_file_elu.set_upstream(get_latest_elu_data)
    update_es_elu.set_upstream(compare_versions_file_elu)
    put_elu_file_error_to_minio.set_upstream(update_es_elu)
    put_elu_file_latest_to_minio.set_upstream(update_es_elu)
    publish_mattermost.set_upstream(put_elu_file_error_to_minio)
    publish_mattermost.set_upstream(put_elu_file_latest_to_minio)
