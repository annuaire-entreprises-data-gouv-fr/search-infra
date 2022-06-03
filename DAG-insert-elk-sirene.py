import os
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.utils import (
    check_elastic_index,
    create_elastic_siren,
    fill_siren,
    format_sirene_notebook,
    get_colors,
    update_color_file,
)
from dotenv import load_dotenv
from operators.clean_folder import CleanFolderOperator

load_dotenv()

DAG_FOLDER = os.getenv("DAG_FOLDER")
DAG_NAME = os.getenv("DAG_NAME")
TMP_FOLDER = os.getenv("TMP_FOLDER")


with DAG(
    dag_id=os.getenv("DAG_NAME"),
    schedule_interval="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60 * 8),
    tags=["siren"],
) as dag:
    get_colors = PythonOperator(
        task_id="get_colors", provide_context=True, python_callable=get_colors
    )

    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=f"{TMP_FOLDER}+{DAG_FOLDER}+{DAG_NAME}",
    )

    format_sirene_notebook = PythonOperator(
        task_id="format_sirene_notebook",
        provide_context=True,
        python_callable=format_sirene_notebook,
    )

    clean_tmp_folder = CleanFolderOperator(
        task_id="clean_tmp_folder",
        folder_path=f"{TMP_FOLDER}+{DAG_FOLDER}+{DAG_NAME}",
    )

    create_elastic_siren = PythonOperator(
        task_id="create_elastic_siren",
        provide_context=True,
        python_callable=create_elastic_siren,
    )

    fill_elastic_siren = PythonOperator(
        task_id="fill_elastic_siren", provide_context=True, python_callable=fill_siren
    )

    check_elastic_index = PythonOperator(
        task_id="check_elastic_index",
        provide_context=True,
        python_callable=check_elastic_index,
    )

    update_color_file = PythonOperator(
        task_id="update_color_file",
        provide_context=True,
        python_callable=update_color_file,
    )

    clean_previous_folder.set_upstream(get_colors)
    format_sirene_notebook.set_upstream(clean_previous_folder)
    clean_tmp_folder.set_upstream(format_sirene_notebook)
    create_elastic_siren.set_upstream(clean_tmp_folder)
    fill_elastic_siren.set_upstream(create_elastic_siren)
    check_elastic_index.set_upstream(fill_elastic_siren)
    update_color_file.set_upstream(check_elastic_index)
