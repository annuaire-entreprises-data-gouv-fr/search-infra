from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, Variable
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.external_data.task_functions import update_es
from dag_datalake_sirene.task_functions import (
    check_elastic_index,
    count_nombre_etablissements,
    count_nombre_etablissements_ouverts,
    create_dirig_pm_table,
    create_dirig_pp_table,
    create_elastic_index,
    create_etablissement_table,
    create_siege_only_table,
    create_sitemap,
    create_sqlite_database,
    create_unite_legale_table,
    fill_elastic_index,
    get_colors,
    get_object_minio,
    put_object_minio,
    update_color_file,
    update_sitemap,
)
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "insert-elk-sirene"
TMP_FOLDER = "/tmp/"
EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")
PATH_AIO = Variable.get("PATH_AIO")

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
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

    create_sqlite_database = PythonOperator(
        task_id="create_sqlite_database",
        provide_context=True,
        python_callable=create_sqlite_database,
    )

    create_unite_legale_table = PythonOperator(
        task_id="create_unite_legale_table",
        provide_context=True,
        python_callable=create_unite_legale_table,
    )

    create_etablissement_table = PythonOperator(
        task_id="create_etablissement_table",
        provide_context=True,
        python_callable=create_etablissement_table,
    )

    count_nombre_etablissements = PythonOperator(
        task_id="count_nombre_etablissements",
        provide_context=True,
        python_callable=count_nombre_etablissements,
    )

    count_nombre_etablissements_ouverts = PythonOperator(
        task_id="count_nombre_etablissements_ouverts",
        provide_context=True,
        python_callable=count_nombre_etablissements_ouverts,
    )

    create_siege_only_table = PythonOperator(
        task_id="create_siege_only_table",
        provide_context=True,
        python_callable=create_siege_only_table,
    )

    get_dirigeants_database = PythonOperator(
        task_id="get_dirig_database",
        provide_context=True,
        python_callable=get_object_minio,
        op_args=(
            "inpi.db",
            "inpi/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/inpi.db",
        ),
    )

    create_dirig_pp_table = PythonOperator(
        task_id="create_dirig_pp_table",
        provide_context=True,
        python_callable=create_dirig_pp_table,
    )

    create_dirig_pm_table = PythonOperator(
        task_id="create_dirig_pm_table",
        provide_context=True,
        python_callable=create_dirig_pm_table,
    )

    create_elastic_index = PythonOperator(
        task_id="create_elastic_index",
        provide_context=True,
        python_callable=create_elastic_index,
    )

    create_sitemap = PythonOperator(
        task_id="create_sitemap",
        provide_context=True,
        python_callable=create_sitemap,
    )

    update_sitemap = PythonOperator(
        task_id="update_sitemap",
        provide_context=True,
        python_callable=update_sitemap,
    )

    fill_elastic_index = PythonOperator(
        task_id="fill_elastic_index",
        provide_context=True,
        python_callable=fill_elastic_index,
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

    get_latest_rge_data = PythonOperator(
        task_id="get_latest_rge_data",
        python_callable=get_object_minio,
        op_args=(
            "rge-latest.csv",
            "ae/external_data/rge/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/rge-latest.csv",
        ),
    )

    update_es_rge = PythonOperator(
        task_id="update_es_rge",
        python_callable=update_es,
        op_args=(
            "rge",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/rge-latest.csv",
            "rge-errors.txt",
            "next",
        ),
    )

    put_file_error_to_minio_rge = PythonOperator(
        task_id="put_file_error_to_minio_rge",
        python_callable=put_object_minio,
        op_args=(
            "rge-errors.txt",
            "ae/external_data/rge/rge-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    get_latest_convcollective_data = PythonOperator(
        task_id="get_latest_convcollective_data",
        python_callable=get_object_minio,
        op_args=(
            "convcollective-latest.csv",
            "ae/external_data/convcollective/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/convcollective-latest.csv",
        ),
    )

    update_es_convcollective = PythonOperator(
        task_id="update_es_convcollective",
        python_callable=update_es,
        op_args=(
            "convcollective",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/convcollective-latest.csv",
            "convcollective-errors.txt",
            "next",
        ),
    )

    put_file_error_to_minio_convcollective = PythonOperator(
        task_id="put_file_error_to_minio_convcollective",
        python_callable=put_object_minio,
        op_args=(
            "convcollective-errors.txt",
            "ae/external_data/convcollective/convcollective-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    get_latest_finess_data = PythonOperator(
        task_id="get_latest_finess_data",
        python_callable=get_object_minio,
        op_args=(
            "finess-latest.csv",
            "ae/external_data/finess/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/finess-latest.csv",
        ),
    )

    update_es_finess = PythonOperator(
        task_id="update_es_finess",
        python_callable=update_es,
        op_args=(
            "finess",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/finess-latest.csv",
            "finess-errors.txt",
            "next",
        ),
    )

    put_file_error_to_minio_finess = PythonOperator(
        task_id="put_file_error_to_minio_finess",
        python_callable=put_object_minio,
        op_args=(
            "finess-errors.txt",
            "ae/external_data/finess/finess-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    get_latest_spectacle_data = PythonOperator(
        task_id="get_latest_spectacle_data",
        python_callable=get_object_minio,
        op_args=(
            "spectacle-latest.csv",
            "ae/external_data/spectacle/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/spectacle-latest.csv",
        ),
    )

    update_es_spectacle = PythonOperator(
        task_id="update_es_spectacle",
        python_callable=update_es,
        op_args=(
            "spectacle",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/spectacle-latest.csv",
            "spectacle-errors.txt",
            "next",
        ),
    )

    put_file_error_to_minio_spectacle = PythonOperator(
        task_id="put_file_error_to_minio_spectacle",
        python_callable=put_object_minio,
        op_args=(
            "spectacle-errors.txt",
            "ae/external_data/spectacle/spectacle-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )
    get_latest_uai_data = PythonOperator(
        task_id="get_latest_uai_data",
        python_callable=get_object_minio,
        op_args=(
            "uai-latest.csv",
            "ae/external_data/uai/",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/uai-latest.csv",
        ),
    )

    update_es_uai = PythonOperator(
        task_id="update_es_uai",
        python_callable=update_es,
        op_args=(
            "uai",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/uai-latest.csv",
            "uai-errors.txt",
            "next",
        ),
    )

    put_file_error_to_minio_uai = PythonOperator(
        task_id="put_file_error_to_minio_uai",
        python_callable=put_object_minio,
        op_args=(
            "uai-errors.txt",
            "ae/external_data/uai/uai-errors.txt",
            TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
        ),
    )

    execute_aio_container = SSHOperator(
        ssh_conn_id="SERVER",
        task_id="execute_aio_container",
        command=f"cd {PATH_AIO} "
        f"&& docker-compose -f docker-compose-aio.yml up --build -d --force",
        dag=dag,
    )

    success_email_body = f"""
    Hi, <br><br>
    insert-elk-sirene-{ENV} DAG has been executed successfully at {datetime.now()}.
    """

    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_LIST,
        subject=f"Airflow Success: DAG-{ENV}!",
        html_content=success_email_body,
        dag=dag,
    )

    clean_previous_folder.set_upstream(get_colors)
    create_sqlite_database.set_upstream(clean_previous_folder)
    create_unite_legale_table.set_upstream(create_sqlite_database)
    create_etablissement_table.set_upstream(create_unite_legale_table)
    count_nombre_etablissements.set_upstream(create_etablissement_table)
    count_nombre_etablissements_ouverts.set_upstream(count_nombre_etablissements)
    create_siege_only_table.set_upstream(count_nombre_etablissements_ouverts)
    get_dirigeants_database.set_upstream(create_siege_only_table)
    create_dirig_pp_table.set_upstream(get_dirigeants_database)
    create_dirig_pm_table.set_upstream(create_dirig_pp_table)
    create_elastic_index.set_upstream(create_dirig_pm_table)
    fill_elastic_index.set_upstream(create_elastic_index)
    create_sitemap.set_upstream(fill_elastic_index)
    update_sitemap.set_upstream(create_sitemap)
    check_elastic_index.set_upstream(fill_elastic_index)
    update_color_file.set_upstream(check_elastic_index)
    get_latest_rge_data.set_upstream(update_color_file)
    update_es_rge.set_upstream(get_latest_rge_data)
    put_file_error_to_minio_rge.set_upstream(update_es_rge)
    get_latest_convcollective_data.set_upstream(update_color_file)
    update_es_convcollective.set_upstream(get_latest_convcollective_data)
    put_file_error_to_minio_convcollective.set_upstream(update_es_convcollective)
    get_latest_finess_data.set_upstream(update_color_file)
    update_es_finess.set_upstream(get_latest_finess_data)
    put_file_error_to_minio_finess.set_upstream(update_es_finess)
    get_latest_spectacle_data.set_upstream(update_color_file)
    update_es_spectacle.set_upstream(get_latest_spectacle_data)
    put_file_error_to_minio_spectacle.set_upstream(update_es_spectacle)
    get_latest_uai_data.set_upstream(update_color_file)
    update_es_uai.set_upstream(get_latest_uai_data)
    put_file_error_to_minio_uai.set_upstream(update_es_uai)
    execute_aio_container.set_upstream(put_file_error_to_minio_rge)
    execute_aio_container.set_upstream(put_file_error_to_minio_convcollective)
    execute_aio_container.set_upstream(put_file_error_to_minio_finess)
    execute_aio_container.set_upstream(put_file_error_to_minio_spectacle)
    execute_aio_container.set_upstream(put_file_error_to_minio_uai)
    send_email.set_upstream(execute_aio_container)
    send_email.set_upstream(update_sitemap)
