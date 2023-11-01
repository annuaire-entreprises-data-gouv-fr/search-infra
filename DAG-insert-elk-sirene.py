from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, Variable
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.task_functions.check_elastic_index import check_elastic_index
from dag_datalake_sirene.task_functions.count_nombre_etablissements import (
    count_nombre_etablissements,
)
from dag_datalake_sirene.task_functions.count_nombre_etablissements_ouverts import (
    count_nombre_etablissements_ouverts,
)
from dag_datalake_sirene.task_functions.create_additional_data_tables import (
    create_agence_bio_table,
    create_bilan_financiers_table,
    create_colter_table,
    create_rge_table,
    create_finess_table,
    create_egapro_table,
    create_elu_table,
    create_organisme_formation_table,
    create_spectacle_table,
    create_uai_table,
    create_convention_collective_table,
)
from dag_datalake_sirene.task_functions.get_colors import get_colors
from dag_datalake_sirene.task_functions.create_dirig_tables import (
    create_dirig_pm_table,
    create_dirig_pp_table,
)
from dag_datalake_sirene.task_functions.create_elastic_index import create_elastic_index
from dag_datalake_sirene.task_functions.create_etablissements_tables import (
    create_etablissements_table,
)
from dag_datalake_sirene.task_functions.create_etablissements_tables import (
    create_flux_etablissements_table,
)
from dag_datalake_sirene.task_functions.create_siege_only_table import (
    create_siege_only_table,
)
from dag_datalake_sirene.task_functions.create_sitemap import create_sitemap
from dag_datalake_sirene.task_functions.create_sqlite_database import (
    create_sqlite_database,
)
from dag_datalake_sirene.task_functions.create_unite_legale_tables import (
    create_flux_unite_legale_table,
)
from dag_datalake_sirene.task_functions.create_unite_legale_tables import (
    create_unite_legale_table,
)
from dag_datalake_sirene.task_functions.create_rna_table import (
    create_rna_table,
)
from dag_datalake_sirene.task_functions.fill_elastic_index import (
    fill_elastic_index_sirene,
    fill_elastic_index_rna,
)
from dag_datalake_sirene.task_functions.flush_cache import flush_cache
from dag_datalake_sirene.task_functions.get_and_put_minio_object import get_object_minio
from dag_datalake_sirene.task_functions.replace_etablissements_table import (
    replace_etablissements_table,
)
from dag_datalake_sirene.task_functions.replace_siege_only_table import (
    replace_siege_only_table,
)
from dag_datalake_sirene.task_functions.replace_unite_legale_table import (
    replace_unite_legale_table,
)
from dag_datalake_sirene.task_functions.send_notification import (
    send_notification_success_tchap,
    send_notification_failure_tchap,
)
from dag_datalake_sirene.task_functions.update_color_file import update_color_file
from dag_datalake_sirene.task_functions.update_sitemap import update_sitemap
from dag_datalake_sirene.tests.e2e_tests.run_tests import run_e2e_tests
from operators.clean_folder import CleanFolderOperator

DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "insert-elk-sirene"
TMP_FOLDER = "/tmp/"
EMAIL_LIST = Variable.get("EMAIL_LIST")
MINIO_BUCKET = Variable.get("MINIO_BUCKET")
ENV = Variable.get("ENV")
PATH_AIO = Variable.get("PATH_AIO")
REDIS_HOST = "redis"
REDIS_PORT = "6379"
REDIS_DB = "0"
REDIS_PASSWORD = Variable.get("REDIS_PASSWORD")

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
    schedule_interval="0 0 * * 1,3,5",
    start_date=datetime(2023, 9, 4),
    dagrun_timeout=timedelta(minutes=60 * 15),
    tags=["siren"],
    catchup=False,  # False to ignore past runs
    on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
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

    create_rna_table = PythonOperator(
        task_id="create_rna_table",
        provide_context=True,
        python_callable=create_rna_table,
    )

    create_unite_legale_table = PythonOperator(
        task_id="create_unite_legale_table",
        provide_context=True,
        python_callable=create_unite_legale_table,
    )

    create_etablissements_table = PythonOperator(
        task_id="create_etablissements_table",
        provide_context=True,
        python_callable=create_etablissements_table,
    )

    create_flux_unite_legale_table = PythonOperator(
        task_id="create_flux_unite_legale_table",
        provide_context=True,
        python_callable=create_flux_unite_legale_table,
    )

    create_flux_etablissements_table = PythonOperator(
        task_id="create_flux_etablissements_table",
        provide_context=True,
        python_callable=create_flux_etablissements_table,
    )

    replace_unite_legale_table = PythonOperator(
        task_id="replace_unite_legale_table",
        provide_context=True,
        python_callable=replace_unite_legale_table,
    )

    replace_etablissements_table = PythonOperator(
        task_id="replace_etablissements_table",
        provide_context=True,
        python_callable=replace_etablissements_table,
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

    replace_siege_only_table = PythonOperator(
        task_id="replace_siege_only_table",
        provide_context=True,
        python_callable=replace_siege_only_table,
    )

    get_dirigeants_database = PythonOperator(
        task_id="get_dirig_database",
        provide_context=True,
        python_callable=get_object_minio,
        op_args=(
            "inpi.db",
            "inpi/",
            f"{TMP_FOLDER}{DAG_FOLDER}{DAG_NAME}/data/inpi.db",
            MINIO_BUCKET,
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

    create_bilan_financiers_table = PythonOperator(
        task_id="create_bilan_financiers_table",
        provide_context=True,
        python_callable=create_bilan_financiers_table,
    )

    create_convention_collective_table = PythonOperator(
        task_id="create_convention_collective_table",
        provide_context=True,
        python_callable=create_convention_collective_table,
    )

    create_rge_table = PythonOperator(
        task_id="create_rge_table",
        provide_context=True,
        python_callable=create_rge_table,
    )

    create_finess_table = PythonOperator(
        task_id="create_finess_table",
        provide_context=True,
        python_callable=create_finess_table,
    )

    create_agence_bio_table = PythonOperator(
        task_id="create_agence_bio_table",
        provide_context=True,
        python_callable=create_agence_bio_table,
    )

    create_organisme_formation_table = PythonOperator(
        task_id="create_organisme_formation_table",
        provide_context=True,
        python_callable=create_organisme_formation_table,
    )

    create_uai_table = PythonOperator(
        task_id="create_uai_table",
        provide_context=True,
        python_callable=create_uai_table,
    )

    create_spectacle_table = PythonOperator(
        task_id="create_spectacle_table",
        provide_context=True,
        python_callable=create_spectacle_table,
    )

    create_egapro_table = PythonOperator(
        task_id="create_egapro_table",
        provide_context=True,
        python_callable=create_egapro_table,
    )

    create_elu_table = PythonOperator(
        task_id="create_elu_table",
        provide_context=True,
        python_callable=create_elu_table,
    )

    create_colter_table = PythonOperator(
        task_id="create_colter_table",
        provide_context=True,
        python_callable=create_colter_table,
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

    fill_elastic_index_sirene = PythonOperator(
        task_id="fill_elastic_index_sirene",
        provide_context=True,
        python_callable=fill_elastic_index_sirene,
    )

    fill_elastic_index_rna = PythonOperator(
        task_id="fill_elastic_index_rna",
        provide_context=True,
        python_callable=fill_elastic_index_rna,
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

    execute_aio_container = SSHOperator(
        ssh_conn_id="SERVER",
        task_id="execute_aio_container",
        command=f"cd {PATH_AIO} "
        f"&& docker-compose -f docker-compose-aio.yml up --build -d --force",
        cmd_timeout=60,
        dag=dag,
    )

    test_api = PythonOperator(
        task_id="test_api",
        provide_context=True,
        python_callable=run_e2e_tests,
    )

    flush_cache = PythonOperator(
        task_id="flush_cache",
        provide_context=True,
        python_callable=flush_cache,
        op_args=(
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            REDIS_PASSWORD,
        ),
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

    send_notification_tchap = PythonOperator(
        task_id="send_notification_tchap",
        python_callable=send_notification_success_tchap,
    )

    clean_previous_folder.set_upstream(get_colors)
    create_sqlite_database.set_upstream(clean_previous_folder)

    create_rna_table.set_upstream(create_sqlite_database)
    # create_unite_legale_table.set_upstream(create_sqlite_database)
    create_unite_legale_table.set_upstream(create_rna_table)

    create_etablissements_table.set_upstream(create_unite_legale_table)
    create_flux_unite_legale_table.set_upstream(create_etablissements_table)
    create_flux_etablissements_table.set_upstream(create_flux_unite_legale_table)
    replace_unite_legale_table.set_upstream(create_flux_etablissements_table)
    replace_etablissements_table.set_upstream(replace_unite_legale_table)
    count_nombre_etablissements.set_upstream(replace_etablissements_table)
    count_nombre_etablissements_ouverts.set_upstream(count_nombre_etablissements)
    create_siege_only_table.set_upstream(count_nombre_etablissements_ouverts)
    replace_siege_only_table.set_upstream(create_siege_only_table)

    get_dirigeants_database.set_upstream(replace_siege_only_table)
    create_dirig_pp_table.set_upstream(get_dirigeants_database)
    create_dirig_pm_table.set_upstream(create_dirig_pp_table)

    create_bilan_financiers_table.set_upstream(create_dirig_pm_table)
    create_convention_collective_table.set_upstream(create_bilan_financiers_table)
    create_rge_table.set_upstream(create_convention_collective_table)
    create_finess_table.set_upstream(create_rge_table)
    create_agence_bio_table.set_upstream(create_finess_table)
    create_organisme_formation_table.set_upstream(create_agence_bio_table)
    create_uai_table.set_upstream(create_organisme_formation_table)
    create_spectacle_table.set_upstream(create_uai_table)
    create_egapro_table.set_upstream(create_spectacle_table)
    create_colter_table.set_upstream(create_egapro_table)
    create_elu_table.set_upstream(create_colter_table)

    create_elastic_index.set_upstream(create_elu_table)
    fill_elastic_index_sirene.set_upstream(create_elastic_index)
    fill_elastic_index_rna.set_upstream(fill_elastic_index_sirene)
    check_elastic_index.set_upstream(fill_elastic_index_rna)

    create_sitemap.set_upstream(check_elastic_index)
    update_sitemap.set_upstream(create_sitemap)

    update_color_file.set_upstream(check_elastic_index)

    execute_aio_container.set_upstream(update_color_file)
    test_api.set_upstream(execute_aio_container)
    flush_cache.set_upstream(test_api)

    send_email.set_upstream(flush_cache)
    send_email.set_upstream(update_sitemap)
    send_notification_tchap.set_upstream(send_email)
