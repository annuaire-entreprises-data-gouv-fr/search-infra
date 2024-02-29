from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from operators.clean_folder import CleanFolderOperator

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_etablissements_tables import (
    add_rne_data_to_siege_table,
    count_nombre_etablissements,
    count_nombre_etablissements_ouverts,
    create_etablissements_table,
    create_date_fermeture_etablissement_table,
    create_flux_etablissements_table,
    create_historique_etablissement_table,
    create_siege_only_table,
    insert_date_fermeture_etablissement,
    replace_etablissements_table,
    replace_siege_only_table,
)

from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_additional_data_tables import (
    create_agence_bio_table,
    create_bilan_financiers_table,
    create_colter_table,
    create_ess_table,
    create_rge_table,
    create_finess_table,
    create_egapro_table,
    create_elu_table,
    create_organisme_formation_table,
    create_spectacle_table,
    create_uai_table,
    create_convention_collective_table,
    create_marche_inclusion_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_dirigeants_tables import (
    create_dirig_pm_table,
    create_dirig_pp_table,
)


from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_sqlite_database import (
    create_sqlite_database,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_unite_legale_tables import (
    create_date_fermeture_unite_legale_table,
    create_flux_unite_legale_table,
    create_historique_unite_legale_table,
    create_unite_legale_table,
    insert_date_fermeture_unite_legale,
    replace_unite_legale_table,
    add_rne_data_to_unite_legale_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.send_notification\
    import (
    send_notification_success_tchap,
    send_notification_failure_tchap,
)
# fmt: on
from dag_datalake_sirene.helpers.minio_helpers import minio_client

from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.upload_db import (
    upload_db_to_minio,
)
from dag_datalake_sirene.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ETL_DAG_NAME,
    AIRFLOW_DAG_FOLDER,
    AIRFLOW_ENV,
    RNE_DATABASE_LOCATION,
    EMAIL_LIST,
)


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=AIRFLOW_ETL_DAG_NAME,
    default_args=default_args,
    schedule_interval="0 5 * * *",  # Run everyday at 5 am
    start_date=datetime(2023, 12, 27),
    dagrun_timeout=timedelta(minutes=60 * 5),
    tags=["preprocessing", "data"],
    catchup=False,  # False to ignore past runs
    on_failure_callback=send_notification_failure_tchap,
    max_active_runs=1,
) as dag:
    clean_previous_folder = CleanFolderOperator(
        task_id="clean_previous_folder",
        folder_path=(f"{AIRFLOW_DAG_TMP}+{AIRFLOW_DAG_FOLDER}+{AIRFLOW_ETL_DAG_NAME}"),
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

    create_historique_unite_legale_table = PythonOperator(
        task_id="create_historique_unite_legale_table",
        provide_context=True,
        python_callable=create_historique_unite_legale_table,
    )

    create_date_fermeture_unite_legale_table = PythonOperator(
        task_id="create_date_fermeture_unite_legale_table",
        provide_context=True,
        python_callable=create_date_fermeture_unite_legale_table,
    )

    insert_date_fermeture_unite_legale = PythonOperator(
        task_id="insert_date_fermeture_unite_legale",
        provide_context=True,
        python_callable=insert_date_fermeture_unite_legale,
    )

    inject_rne_unite_legale_data = PythonOperator(
        task_id="add_rne_siren_data_to_unite_legale_table",
        provide_context=True,
        python_callable=add_rne_data_to_unite_legale_table,
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

    inject_rne_siege_data = PythonOperator(
        task_id="add_rne_data_to_siege_table",
        provide_context=True,
        python_callable=add_rne_data_to_siege_table,
    )

    create_historique_etablissement_table = PythonOperator(
        task_id="create_historique_etablissement_table",
        provide_context=True,
        python_callable=create_historique_etablissement_table,
    )

    create_date_fermeture_etablissement_table = PythonOperator(
        task_id="create_date_fermeture_etablissement_table",
        provide_context=True,
        python_callable=create_date_fermeture_etablissement_table,
    )

    insert_date_fermeture_etablissement = PythonOperator(
        task_id="insert_date_fermeture_etablissement",
        provide_context=True,
        python_callable=insert_date_fermeture_etablissement,
    )

    get_latest_dirigeants_database = PythonOperator(
        task_id="get_dirig_database",
        provide_context=True,
        python_callable=minio_client.get_latest_file_minio,
        op_args=(
            f"ae/{AIRFLOW_ENV}/rne/database/",
            RNE_DATABASE_LOCATION,
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

    create_ess_table = PythonOperator(
        task_id="create_ess_table",
        provide_context=True,
        python_callable=create_ess_table,
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

    create_marche_inclusion_table = PythonOperator(
        task_id="create_marche_inclusion_table",
        provide_context=True,
        python_callable=create_marche_inclusion_table,
    )

    send_database_to_minio = PythonOperator(
        task_id="upload_db_to_minio",
        provide_context=True,
        python_callable=upload_db_to_minio,
    )

    clean_folder = CleanFolderOperator(
        task_id="clean_folder",
        folder_path=(f"{AIRFLOW_DAG_TMP}+{AIRFLOW_DAG_FOLDER}+{AIRFLOW_ETL_DAG_NAME}"),
    )

    success_email_body = f"""
    Hi, <br><br>
    preprocess-data-sirene-{AIRFLOW_ENV} DAG has been executed
    successfully at {datetime.now()}.
    """

    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_LIST,
        subject=f"Airflow Success: DAG-preprocess-{AIRFLOW_ENV}!",
        html_content=success_email_body,
        dag=dag,
    )

    send_notification_tchap = PythonOperator(
        task_id="send_notification_tchap",
        python_callable=send_notification_success_tchap,
    )

    create_sqlite_database.set_upstream(clean_previous_folder)

    create_unite_legale_table.set_upstream(create_sqlite_database)
    create_historique_unite_legale_table.set_upstream(create_unite_legale_table)
    create_date_fermeture_unite_legale_table.set_upstream(
        create_historique_unite_legale_table
    )
    create_etablissements_table.set_upstream(create_date_fermeture_unite_legale_table)
    create_flux_unite_legale_table.set_upstream(create_etablissements_table)
    create_flux_etablissements_table.set_upstream(create_flux_unite_legale_table)
    replace_unite_legale_table.set_upstream(create_flux_etablissements_table)
    insert_date_fermeture_unite_legale.set_upstream(replace_unite_legale_table)
    replace_etablissements_table.set_upstream(insert_date_fermeture_unite_legale)
    count_nombre_etablissements.set_upstream(replace_etablissements_table)
    count_nombre_etablissements_ouverts.set_upstream(count_nombre_etablissements)
    create_siege_only_table.set_upstream(count_nombre_etablissements_ouverts)
    replace_siege_only_table.set_upstream(create_siege_only_table)
    create_historique_etablissement_table.set_upstream(replace_siege_only_table)
    create_date_fermeture_etablissement_table.set_upstream(
        create_historique_etablissement_table
    )
    insert_date_fermeture_etablissement.set_upstream(
        create_date_fermeture_etablissement_table
    )

    get_latest_dirigeants_database.set_upstream(insert_date_fermeture_etablissement)
    inject_rne_unite_legale_data.set_upstream(get_latest_dirigeants_database)
    inject_rne_siege_data.set_upstream(inject_rne_unite_legale_data)
    create_dirig_pp_table.set_upstream(inject_rne_siege_data)
    create_dirig_pm_table.set_upstream(create_dirig_pp_table)

    create_bilan_financiers_table.set_upstream(create_dirig_pm_table)
    create_convention_collective_table.set_upstream(create_bilan_financiers_table)
    create_ess_table.set_upstream(create_convention_collective_table)
    create_rge_table.set_upstream(create_ess_table)
    create_finess_table.set_upstream(create_rge_table)
    create_agence_bio_table.set_upstream(create_finess_table)
    create_organisme_formation_table.set_upstream(create_agence_bio_table)
    create_uai_table.set_upstream(create_organisme_formation_table)
    create_spectacle_table.set_upstream(create_uai_table)
    create_egapro_table.set_upstream(create_spectacle_table)
    create_colter_table.set_upstream(create_egapro_table)
    create_elu_table.set_upstream(create_colter_table)
    create_marche_inclusion_table.set_upstream(create_elu_table)

    send_database_to_minio.set_upstream(create_marche_inclusion_table)

    clean_folder.set_upstream(send_database_to_minio)
    send_email.set_upstream(clean_folder)
    send_notification_tchap.set_upstream(send_email)
