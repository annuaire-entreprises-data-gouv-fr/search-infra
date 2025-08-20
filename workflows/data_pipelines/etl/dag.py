import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DAG_NAME,
    AIRFLOW_ETL_DAG_NAME,
    EMAIL_LIST,
    SIRENE_DATABASE_LOCATION,
)
from dag_datalake_sirene.helpers import Notification
from dag_datalake_sirene.helpers.database_constructor import DatabaseTableConstructor
from dag_datalake_sirene.workflows.data_pipelines.achats_responsables.config import (
    ACHATS_RESPONSABLES_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.agence_bio.config import (
    AGENCE_BIO_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.alim_confiance.config import (
    ALIM_CONFIANCE_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.bilan_ges.config import (
    BILAN_GES_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.bilans_financiers.config import (
    BILANS_FINANCIERS_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.colter.config import (
    COLTER_CONFIG,
    ELUS_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.convcollective.config import (
    CONVENTION_COLLECTIVE_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.egapro.config import EGAPRO_CONFIG
from dag_datalake_sirene.workflows.data_pipelines.ess_france.config import ESS_CONFIG

# fmt: off
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_etablissements_tables import (
    add_rne_data_to_siege_table,
    count_nombre_etablissement,
    count_nombre_etablissement_ouvert,
    create_date_fermeture_etablissement_table,
    create_etablissement_table,
    create_flux_etablissement_table,
    create_historique_etablissement_table,
    create_siege_table,
    insert_date_fermeture_etablissement,
    replace_etablissement_table,
    replace_siege_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_immatriculation_table import (
    copy_immatriculation_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_json_last_modified import (
    create_data_source_last_modified_file,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.\
    create_unite_legale_tables import (
    add_ancien_siege_flux_data,
    add_rne_siren_data_to_unite_legale_table,
    create_date_fermeture_unite_legale_table,
    create_flux_unite_legale_table,
    create_historique_unite_legale_table,
    create_unite_legale_table,
    insert_date_fermeture_unite_legale,
    replace_unite_legale_table,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.create_dirig_tables import (
    create_dirig_pm_table,
    create_dirig_pp_table,
    get_rne_database,
)

# fmt: on
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.determine_sirene_date import (
    determine_sirene_date,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.upload_db import (
    upload_db_to_minio,
)
from dag_datalake_sirene.workflows.data_pipelines.etl.task_functions.validation import (
    validate_table,
)
from dag_datalake_sirene.workflows.data_pipelines.finess.config import FINESS_CONFIG
from dag_datalake_sirene.workflows.data_pipelines.formation.config import (
    FORMATION_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.marche_inclusion.config import (
    MARCHE_INCLUSION_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.patrimoine_vivant.config import (
    PATRIMOINE_VIVANT_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.rge.config import RGE_CONFIG
from dag_datalake_sirene.workflows.data_pipelines.spectacle.config import (
    SPECTACLE_CONFIG,
)
from dag_datalake_sirene.workflows.data_pipelines.uai.config import UAI_CONFIG

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id=AIRFLOW_ETL_DAG_NAME,
    tags=["database", "all-data"],
    default_args=default_args,
    schedule="0 5 * * *",  # Run everyday at 5 am local time
    start_date=datetime(2025, 8, 20),
    dagrun_timeout=timedelta(minutes=60 * 5),
    params={},
    catchup=False,  # False to ignore past runs
    on_failure_callback=Notification.send_notification_mattermost,
    on_success_callback=Notification.send_notification_mattermost,
    max_active_runs=1,
)
def database_constructor():
    @task.bash
    def clean_previous_tmp_folder() -> str:
        db_folder_path = os.path.dirname(SIRENE_DATABASE_LOCATION)
        return f"rm -rf {db_folder_path} && mkdir -p {db_folder_path}"

    @task(retries=0)
    def validate_unite_legale_stock_table() -> None:
        validate_table(
            table_name="unite_legale",
            datatabase_location=SIRENE_DATABASE_LOCATION,
            validations=["row_count"],
            file_alias="stock",
        )

    @task(retries=0)
    def validate_etablissement_stock_table() -> None:
        validate_table(
            table_name="etablissement",
            datatabase_location=SIRENE_DATABASE_LOCATION,
            validations=["row_count"],
            file_alias="stock",
        )

    @task(retries=0)
    def validate_unite_legale_stock_flux_table() -> None:
        validate_table(
            table_name="unite_legale",
            datatabase_location=SIRENE_DATABASE_LOCATION,
            validations=["row_count"],
            file_alias="stock+flux",
        )

    @task(retries=0)
    def validate_etablissement_stock_flux_table() -> None:
        validate_table(
            table_name="etablissement",
            datatabase_location=SIRENE_DATABASE_LOCATION,
            validations=["row_count"],
            file_alias="stock+flux",
        )

    @task(retries=0)
    def validate_unite_legale_with_rne_table() -> None:
        validate_table(
            table_name="unite_legale",
            datatabase_location=SIRENE_DATABASE_LOCATION,
            validations=["row_count"],
            file_alias="stock+flux+rne",
        )

    @task_group
    def additional_data_enrichement() -> None:
        config_list = [
            AGENCE_BIO_CONFIG,
            BILANS_FINANCIERS_CONFIG,
            COLTER_CONFIG,
            ELUS_CONFIG,
            ESS_CONFIG,
            RGE_CONFIG,
            FINESS_CONFIG,
            EGAPRO_CONFIG,
            FORMATION_CONFIG,
            SPECTACLE_CONFIG,
            UAI_CONFIG,
            CONVENTION_COLLECTIVE_CONFIG,
            MARCHE_INCLUSION_CONFIG,
            ACHATS_RESPONSABLES_CONFIG,
            PATRIMOINE_VIVANT_CONFIG,
            ALIM_CONFIANCE_CONFIG,
            BILAN_GES_CONFIG,
        ]
        tasks = []
        for config in config_list:

            @task(task_id=f"create_{config.name}_table")
            def create_table(config=config) -> None:
                DatabaseTableConstructor(config).etl_create_table(
                    SIRENE_DATABASE_LOCATION
                )

            task_instance = create_table()
            tasks.append(task_instance)

        for i in range(len(tasks) - 1):
            tasks[i] >> tasks[i + 1]

    @task.bash
    def clean_current_tmp_folder() -> str:
        db_folder_path = os.path.dirname(SIRENE_DATABASE_LOCATION)
        return f"rm -rf {db_folder_path} && mkdir -p {db_folder_path}"

    trigger_indexing_dag = TriggerDagRunOperator(
        task_id="trigger_indexing_dag",
        trigger_dag_id=AIRFLOW_ELK_DAG_NAME,
        wait_for_completion=False,
        deferrable=False,
    )

    (
        clean_previous_tmp_folder()
        >> determine_sirene_date()
        >> create_unite_legale_table()
        >> validate_unite_legale_stock_table()
        >> create_historique_unite_legale_table()
        >> create_date_fermeture_unite_legale_table()
        >> create_etablissement_table()
        >> validate_etablissement_stock_table()
        >> create_flux_unite_legale_table()
        >> create_flux_etablissement_table()
        >> replace_unite_legale_table()
        >> validate_unite_legale_stock_flux_table()
        >> insert_date_fermeture_unite_legale()
        >> replace_etablissement_table()
        >> validate_etablissement_stock_flux_table()
        >> count_nombre_etablissement()
        >> count_nombre_etablissement_ouvert()
        >> create_siege_table()
        >> replace_siege_table()
        >> add_ancien_siege_flux_data()
        >> create_historique_etablissement_table()
        >> create_date_fermeture_etablissement_table()
        >> insert_date_fermeture_etablissement()
        >> get_rne_database()
        >> add_rne_siren_data_to_unite_legale_table()
        >> validate_unite_legale_with_rne_table()
        >> add_rne_data_to_siege_table()
        >> create_dirig_pp_table()
        >> create_dirig_pm_table()
        >> copy_immatriculation_table()
        >> additional_data_enrichement()
        >> upload_db_to_minio()
        >> create_data_source_last_modified_file()
        >> clean_current_tmp_folder()
        >> trigger_indexing_dag
    )


database_constructor()
