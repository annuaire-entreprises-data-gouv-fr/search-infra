from airflow.models import Variable

from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

MARCHE_INCLUSION_CONFIG = DataSourceConfig(
    name="marche_inclusion",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/marche_inclusion",
    object_storage_path="marche_inclusion",
    file_name="stock_marche_inclusion",
    url_api="https://lemarche.inclusion.beta.gouv.fr",
    endpoint_api="/api/siae",
    auth_api=Variable.get("SECRET_TOKEN_MARCHE_INCLUSION", ""),
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}marche_inclusion/latest/stock_marche_inclusion.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}marche_inclusion/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/marche_inclusion/stock_marche_inclusion.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS marche_inclusion
        (
            siren TEXT PRIMARY KEY,
            type_siae TEXT,
            est_siae INTEGER

        );
        COMMIT;
    """,
)
