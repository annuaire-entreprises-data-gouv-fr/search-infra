from airflow.models import Variable

from dag_datalake_sirene.config import (
    MINIO_BASE_URL,
    DataSourceConfig,
)

MARCHE_INCLUSION_CONFIG = DataSourceConfig(
    name="marche_inclusion",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/marche_inclusion",
    minio_path="marche_inclusion",
    file_name="stock_marche_inclusion",
    url_api="https://lemarche.inclusion.beta.gouv.fr",
    endpoint_api="/api/siae",
    auth_api=Variable.get("SECRET_TOKEN_MARCHE_INCLUSION", ""),
    url_minio=f"{MINIO_BASE_URL}marche_inclusion/latest/stock_marche_inclusion.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}marche_inclusion/latest/metadata.json",
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
