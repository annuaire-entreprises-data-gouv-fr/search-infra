from dag_datalake_sirene.config import (
    MINIO_BASE_URL,
    DataSourceConfig,
)

RGE_CONFIG = DataSourceConfig(
    name="rge",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/rge",
    minio_path="rge",
    file_name="rge",
    files_to_download={
        "rge": {
            "url": "https://data.ademe.fr/data-fair/api/v1/datasets/"
            "liste-des-entreprises-rge-2/lines?size=10000&select=siret%2Ccode_qualification",
        }
    },
    url_minio=f"{MINIO_BASE_URL}rge/latest/rge.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}rge/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS rge
        (
            siret TEXT PRIMARY KEY,
            liste_rge TEXT
        );
        COMMIT;
    """,
)
