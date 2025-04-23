from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

FINESS_CONFIG = DataSourceConfig(
    name="finess",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/finess",
    minio_path="finess",
    file_name="finess",
    files_to_download={
        "finess": {
            "url": f"{DATA_GOUV_BASE_URL}2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
            "resource_id": "2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
            "destination": f"{DataSourceConfig.base_tmp_folder}/finess/finess-download.csv",
        }
    },
    url_minio=f"{MINIO_BASE_URL}finess/latest/finess.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}finess/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS finess
        (
            siret TEXT PRIMARY KEY,
            liste_finess TEXT
        );
        COMMIT;
    """,
)
