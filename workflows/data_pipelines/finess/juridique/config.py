from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

FINESS_JURIDIQUE_CONFIG = DataSourceConfig(
    name="finess_juridique",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/finess_juridique",
    minio_path="finess_juridique",
    file_name="finess_juridique",
    files_to_download={
        "finess_geographique": {
            "url": f"{DATA_GOUV_BASE_URL}2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
            "resource_id": "2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
            "destination": f"{DataSourceConfig.base_tmp_folder}/finess_juridique/finess-geographique-download.csv",
        },
        "finess_juridique": {
            "url": f"{DATA_GOUV_BASE_URL}2cba77b2-f1de-4ef8-8428-bfe660e86844",
            "resource_id": "2cba77b2-f1de-4ef8-8428-bfe660e86844",
            "destination": f"{DataSourceConfig.base_tmp_folder}/finess_juridique/finess-juridique-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}finess_juridique/latest/finess_juridique.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}finess_juridique/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS finess_juridique
        (
            siren TEXT PRIMARY KEY,
            liste_finess_juridique TEXT,
            has_finess_from_geographique_only BOOLEAN
        );
        COMMIT;
    """,
)
