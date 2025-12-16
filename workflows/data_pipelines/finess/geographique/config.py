from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

FINESS_GEOGRAPHIQUE_CONFIG = DataSourceConfig(
    name="finess_geographique",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/finess_geographique",
    minio_path="finess_geographique",
    file_name="finess_geographique",
    files_to_download={
        "finess_geographique": {
            "url": f"{DATA_GOUV_BASE_URL}2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
            "resource_id": "2ce43ade-8d2c-4d1d-81da-ca06c82abc68",
            "destination": f"{DataSourceConfig.base_tmp_folder}/finess_geographique/finess-geographique-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}finess_geographique/latest/finess_geographique.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}finess_geographique/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS finess_geographique
        (
            siret TEXT PRIMARY KEY,
            liste_finess_geographique TEXT
        );
        COMMIT;
    """,
)
