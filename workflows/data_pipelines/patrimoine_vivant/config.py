from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

PATRIMOINE_VIVANT_CONFIG = DataSourceConfig(
    name="patrimoine_vivant",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/patrimoine_vivant",
    minio_path="patrimoine_vivant",
    file_name="patrimoine_vivant",
    files_to_download={
        "patrimoine_vivant": {
            "url": f"{DATA_GOUV_BASE_URL}9bca3cea-1659-422d-8c4d-40137b2a1ee8",
            "resource_id": "9bca3cea-1659-422d-8c4d-40137b2a1ee8",
            "destination": f"{DataSourceConfig.base_tmp_folder}/patrimoine_vivant/patrimoine-vivant-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}patrimoine_vivant/latest/patrimoine_vivant.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}patrimoine_vivant/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/patrimoine_vivant/patrimoine_vivant.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS patrimoine_vivant
        (
            siren PRIMARY KEY,
            est_patrimoine_vivant INTEGER
        );
        COMMIT;
    """,
)
