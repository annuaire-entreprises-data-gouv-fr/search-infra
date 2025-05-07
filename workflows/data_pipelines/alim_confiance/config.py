from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

ALIM_CONFIANCE_CONFIG = DataSourceConfig(
    name="alim_confiance",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/alim_confiance",
    minio_path="alim_confiance",
    file_name="alim_confiance",
    files_to_download={
        "alim_confiance": {
            "url": f"{DATA_GOUV_BASE_URL}fff0cc27-977b-40d5-9c11-f7e4e79a0b72",
            "resource_id": "fff0cc27-977b-40d5-9c11-f7e4e79a0b72",
            "destination": f"{DataSourceConfig.base_tmp_folder}/alim_confiance/alim-confiance-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}alim_confiance/latest/alim_confiance.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}alim_confiance/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/alim_confiance/alim_confiance.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS alim_confiance
        (
            siren PRIMARY KEY,
            est_alim_confiance INTEGER
        );
        COMMIT;
    """,
)
