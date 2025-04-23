from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

SPECTACLE_CONFIG = DataSourceConfig(
    name="spectacle",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/spectacle",
    minio_path="spectacle",
    file_name="spectacle",
    files_to_download={
        "spectacle": {
            "resource_id": "fb6c3b2e-da8c-4e69-a719-6a96329e4cb2",
            "url": f"{DATA_GOUV_BASE_URL}fb6c3b2e-da8c-4e69-a719-6a96329e4cb2",
            "destination": f"{DataSourceConfig.base_tmp_folder}/spectacle/spectacle-download.csv",
        }
    },
    url_minio=f"{MINIO_BASE_URL}spectacle/latest/spectacle.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}spectacle/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/spectacle/spectacle.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS spectacle
        (
            siren TEXT PRIMARY KEY,
            est_entrepreneur_spectacle INTEGER,
            statut_entrepreneur_spectacle TEXT
        );
        COMMIT;
    """,
)
