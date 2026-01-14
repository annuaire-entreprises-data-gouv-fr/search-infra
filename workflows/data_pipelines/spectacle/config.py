from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

SPECTACLE_CONFIG = DataSourceConfig(
    name="spectacle",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/spectacle",
    object_storage_path="spectacle",
    file_name="spectacle",
    files_to_download={
        "spectacle": {
            "resource_id": "fb6c3b2e-da8c-4e69-a719-6a96329e4cb2",
            "url": f"{DATA_GOUV_BASE_URL}fb6c3b2e-da8c-4e69-a719-6a96329e4cb2",
            "destination": f"{DataSourceConfig.base_tmp_folder}/spectacle/spectacle-download.csv",
            "encoding": "latin-1",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}spectacle/latest/spectacle.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}spectacle/latest/metadata.json",
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
