from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

AIDES_CONFIG = DataSourceConfig(
    name="aides",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/aides",
    object_storage_path="aides",
    file_name="aides",
    files_to_download={
        "aides": {
            "url": f"{DATA_GOUV_BASE_URL}f43a5294-8f79-4b40-b5f9-88b62ea1ad14",
            "resource_id": "f43a5294-8f79-4b40-b5f9-88b62ea1ad14",
            "destination": f"{DataSourceConfig.base_tmp_folder}/aides/aides-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}aides/latest/aides.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}aides/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/aides/aides.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS aides
        (
            siren TEXT PRIMARY KEY,
            aides_de_minimis_renseignee INTEGER
        );
        COMMIT;
    """,
)
