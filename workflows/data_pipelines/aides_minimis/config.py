from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

AIDES_MINIMIS_CONFIG = DataSourceConfig(
    name="aides_minimis",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/aides_minimis",
    object_storage_path="aides_minimis",
    file_name="aides_minimis",
    files_to_download={
        "aides_minimis": {
            "url": f"{DATA_GOUV_BASE_URL}f43a5294-8f79-4b40-b5f9-88b62ea1ad14",
            "resource_id": "f43a5294-8f79-4b40-b5f9-88b62ea1ad14",
            "destination": f"{DataSourceConfig.base_tmp_folder}/aides_minimis/aides-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}aides_minimis/latest/aides_minimis.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}aides_minimis/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/aides_minimis/aides_minimis.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS aides_minimis
        (
            siren TEXT PRIMARY KEY,
            aide_de_minimis_renseignee INTEGER
        );
        COMMIT;
    """,
)
