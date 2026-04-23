from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

AIDES_ADEME_CONFIG = DataSourceConfig(
    name="aides_ademe",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/aides_ademe",
    object_storage_path="aides_ademe",
    file_name="aides_ademe",
    files_to_download={
        "aides_ademe": {
            "url": f"{DATA_GOUV_BASE_URL}a514049e-6213-4b7e-81e2-5443fb7cd375",
            "resource_id": "a514049e-6213-4b7e-81e2-5443fb7cd375",
            "destination": f"{DataSourceConfig.base_tmp_folder}/aides_ademe/aides-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}aides_ademe/latest/aides_ademe.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}aides_ademe/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/aides_ademe/aides_ademe.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS aides_ademe
        (
            siren TEXT PRIMARY KEY,
            aide_ademe_renseignee INTEGER
        );
        COMMIT;
    """,
)
