from data_pipelines_annuaire.config import (
    DATAGOUV_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

AVOCAT_CONFIG = DataSourceConfig(
    name="avocat",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/avocat",
    object_storage_path="avocat",
    file_name="avocat",
    files_to_download={
        "avocat": {
            "url": f"{DATAGOUV_URL}/api/1/datasets/6357de8624b187e5486cbef3",
            "dataset_id": "6357de8624b187e5486cbef3",
            "destination": f"{DataSourceConfig.base_tmp_folder}/avocat/avocat-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}avocat/latest/avocat.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}avocat/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/avocat/avocat.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS avocat
        (
            siren TEXT PRIMARY KEY,
            est_avocat INTEGER
        );
        COMMIT;
    """,
)
