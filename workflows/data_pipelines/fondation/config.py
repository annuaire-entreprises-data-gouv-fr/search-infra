from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

FONDATION_CONFIG = DataSourceConfig(
    name="fondation",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/fondation",
    object_storage_path="fondation",
    file_name="fonadtion",
    files_to_download={
        "fondation": {
            "url": f"{DATA_GOUV_BASE_URL}d434859f-8d3b-4381-bcdb-ec9200653ae6",
            "resource_id": "d434859f-8d3b-4381-bcdb-ec9200653ae6",
            "destination": f"{DataSourceConfig.base_tmp_folder}/fondation/opendata_fondations.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}fondation/opendata_fondations.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}fondation/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS fondation
        (
            siren TEXT,
            numero_rnf TEXT,
            type_organisme TEXT,
            est_fondation INTEGER
        );
        COMMIT;
    """,
)
