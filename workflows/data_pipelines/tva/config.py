from data_pipelines_annuaire.config import (
    DATA_GOUV_BASE_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

TVA_CONFIG = DataSourceConfig(
    name="tva",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/tva",
    object_storage_path="tva",
    file_name="tva",
    files_to_download={
        "tva": {
            "url": f"{DATA_GOUV_BASE_URL}d434859f-8d3b-4381-bcdb-ec9200653ae6",
            "resource_id": "d434859f-8d3b-4381-bcdb-ec9200653ae6",
            "destination": f"{DataSourceConfig.base_tmp_folder}/tva/tva-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}tva/latest/tva.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}tva/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS tva
        (
            siren TEXT PRIMARY KEY,
            liste_tva TEXT
        );
        COMMIT;
    """,
)
