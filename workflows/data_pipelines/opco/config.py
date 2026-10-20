from data_pipelines_annuaire.config import (
    DATAGOUV_URL,
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

OPCO_CONFIG = DataSourceConfig(
    name="opco",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/opco",
    object_storage_path="opco",
    file_name="opco",
    files_to_download={
        "opco": {
            "url": f"{DATAGOUV_URL}/api/1/datasets/688a210c012cfbf595d7a99a",
            "dataset_id": "688a210c012cfbf595d7a99a",
            "destination": f"{DataSourceConfig.base_tmp_folder}/opco/opco-download.csv",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}opco/latest/opco.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}opco/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/opco/opco.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS opco
        (
            siret TEXT PRIMARY KEY,
            a_opco INTEGER
        );
        COMMIT;
    """,
)
