from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

RGE_CONFIG = DataSourceConfig(
    name="rge",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/rge",
    object_storage_path="rge",
    file_name="rge",
    files_to_download={
        "rge": {
            "url": "https://data.ademe.fr/data-fair/api/v1/datasets/"
            "liste-des-entreprises-rge-2/lines?size=10000&select=siret%2Ccode_qualification",
        }
    },
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}rge/latest/rge.csv",
    url_object_storage_metadata=f"{OBJECT_STORAGE_BASE_URL}rge/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS rge
        (
            siret TEXT PRIMARY KEY,
            liste_rge TEXT
        );
        COMMIT;
    """,
)
