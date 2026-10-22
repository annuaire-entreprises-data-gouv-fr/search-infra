from data_pipelines_annuaire.config import (
    OBJECT_STORAGE_BASE_URL,
    DataSourceConfig,
)

SIREN_PROTEGE_CONFIG = DataSourceConfig(
    name="siren_protege",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/siren_protege",
    object_storage_path="siren_protege",
    file_name="siren_protege",
    url_object_storage=f"{OBJECT_STORAGE_BASE_URL}siren_protege/latest/siren_protege.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS siren_protege
        (
            siren TEXT PRIMARY KEY
        );
        COMMIT;
    """,
)
