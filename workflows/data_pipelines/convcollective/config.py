from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

CONVENTION_COLLECTIVE_CONFIG = DataSourceConfig(
    name="convention_collective",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/convention_collective",
    minio_path="convention_collective",
    file_name="convention_collective",
    files_to_download={
        "convention_collective": {
            "url": f"{DATA_GOUV_BASE_URL}a22e54f7-b937-4483-9a72-aad2ea1316f1",
            "resource_id": "a22e54f7-b937-4483-9a72-aad2ea1316f1",
            "destination": f"{DataSourceConfig.base_tmp_folder}/convention_collective/convention_collective-download.csv",
        }
    },
    url_minio=f"{MINIO_BASE_URL}convention_collective/latest/convention_collective.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}convention_collective/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/convention_collective/convention_collective.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS convention_collective
        (
            siren TEXT,
            siret TEXT PRIMARY KEY,
            liste_idcc_etablissement TEXT,
            liste_idcc_unite_legale TEXT,
            sirets_par_idcc TEXT
        );
        CREATE INDEX idx_siren_convention_collective ON convention_collective (siren);
        COMMIT;
    """,
)
