from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

ACHATS_RESPONSABLES_CONFIG = DataSourceConfig(
    name="achats_responsables",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/achats_responsables",
    minio_path="achats_responsables",
    file_name="achats_responsables",
    files_to_download={
        "achats_responsables": {
            "url": f"{DATA_GOUV_BASE_URL}a61ad4c3-5656-480d-b71c-bfc3cc99c9b0",
            "resource_id": "a61ad4c3-5656-480d-b71c-bfc3cc99c9b0",
            "destination": f"{DataSourceConfig.base_tmp_folder}/achats_responsables/achats-responsables-download.csv",
        },
    },
    url_minio=f"{MINIO_BASE_URL}achats_responsables/latest/achats_responsables.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}achats_responsables/latest/metadata.json",
    file_output=f"{DataSourceConfig.base_tmp_folder}/achats_responsables/achats_responsables.csv",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS achats_responsables
        (
            siren PRIMARY KEY,
            perimetre_label TEXT,
            est_achats_responsables INTEGER
        );
        COMMIT;
    """,
)
