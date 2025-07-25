from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

BILAN_GES_CONFIG = DataSourceConfig(
    name="bilan_ges",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/bilan_ges",
    minio_path="bilan_ges",
    file_name="bilan_ges",
    files_to_download={
        "bilan_ges": {
            "url": f"{DATA_GOUV_BASE_URL}62a33491-61c2-473c-89af-7ccab392e50b",
            "resource_id": "62a33491-61c2-473c-89af-7ccab392e50b",
        }
    },
    url_minio=f"{MINIO_BASE_URL}bilan_ges/latest/bilan_ges.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}bilan_ges/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS bilan_ges
        (
            siren TEXT PRIMARY KEY,
            bilan_ges_renseigne INTEGER
        );
        COMMIT;
    """,
)
