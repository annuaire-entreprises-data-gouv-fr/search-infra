from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
    DataSourceConfig,
)

ESS_CONFIG = DataSourceConfig(
    name="ess_france",
    tmp_folder=f"{DataSourceConfig.base_tmp_folder}/ess",
    minio_path="ess",
    file_name="ess",
    files_to_download={
        "ess": {
            "url": f"{DATA_GOUV_BASE_URL}57bc99ca-0432-4b46-8fcc-e76a35c9efaf",
            "resource_id": "57bc99ca-0432-4b46-8fcc-e76a35c9efaf",
        },
    },
    url_minio=f"{MINIO_BASE_URL}ess/latest/ess.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}ess/latest/metadata.json",
    table_ddl="""
        BEGIN;
        CREATE TABLE IF NOT EXISTS ess_france
        (
            siren TEXT PRIMARY KEY,
            est_ess_france INTEGER
        );
        COMMIT;
    """,
)
