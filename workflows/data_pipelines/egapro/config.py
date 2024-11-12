from dag_datalake_sirene.config import (
    DataSourceConfig,
    BASE_TMP_FOLDER,
    DATA_GOUV_BASE_URL,
    MINIO_BASE_URL,
)


EGAPRO_CONFIG = DataSourceConfig(
    name="egapro",
    tmp_folder=f"{BASE_TMP_FOLDER}/egapro",
    minio_path="egapro",
    file_name="egapro",
    resource_id="5b191d0f-b933-40d1-a1a4-9743c9727059",
    url=f"{DATA_GOUV_BASE_URL}5b191d0f-b933-40d1-a1a4-9743c9727059",
    url_minio=f"{MINIO_BASE_URL}egapro/latest/egapro.csv",
    url_minio_metadata=f"{MINIO_BASE_URL}egapro/latest/metadata.json",
)
