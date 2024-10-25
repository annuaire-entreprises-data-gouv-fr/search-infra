from dataclasses import dataclass
from airflow.models import Variable

BASE_TMP_FOLDER = "/tmp"

DATA_GOUV_BASE_URL = "https://www.data.gouv.fr/fr/datasets/r/"
MINIO_BASE_URL = "https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/"


AIRFLOW_ENV = Variable.get("ENV", "dev")


@dataclass
class DataSourceConfig:
    name: str
    tmp_folder: str
    minio_path: str
    file_name: str
    resource_id: str | None = None
    url: str | None = None
    url_minio: str | None = None
    url_minio_metadata: str | None = None


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


DATA_SOURCES = {
    "egapro": EGAPRO_CONFIG,
}


def get_data_source_config(name: str) -> DataSourceConfig:
    return DATA_SOURCES[name]
