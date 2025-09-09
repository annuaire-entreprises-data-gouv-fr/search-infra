import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from airflow.models import Variable


@dataclass
class DataSourceConfig:
    """
    Configuration for a data source.

    Attributes:
        name (str): Unique name of the data source. Also used as the table name in the database.
        tmp_folder (str): Local path folder for storing temporary and intermediate files.
        minio_path (str): Folder name in MinIO where files are stored.
        file_name (str): Name of the main file associated with the data source.
        files_to_download (dict[str, dict[str, str]]): Information about files to download.
            Keys represent unique file identifiers, and values are dictionaries with details about the files:
            - url (str, None): URL of the file to download.
            - resource_id (str, None): Data.gouv resource ID.
            - dataset_id (str, None): Data.gouv dataset ID (the most recent resource will be downloaded).
            - destination (str, None): Local path where the downloaded file will be saved.
        url_minio (str | None): MinIO URL where the processed file will be stored. Defaults to None.
        url_minio_metadata (str | None): MinIO URL where the metadata file will be stored. Defaults to None.
        file_output (str | None): Local file path of the output file. Defaults to None.
        base_tmp_folder (str, None): Base path for temporary folders. Defaults to "/tmp".
        url_api (str | None): URL of the API to fetch data from. Defaults to None.
        endpoint_api (str | None): Specific endpoint of the API to fetch data from. Defaults to None.
        auth_api (str | None): Authentication credentials for the API. Defaults to None.
        table_ddl (str | None): SQL query to create the database table in the ETL DAG. Defaults to None.
    """

    name: str
    tmp_folder: str
    minio_path: str
    file_name: str | None = None
    files_to_download: dict[str, dict[str, str]] = field(default_factory=dict)
    url_minio: str | None = None
    url_minio_metadata: str | None = None
    file_output: str | None = None
    base_tmp_folder: str = "/tmp"
    url_api: str | None = None
    endpoint_api: str | None = None
    auth_api: str | None = None
    table_ddl: str | None = None


CURRENT_MONTH: str = datetime.now().strftime("%Y-%m")
PREVIOUS_MONTH: str = (datetime.now().replace(day=1) - timedelta(days=1)).strftime(
    "%Y-%m"
)

AIRFLOW_ENV = Variable.get("ENV", "dev")
BASE_TMP_FOLDER = "/tmp"
DATA_GOUV_BASE_URL = "https://www.data.gouv.fr/datasets/r/"
MINIO_BASE_URL = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/"

# Airflow
AIRFLOW_DAG_HOME = Variable.get("AIRFLOW_DAG_HOME", "/opt/airflow/dags/")
AIRFLOW_DAG_TMP = Variable.get("AIRFLOW_DAG_TMP", "/tmp/")
AIRFLOW_DAG_FOLDER = "dag_datalake_sirene/"
AIRFLOW_ETL_DAG_NAME = "extract_transform_load_db"
AIRFLOW_ELK_DAG_NAME = "index_elasticsearch"
AIRFLOW_SNAPSHOT_DAG_NAME = "snapshot_index"
AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME = "snapshot_index_rollback"
AIRFLOW_PUBLISH_DAG_NAME = "publish_data_gouv"
AIRFLOW_URL = Variable.get("AIRFLOW_URL", "")
AIRFLOW_ETL_DATA_DIR = (
    AIRFLOW_DAG_TMP + AIRFLOW_DAG_FOLDER + AIRFLOW_ETL_DAG_NAME + "/data/"
)
AIRFLOW_ELK_DATA_DIR = (
    AIRFLOW_DAG_TMP + AIRFLOW_DAG_FOLDER + AIRFLOW_ELK_DAG_NAME + "/data/"
)
AIRFLOW_DATAGOUV_DATA_DIR = AIRFLOW_DAG_TMP + AIRFLOW_PUBLISH_DAG_NAME + "/data/"
SIRENE_DATABASE_LOCATION = AIRFLOW_ETL_DATA_DIR + "sirene.db"
SIRENE_MINIO_DATA_PATH = "sirene/database/"
DATABASE_VALIDATION_MINIO_PATH = "database/validation/"
RNE_DATABASE_LOCATION = AIRFLOW_ETL_DATA_DIR + "rne.db"
RNE_DB_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/database/"
RNE_MINIO_DATA_PATH = "rne/database/"
RNE_LATEST_DATE_FILE = "latest_rne_date.json"
RNE_MINIO_FLUX_DATA_PATH = "rne/flux/"
RNE_MINIO_STOCK_DATA_PATH = "rne/stock/"
RNE_FLUX_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/flux/"
RNE_FLUX_DATADIR = f"{RNE_FLUX_TMP_FOLDER}data"
RNE_DEFAULT_START_DATE = "2025-04-03"
RNE_STOCK_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/stock/"
RNE_STOCK_ZIP_FILE_PATH = f"{RNE_STOCK_TMP_FOLDER}stock_rne.zip"
RNE_STOCK_EXTRACTED_FILES_PATH = f"{RNE_STOCK_TMP_FOLDER}extracted/"
RNE_STOCK_DATADIR = f"{RNE_STOCK_TMP_FOLDER}data"
RNE_DAG_FOLDER = "dag_datalake_sirene/workflows/data_pipelines/"
METADATA_CC_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metadata/cc/"
METADATA_CC_MINIO_PATH = "metadata/cc/"
INSEE_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene/ul/"
CC_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}convention_collective/"
MINIO_DATA_SOURCE_UPDATE_DATES_FILE = "data_source_updates.json"

# Notification
TCHAP_ANNUAIRE_WEBHOOK = Variable.get("TCHAP_ANNUAIRE_WEBHOOK", "")
TCHAP_ANNUAIRE_ROOM_ID = Variable.get("TCHAP_ANNUAIRE_ROOM_ID", "")
MATTERMOST_WEBHOOK = Variable.get("MATTERMOST_WEBHOOK", "")
EMAIL_LIST = Variable.get("EMAIl_LIST", "")

# Minio
MINIO_URL = Variable.get("MINIO_URL", "object.files.data.gouv.fr")
MINIO_BUCKET = Variable.get("MINIO_BUCKET", "")
MINIO_BUCKET_DATA_PIPELINE = Variable.get("MINIO_BUCKET_DATA_PIPELINE", None)
MINIO_USER = Variable.get("MINIO_USER", "")
MINIO_PASSWORD = Variable.get("MINIO_PASSWORD", "")

# RNE
RNE_FTP_URL = Variable.get("RNE_FTP_URL", "")
RNE_AUTH = json.loads(Variable.get("RNE_AUTH", "[]"))
RNE_API_TOKEN_URL = "https://registre-national-entreprises.inpi.fr/api/sso/login"
RNE_API_DIFF_URL = "https://registre-national-entreprises.inpi.fr/api/companies/diff?"

# AIO
AIO_URL = Variable.get("AIO_URL", None)
COLOR_URL = Variable.get("COLOR_URL", None)
PATH_AIO = Variable.get("PATH_AIO", None)
COLOR_IS_DAILY = bool(Variable.get("COLOR_IS_DAILY", "False"))

# Redis
REDIS_HOST = Variable.get("REDIS_HOST", "redis")
REDIS_PORT = Variable.get("REDIS_PORT", "6379")
REDIS_DB = Variable.get("REDIS_DB", "0")
REDIS_PASSWORD = Variable.get("REDIS_PASSWORD", None)

# ElasticSearch
ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD", None)
ELASTIC_URL = Variable.get("ELASTIC_URL", None)
ELASTIC_USER = Variable.get("ELASTIC_USER", None)
ELASTIC_BULK_THREAD_COUNT = int(Variable.get("ELASTIC_BULK_THREAD_COUNT", 4))
ELASTIC_BULK_SIZE = int(Variable.get("ELASTIC_BULK_SIZE", 1500))
ELASTIC_SHARDS = 2
ELASTIC_REPLICAS = 0

ELASTIC_MAX_LIVE_VERSIONS = int(Variable.get("ELASTIC_MAX_LIVE_VERSIONS", 2))

ELASTIC_SNAPSHOT_REPOSITORY = Variable.get("ELASTIC_SNAPSHOT_REPOSITORY", "data-prod")
ELASTIC_SNAPSHOT_MAX_REVISIONS = 5
ELASTIC_SNAPSHOT_MINIO_STATE_PATH = Variable.get(
    "ELASTIC_SNAPSHOT_MINIO_STATE_PATH", "elastic_index_version"
)

ELASTIC_DOWNSTREAM_ALIAS = Variable.get("ELASTIC_DOWNSTREAM_ALIAS", "siren-reader")
ELASTIC_DOWNSTREAM_ALIAS_NEXT = Variable.get("ELASTIC_DOWNSTREAM_ALIAS_NEXT", "siren-reader-next")
# comma separated URL
ELASTIC_DOWNSTREAM_URLS = Variable.get("ELASTIC_DOWNSTREAM_URLS", "")
ELASTIC_DOWNSTREAM_USER = Variable.get("ELASTIC_DOWNSTREAM_USER", "")
ELASTIC_DOWNSTREAM_PASSWORD = Variable.get("ELASTIC_DOWNSTREAM_PASSWORD", "")

API_URL = Variable.get("API_URL", "")
API_IS_REMOTE = Variable.get("API_IS_REMOTE", "False").lower() not in ["false", "0"]

# Datasets
URL_STOCK_ETABLISSEMENTS = {
    "last": "https://files.data.gouv.fr/geo-sirene/last/dep/geo_siret",
    "current": f"https://files.data.gouv.fr/geo-sirene/{CURRENT_MONTH}/dep/geo_siret",
    "previous": f"https://files.data.gouv.fr/geo-sirene/{PREVIOUS_MONTH}/dep/geo_siret",
}
URL_CC_DARES = "https://travail-emploi.gouv.fr/sites/travail-emploi/files"
# Caution: DARES file is case sensitive or returns 404
FILE_CC_DATE = "Dares_donnes_Identifiant_convention_collective_"
URL_CC_KALI = "https://www.data.gouv.fr/datasets/r/02b67492-5243-44e8-8dd1-0cb3f90f35ff"

# DataGouv
DATAGOUV_URL = "https://www.data.gouv.fr"
DATAGOUV_RESOURCE_PATH = "/datasets/r/"
ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"
DATAGOUV_SECRET_API_KEY = Variable.get("DATAGOUV_SECRET_API_KEY", "")
