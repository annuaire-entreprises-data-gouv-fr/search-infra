from airflow.models import Variable
import json
from dataclasses import dataclass


@dataclass
class DataSourceConfig:
    ####################
    # DAG placeholders #

    name: str
    tmp_folder: str
    minio_path: str
    file_name: str
    resource_id: str | None = None
    url: str | None = None
    url_minio: str | None = None
    url_minio_metadata: str | None = None
    file_download_destination: str | None = None
    file_output: str | None = None
    base_tmp_folder: str = "/tmp"  # Default value


AIRFLOW_ENV = Variable.get("ENV", "dev")
BASE_TMP_FOLDER = "/tmp"
DATA_GOUV_BASE_URL = "https://www.data.gouv.fr/fr/datasets/r/"
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
RNE_DATABASE_LOCATION = AIRFLOW_ETL_DATA_DIR + "rne.db"
RNE_DB_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/database/"
RNE_MINIO_DATA_PATH = "rne/database/"
RNE_LATEST_DATE_FILE = "latest_rne_date.json"
RNE_MINIO_FLUX_DATA_PATH = "rne/flux/"
RNE_MINIO_STOCK_DATA_PATH = "rne/stock/"
RNE_FLUX_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/flux/"
RNE_FLUX_DATADIR = f"{RNE_FLUX_TMP_FOLDER}data"
RNE_DEFAULT_START_DATE = "2024-09-03"
RNE_STOCK_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/stock/"
RNE_STOCK_ZIP_FILE_PATH = f"{RNE_STOCK_TMP_FOLDER}stock_rne.zip"
RNE_STOCK_EXTRACTED_FILES_PATH = f"{RNE_STOCK_TMP_FOLDER}extracted/"
RNE_STOCK_DATADIR = f"{RNE_STOCK_TMP_FOLDER}data"
RNE_DAG_FOLDER = "dag_datalake_sirene/workflows/data_pipelines/"
METADATA_CC_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metadata/cc/"
METADATA_CC_MINIO_PATH = "metadata/cc/"
INSEE_FLUX_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene/flux/"
INSEE_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene/ul/"
MARCHE_INCLUSION_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}marche_inclusion/"
AGENCE_BIO_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}agence_bio/"
UAI_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}uai/"
BILANS_FINANCIERS_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}bilans_financiers/"
FORMATION_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}formation/"
COLTER_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}colter/"
CC_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}convention_collective/"
MINIO_DATA_SOURCE_UPDATE_DATES_FILE = "data_source_updates.json"

# Insee
INSEE_SECRET_BEARER = Variable.get("SECRET_BEARER_INSEE", None)
INSEE_API_URL = "https://api.insee.fr/api-sirene/3.11/"

# Notification
TCHAP_ANNUAIRE_WEBHOOK = Variable.get("TCHAP_ANNUAIRE_WEBHOOK", "")
TCHAP_ANNUAIRE_ROOM_ID = Variable.get("TCHAP_ANNUAIRE_ROOM_ID", "")
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

# MARCHE INCLUSION
MARCHE_INCLUSION_API_URL = "https://lemarche.inclusion.beta.gouv.fr/api/siae/?"
SECRET_TOKEN_MARCHE_INCLUSION = Variable.get("SECRET_TOKEN_MARCHE_INCLUSION", "")
URL_MINIO_MARCHE_INCLUSION = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/marche_inclusion"
    "/stock_marche_inclusion.csv"
)
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
# comma separated URL
ELASTIC_DOWNSTREAM_URLS = Variable.get("ELASTIC_DOWNSTREAM_URLS", "")
ELASTIC_DOWNSTREAM_USER = Variable.get("ELASTIC_DOWNSTREAM_USER", "")
ELASTIC_DOWNSTREAM_PASSWORD = Variable.get("ELASTIC_DOWNSTREAM_PASSWORD", "")

API_URL = Variable.get("API_URL", "")
API_IS_REMOTE = Variable.get("API_IS_REMOTE", "False").lower() not in ["false", "0"]

# Datasets
URL_MINIO_AGENCE_BIO = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/agence_bio"
    "/latest/agence_bio_certifications.csv"
)
URL_MINIO_AGENCE_BIO_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/agence_bio"
    "/latest/metadata.json"
)
RESOURCE_ID_BILANS_FINANCIERS = "9d213815-1649-4527-9eb4-427146ef2e5b"
URL_MINIO_BILANS_FINANCIERS = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/bilans_financiers"
    "/latest/synthese_bilans.csv"
)
URL_MINIO_BILANS_FINANCIERS_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/bilans_financiers"
    "/latest/metadata.json"
)
RESOURCE_ID_COLTER_REGIONS = "619ee62e-8f9e-4c62-b166-abc6f2b86201"
URL_COLTER_REGIONS = (
    f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_COLTER_REGIONS}"
)
RESOURCE_ID_COLTER_DEP = "2f4f901d-e3ce-4760-b122-56a311340fc4"
URL_COLTER_DEP = f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_COLTER_DEP}"
RESOURCE_ID_COLTER_COMMUNES = "42b16d68-958e-4518-8551-93e095fe8fda"
URL_COLTER_COMMUNES = (
    f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_COLTER_COMMUNES}"
)
RESOURCE_ID_ELUS_EPCI = "41d95d7d-b172-4636-ac44-32656367cdc7"
URL_ELUS_EPCI = f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_ELUS_EPCI}"
RESOURCE_ID_CONSEILLERS_REGIONAUX = "430e13f9-834b-4411-a1a8-da0b4b6e715c"
URL_CONSEILLERS_REGIONAUX = (
    f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_CONSEILLERS_REGIONAUX}"
)
RESOURCE_ID_CONSEILLERS_DEPARTEMENTAUX = "601ef073-d986-4582-8e1a-ed14dc857fba"
URL_CONSEILLERS_DEPARTEMENTAUX = (
    f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_CONSEILLERS_DEPARTEMENTAUX}"
)
RESOURCE_ID_CONSEILLERS_MUNICIPAUX = "d5f400de-ae3f-4966-8cb6-a85c70c6c24a"
URL_CONSEILLERS_MUNICIPAUX = (
    f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_CONSEILLERS_MUNICIPAUX}"
)
RESOURCE_ID_ASSEMBLEE_COL_STATUT_PARTICULIER = "a595be27-cfab-4810-b9d4-22e193bffe35"
URL_ASSEMBLEE_COL_STATUT_PARTICULIER = (
    "https://www.data.gouv.fr/fr/datasets/"
    f"r/{RESOURCE_ID_ASSEMBLEE_COL_STATUT_PARTICULIER}"
)
URL_MINIO_COLTER = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/colter"
    "/latest/colter.csv"
)
URL_MINIO_COLTER_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/colter"
    "/latest/metadata.json"
)
URL_MINIO_ELUS = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/colter"
    "/latest/elus.csv"
)
RESOURCE_ID_CONVENTION_COLLECTIVE = "a22e54f7-b937-4483-9a72-aad2ea1316f1"
URL_CONVENTION_COLLECTIVE = (
    f"https://www.data.gouv.fr/fr/datasets/r/{RESOURCE_ID_CONVENTION_COLLECTIVE}"
)
URL_MINIO_CONVENTION_COLLECTIVE = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/convention_collective"
    "/latest/cc.csv"
)
URL_MINIO_CONVENTION_COLLECTIVE_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/convention_collective"
    "/latest/metadata.json"
)
URL_ETABLISSEMENTS = "https://files.data.gouv.fr/geo-sirene/last/dep/geo_siret"
URL_MINIO_ETABLISSEMENTS = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee"
    "/sirene/stock/StockEtablissement_utf8.zip"
)
URL_ETABLISSEMENTS_HISTORIQUE = (
    "https://www.data.gouv.fr/fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32"
)
URL_MINIO_ETABLISSEMENTS_HISTORIQUE = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee"
    "/sirene/historique/StockEtablissementHistorique_utf8.zip"
)
URL_ORGANISME_FORMATION = (
    "https://dgefp.opendatasoft.com/api/explore/v2.1/catalog/datasets/liste"
    "-publique-des-of-v2/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels"
    "=true&delimiter=%3B"
)
URL_MINIO_ORGANISME_FORMATION = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/formation"
    "/latest/formation.csv"
)
URL_MINIO_ORGANISME_FORMATION_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/formation"
    "/latest/metadata.json"
)

# Ministère de l'Education Nationale et de la Jeunesse
RESOURCE_ID_UAI_MENJ = "85aefd85-3025-400f-90ff-ccfd17ca588e"
# Ministère de l'Enseignement Supérieur er de la Recherche
RESOURCE_ID_UAI_MESR = "bcc3229a-beb2-4077-a8d8-50a065dfbbfa"
# Idéo-Structures d'enseignement supérieur
DATASET_ID_UAI_ONISEP = "5fa5e386afdaa6152360f323"
URL_MINIO_UAI = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/uai"
    "/latest/annuaire_uai.csv"
)
URL_MINIO_UAI_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/uai"
    "/latest/metadata.json"
)

URL_UNITE_LEGALE = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"
URL_MINIO_UNITE_LEGALE = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee"
    "/sirene/stock/StockUniteLegale_utf8.zip"
)
URL_UNITE_LEGALE_HISTORIQUE = (
    "https://www.data.gouv.fr/fr/datasets/r/0835cd60-2c2a-497b-bc64-404de704ce89"
)
URL_MINIO_UNITE_LEGALE_HISTORIQUE = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee"
    "/sirene/historique/StockUniteLegaleHistorique_utf8.zip"
)
URL_CC_DARES = "https://travail-emploi.gouv.fr/sites/travail-emploi/files"
# Caution: DARES file is case sensitive or returns 404
FILE_CC_DATE = "Dares_donnes_Identifiant_convention_collective_"
URL_CC_KALI = (
    "https://www.data.gouv.fr/fr/datasets/r/02b67492-5243-44e8-8dd1-0cb3f90f35ff"
)
URL_MINIO_SIRENE_METADATA = (
    f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee/sirene/"
    "flux/metadata.json"
)

# DataGouv
DATAGOUV_URL = "https://www.data.gouv.fr"
DATAGOUV_RESOURCE_PATH = "/fr/datasets/r/"
ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"
DATAGOUV_SECRET_API_KEY = Variable.get("DATAGOUV_SECRET_API_KEY", "")
