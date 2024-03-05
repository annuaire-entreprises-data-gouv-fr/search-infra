from airflow.models import Variable
import json

# Airflow
AIRFLOW_DAG_HOME = Variable.get("AIRFLOW_DAG_HOME", "/opt/airflow/dags/")
AIRFLOW_DAG_TMP = Variable.get("AIRFLOW_DAG_TMP", "/tmp/")
AIRFLOW_DAG_FOLDER = "dag_datalake_sirene/"
AIRFLOW_ETL_DAG_NAME = "extract_transform_load_db"
AIRFLOW_ELK_DAG_NAME = "index_elasticsearch"
AIRFLOW_SNAPSHOT_DAG_NAME = "snapshot_index"
AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME = "snapshot_index_rollback"
AIRFLOW_ENV = Variable.get("ENV", "dev")
AIRFLOW_URL = Variable.get("AIRFLOW_URL", "")
AIRFLOW_ETL_DATA_DIR = (
    AIRFLOW_DAG_TMP + AIRFLOW_DAG_FOLDER + AIRFLOW_ETL_DAG_NAME + "/data/"
)
AIRFLOW_ELK_DATA_DIR = (
    AIRFLOW_DAG_TMP + AIRFLOW_DAG_FOLDER + AIRFLOW_ELK_DAG_NAME + "/data/"
)
SIRENE_DATABASE_LOCATION = AIRFLOW_ETL_DATA_DIR + "sirene.db"
SIRENE_MINIO_DATA_PATH = "sirene/database/"
RNE_DATABASE_LOCATION = AIRFLOW_ETL_DATA_DIR + "rne.db"
RNE_DB_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/database/"
RNE_MINIO_DATA_PATH = "rne/database/"
RNE_LATEST_DATE_FILE = "latest_rne_date.json"
RNE_MINIO_FLUX_DATA_PATH = "rne/flux/data/"
RNE_MINIO_STOCK_DATA_PATH = "rne/stock/data/"
RNE_FLUX_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/flux/"
RNE_FLUX_DATADIR = f"{RNE_FLUX_TMP_FOLDER}data"
RNE_DEFAULT_START_DATE = "2023-07-01"
RNE_STOCK_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/stock/"
RNE_STOCK_ZIP_FILE_PATH = f"{RNE_STOCK_TMP_FOLDER}stock_rne.zip"
RNE_STOCK_EXTRACTED_FILES_PATH = f"{RNE_STOCK_TMP_FOLDER}extracted/"
RNE_STOCK_DATADIR = f"{RNE_STOCK_TMP_FOLDER}data"
RNE_DAG_FOLDER = "dag_datalake_sirene/data_pipelines/"
METADATA_CC_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metadata/cc/"
METADATA_CC_MINIO_PATH = "metadata/cc/"
INSEE_FLUX_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene/flux/"
MARCHE_INCLUSION_TMP_FOLDER = f"{AIRFLOW_DAG_TMP}marche_inclusion/"

# Insee
INSEE_SECRET_BEARER = Variable.get("SECRET_BEARER_INSEE", None)
INSEE_API_URL = "https://api.insee.fr/entreprises/sirene/V3/"

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
# AIO
AIO_URL = Variable.get("AIO_URL", None)
COLOR_URL = Variable.get("COLOR_URL", None)
PATH_AIO = Variable.get("PATH_AIO", None)
COLOR_IS_DAILY = bool(Variable.get("COLOR_IS_DAILY", "False"))

# Redis
REDIS_HOST = "redis"
REDIS_PORT = "6379"
REDIS_DB = "0"
REDIS_PASSWORD = Variable.get("REDIS_PASSWORD", None)

# ElasticSearch
ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD", None)
ELASTIC_URL = Variable.get("ELASTIC_URL", None)
ELASTIC_USER = Variable.get("ELASTIC_USER", None)
ELASTIC_BULK_SIZE = 1500
ELASTIC_SHARDS = 2
ELASTIC_REPLICAS = 0

ELASTIC_MAX_LIVE_VERSIONS = int(Variable.get("ELASTIC_MAX_LIVE_VERSIONS", 1))

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
URL_AGENCE_BIO = (
    "https://object.files.data.gouv.fr/data-pipeline-open/prod/agence_bio"
    "/latest/agence_bio_certifications.csv"
)
URL_BILANS_FINANCIERS = (
    "https://object.files.data.gouv.fr/data-pipeline-open/"
    "prod/signaux_faibles/latest/synthese_bilans.csv"
)
URL_COLTER_REGIONS = (
    "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201"
)
URL_COLTER_DEP = (
    "https://www.data.gouv.fr/fr/datasets/r/2f4f901d-e3ce-4760-b122-56a311340fc4"
)
URL_COLTER_COMMUNES = (
    "https://www.data.gouv.fr/fr/datasets/r/42b16d68-958e-4518-8551-93e095fe8fda"
)
URL_ELUS_EPCI = (
    "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7"
)
URL_CONSEILLERS_REGIONAUX = (
    "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c"
)
URL_CONSEILLERS_DEPARTEMENTAUX = (
    "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba"
)
URL_CONSEILLERS_MUNICIPAUX = (
    "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a"
)
URL_ASSEMBLEE_COL_STATUT_PARTICULIER = (
    "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35"
)
URL_CONVENTION_COLLECTIVE = (
    "https://www.data.gouv.fr/fr/datasets/r/a22e54f7-b937-4483-9a72-aad2ea1316f1"
)
URL_EGAPRO = (
    "https://www.data.gouv.fr/fr/datasets/r/d434859f-8d3b-4381-bcdb-ec9200653ae6"
)
URL_ENTREPRENEUR_SPECTACLE = (
    "https://www.data.gouv.fr/fr/datasets/r/fb6c3b2e-da8c-4e69-a719-6a96329e4cb2"
)
URL_ETABLISSEMENTS = "https://files.data.gouv.fr/geo-sirene/last/dep/geo_siret"
URL_ETABLISSEMENTS_HISTORIQUE = (
    "https://www.data.gouv.fr/fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32"
)
URL_FINESS = (
    "https://www.data.gouv.fr/fr/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
)
URL_ORGANISME_FORMATION = (
    "https://dgefp.opendatasoft.com/api/explore/v2.1/catalog/datasets/liste"
    "-publique-des-of-v2/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels"
    "=true&delimiter=%3B"
)
URL_RGE = (
    "https://data.ademe.fr/data-fair/api/v1/datasets/"
    "liste-des-entreprises-rge-2/lines?size=10000&select=siret%2Ccode_qualification"
)
URL_UAI = (
    "https://object.files.data.gouv.fr/data-pipeline-open/"
    "prod/uai/latest/annuaire_uai.csv"
)
URL_UNITE_LEGALE = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"
URL_UNITE_LEGALE_HISTORIQUE = (
    "https://www.data.gouv.fr/fr/datasets/r/0835cd60-2c2a-497b-bc64-404de704ce89"
)

URL_ESS_FRANCE = (
    "https://www.data.gouv.fr/fr/datasets/r/57bc99ca-0432-4b46-8fcc-e76a35c9efaf"
)
URL_CC_DARES = (
    "https://travail-emploi.gouv.fr/IMG/xlsx/"
    "dares_donnes_identifiant_convention_collective_"
)
URL_CC_KALI = (
    "https://www.data.gouv.fr/fr/datasets/r/02b67492-5243-44e8-8dd1-0cb3f90f35ff"
)
