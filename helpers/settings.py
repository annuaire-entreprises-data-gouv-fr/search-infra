import os
from typing import Dict, List
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

class RneAuth(BaseModel):
    username: str
    password: str

class EnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    # Minio configuration
    MINIO_PORT: int = 9000
    MINIO_CONSOLE_PORT: int = 9001
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_BUCKET: str = "mybucket"
    MINIO_URL: str = "localhost"
    MINIO_USER: str = "minioadmin"
    MINIO_PASSWORD: str = "minioadmin"

    # Airflow configuration
    AIRFLOW_DAG_HOME: str = "/opt/airflow/dags/"
    AIRFLOW_DAG_TMP: str = "/opt/airflow/data/tmp/"
    ENV: str = "dev"
    AIRFLOW_URL: str = ""

    # Insee configuration
    SECRET_BEARER_INSEE: str = ""

    # Notification configuration
    TCHAP_ANNUAIRE_WEBHOOK: str = ""
    TCHAP_ANNUAIRE_ROOM_ID: str = ""
    EMAIL_LIST: str = ""

    # RNE configuration
    RNE_FTP_URL: str = ""
    RNE_AUTH: List[Dict[str, str]] = []
    RNE_DEFAULT_START_DATE: str

    # Marche Inclusion configuration
    SECRET_TOKEN_MARCHE_INCLUSION: str = ""

    # Redis configuration
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = "TOTO"

    # ElasticSearch configuration
    ELASTIC_PASSWORD: str = ""
    ELASTIC_URL: str = ""
    ELASTIC_USER: str = ""
    ELASTIC_BULK_THREAD_COUNT: int = 4
    ELASTIC_BULK_SIZE: int = 1500
    ELASTIC_MAX_LIVE_VERSIONS: int = 2
    ELASTIC_SNAPSHOT_REPOSITORY: str = "data-prod"
    ELASTIC_SNAPSHOT_MINIO_STATE_PATH: str = "elastic_index_version"
    ELASTIC_DOWNSTREAM_ALIAS: str = "siren-reader"
    ELASTIC_DOWNSTREAM_URLS: str = ""
    ELASTIC_DOWNSTREAM_USER: str = ""
    ELASTIC_DOWNSTREAM_PASSWORD: str = ""

    # API configuration
    API_URL: str = ""
    API_IS_REMOTE: bool = False

    # DataGouv configuration
    DATAGOUV_SECRET_API_KEY: str = ""

    # New environment variables
    POSTGRES_USER: str = "airflow"
    POSTGRES_PASSWORD: str = "airflow"
    POSTGRES_DB: str = "airflow"
    POSTGRES_PORT: int = 5432
    POSTGRES_HOST: str = "postgres"

env_settings = EnvSettings()

class Settings:
    # Minio configuration
    MINIO_PORT: int = env_settings.MINIO_PORT
    MINIO_CONSOLE_PORT: int = env_settings.MINIO_CONSOLE_PORT
    MINIO_ROOT_USER: str = env_settings.MINIO_ROOT_USER
    MINIO_ROOT_PASSWORD: str = env_settings.MINIO_ROOT_PASSWORD
    MINIO_BUCKET: str = env_settings.MINIO_BUCKET
    MINIO_URL: str = env_settings.MINIO_URL
    MINIO_USER: str = env_settings.MINIO_USER
    MINIO_PASSWORD: str = env_settings.MINIO_PASSWORD

    # Airflow configuration
    AIRFLOW_DAG_HOME: str = env_settings.AIRFLOW_DAG_HOME
    AIRFLOW_DAG_TMP: str = env_settings.AIRFLOW_DAG_TMP
    AIRFLOW_DAG_FOLDER: str = "dag_datalake_sirene/"
    AIRFLOW_ETL_DAG_NAME: str = "extract_transform_load_db"
    AIRFLOW_ELK_DAG_NAME: str = "index_elasticsearch"
    AIRFLOW_SNAPSHOT_DAG_NAME: str = "snapshot_index"
    AIRFLOW_SNAPSHOT_ROLLBACK_DAG_NAME: str = "snapshot_index_rollback"
    AIRFLOW_PUBLISH_DAG_NAME: str = "publish_data_gouv"
    AIRFLOW_ENV: str = env_settings.ENV
    AIRFLOW_URL: str = env_settings.AIRFLOW_URL
    AIRFLOW_ETL_DATA_DIR: str = f"{AIRFLOW_DAG_TMP}{AIRFLOW_DAG_FOLDER}{AIRFLOW_ETL_DAG_NAME}/data/"
    AIRFLOW_ELK_DATA_DIR: str = f"{AIRFLOW_DAG_TMP}{AIRFLOW_DAG_FOLDER}{AIRFLOW_ELK_DAG_NAME}/data/"
    AIRFLOW_DATAGOUV_DATA_DIR: str = f"{AIRFLOW_DAG_TMP}{AIRFLOW_PUBLISH_DAG_NAME}/data/"
    SIRENE_DATABASE_LOCATION: str = f"{AIRFLOW_ETL_DATA_DIR}sirene.db"
    SIRENE_MINIO_DATA_PATH: str = "sirene/database/"
    RNE_DATABASE_LOCATION: str = f"{AIRFLOW_ETL_DATA_DIR}rne.db"
    RNE_DB_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}rne/database/"
    RNE_MINIO_DATA_PATH: str = "rne/database/"
    RNE_LATEST_DATE_FILE: str = "latest_rne_date.json"
    RNE_MINIO_FLUX_DATA_PATH: str = "rne/flux/"
    RNE_MINIO_STOCK_DATA_PATH: str = f"{AIRFLOW_DAG_TMP}rne/stock/"
    RNE_FLUX_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}rne/flux/"
    RNE_FLUX_DATADIR: str = f"{RNE_FLUX_TMP_FOLDER}data"
    RNE_DEFAULT_START_DATE: str = env_settings.RNE_DEFAULT_START_DATE
    RNE_STOCK_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}rne/stock/"
    RNE_STOCK_ZIP_FILE_PATH: str = f"{RNE_STOCK_TMP_FOLDER}stock_rne.zip"
    RNE_STOCK_EXTRACTED_FILES_PATH: str = f"{RNE_STOCK_TMP_FOLDER}extracted/"
    RNE_DAG_FOLDER: str = "/opt/airflow/workflows/data_pipelines/"
    METADATA_CC_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}metadata/cc/"
    METADATA_CC_MINIO_PATH: str = "metadata/cc/"
    INSEE_FLUX_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}sirene/flux/"
    INSEE_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}sirene/ul/"
    MARCHE_INCLUSION_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}marche_inclusion/"
    AGENCE_BIO_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}agence_bio/"
    EGAPRO_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}egapro/"
    UAI_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}uai/"
    BILANS_FINANCIERS_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}bilans_financiers/"
    SPECTACLE_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}spectacle/"
    FINESS_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}finess/"
    RGE_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}rge/"
    FORMATION_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}formation/"
    ESS_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}ess/"
    COLTER_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}colter/"
    CC_TMP_FOLDER: str = f"{AIRFLOW_DAG_TMP}convention_collective/"

    # Insee configuration
    INSEE_SECRET_BEARER: str = env_settings.SECRET_BEARER_INSEE
    INSEE_API_URL: str = "https://api.insee.fr/entreprises/sirene/V3.11/"

    # Notification configuration
    TCHAP_ANNUAIRE_WEBHOOK: str = env_settings.TCHAP_ANNUAIRE_WEBHOOK
    TCHAP_ANNUAIRE_ROOM_ID: str = env_settings.TCHAP_ANNUAIRE_ROOM_ID
    EMAIL_LIST: str = env_settings.EMAIL_LIST

    # Minio configuration
    MINIO_URL: str = env_settings.MINIO_URL
    MINIO_BUCKET: str = env_settings.MINIO_BUCKET
    MINIO_USER: str = env_settings.MINIO_USER
    MINIO_PASSWORD: str = env_settings.MINIO_PASSWORD

    # RNE configuration
    RNE_FTP_URL: str = env_settings.RNE_FTP_URL
    RNE_AUTH: list = [i for i in env_settings.RNE_AUTH]
    RNE_API_TOKEN_URL: str = "https://registre-national-entreprises.inpi.fr/api/sso/login"
    RNE_API_DIFF_URL: str = "https://registre-national-entreprises.inpi.fr/api/companies/diff?"

    # MARCHE INCLUSION configuration
    MARCHE_INCLUSION_API_URL: str = "https://lemarche.inclusion.beta.gouv.fr/api/siae/?"
    SECRET_TOKEN_MARCHE_INCLUSION: str = env_settings.SECRET_TOKEN_MARCHE_INCLUSION
    URL_MINIO_MARCHE_INCLUSION: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/marche_inclusion/stock_marche_inclusion.csv"


    # Redis configuration
    REDIS_HOST: str = env_settings.REDIS_HOST
    REDIS_PORT: str = env_settings.REDIS_PORT
    REDIS_DB: str = env_settings.REDIS_DB
    REDIS_PASSWORD: str = env_settings.REDIS_PASSWORD

    # ElasticSearch configuration
    ELASTIC_PASSWORD: str = env_settings.ELASTIC_PASSWORD
    ELASTIC_URL: str = env_settings.ELASTIC_URL
    ELASTIC_USER: str = env_settings.ELASTIC_USER
    ELASTIC_BULK_THREAD_COUNT: int = env_settings.ELASTIC_BULK_THREAD_COUNT
    ELASTIC_BULK_SIZE: int = env_settings.ELASTIC_BULK_SIZE
    ELASTIC_SHARDS: int = 2
    ELASTIC_REPLICAS: int = 0
    ELASTIC_MAX_LIVE_VERSIONS: int = env_settings.ELASTIC_MAX_LIVE_VERSIONS
    ELASTIC_SNAPSHOT_REPOSITORY: str = env_settings.ELASTIC_SNAPSHOT_REPOSITORY
    ELASTIC_SNAPSHOT_MAX_REVISIONS: int = 5
    ELASTIC_SNAPSHOT_MINIO_STATE_PATH: str = env_settings.ELASTIC_SNAPSHOT_MINIO_STATE_PATH
    ELASTIC_DOWNSTREAM_ALIAS: str = env_settings.ELASTIC_DOWNSTREAM_ALIAS
    ELASTIC_DOWNSTREAM_URLS: str = env_settings.ELASTIC_DOWNSTREAM_URLS
    ELASTIC_DOWNSTREAM_USER: str = env_settings.ELASTIC_DOWNSTREAM_USER
    ELASTIC_DOWNSTREAM_PASSWORD: str = env_settings.ELASTIC_DOWNSTREAM_PASSWORD

    # API configuration
    API_URL: str = env_settings.API_URL
    API_IS_REMOTE: bool = env_settings.API_IS_REMOTE

    # Datasets URLs
    URL_AGENCE_BIO: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/agence_bio/latest/agence_bio_certifications.csv"
    URL_BILANS_FINANCIERS: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/bilans_financiers/latest/synthese_bilans.csv"
    URL_COLTER_REGIONS: str = "https://www.data.gouv.fr/fr/datasets/r/619ee62e-8f9e-4c62-b166-abc6f2b86201"
    URL_COLTER_DEP: str = "https://www.data.gouv.fr/fr/datasets/r/2f4f901d-e3ce-4760-b122-56a311340fc4"
    URL_COLTER_COMMUNES: str = "https://www.data.gouv.fr/fr/datasets/r/42b16d68-958e-4518-8551-93e095fe8fda"
    URL_ELUS_EPCI: str = "https://www.data.gouv.fr/fr/datasets/r/41d95d7d-b172-4636-ac44-32656367cdc7"
    URL_CONSEILLERS_REGIONAUX: str = "https://www.data.gouv.fr/fr/datasets/r/430e13f9-834b-4411-a1a8-da0b4b6e715c"
    URL_CONSEILLERS_DEPARTEMENTAUX: str = "https://www.data.gouv.fr/fr/datasets/r/601ef073-d986-4582-8e1a-ed14dc857fba"
    URL_CONSEILLERS_MUNICIPAUX: str = "https://www.data.gouv.fr/fr/datasets/r/d5f400de-ae3f-4966-8cb6-a85c70c6c24a"
    URL_ASSEMBLEE_COL_STATUT_PARTICULIER: str = "https://www.data.gouv.fr/fr/datasets/r/a595be27-cfab-4810-b9d4-22e193bffe35"
    URL_MINIO_COLTER: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/colter/latest/colter.csv"
    URL_MINIO_ELUS: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/colter/latest/elus.csv"
    URL_CONVENTION_COLLECTIVE: str = "https://www.data.gouv.fr/fr/datasets/r/a22e54f7-b937-4483-9a72-aad2ea1316f1"
    URL_MINIO_CONVENTION_COLLECTIVE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/convention_collective/latest/cc.csv"
    URL_EGAPRO: str = "https://www.data.gouv.fr/fr/datasets/r/d434859f-8d3b-4381-bcdb-ec9200653ae6"
    URL_MINIO_EGAPRO: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/egapro/latest/egapro.csv"
    URL_ENTREPRENEUR_SPECTACLE: str = "https://www.data.gouv.fr/fr/datasets/r/fb6c3b2e-da8c-4e69-a719-6a96329e4cb2"
    URL_MINIO_ENTREPRENEUR_SPECTACLE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/spectacle/latest/spectacle.csv"
    URL_ETABLISSEMENTS: str = "https://files.data.gouv.fr/geo-sirene/last/dep/geo_siret"
    URL_MINIO_ETABLISSEMENTS: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee/sirene/stock/StockEtablissement_utf8.zip"
    URL_ETABLISSEMENTS_HISTORIQUE: str = "https://www.data.gouv.fr/fr/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32"
    URL_MINIO_ETABLISSEMENTS_HISTORIQUE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee/sirene/historique/StockEtablissementHistorique_utf8.zip"
    URL_FINESS: str = "https://www.data.gouv.fr/fr/datasets/r/2ce43ade-8d2c-4d1d-81da-ca06c82abc68"
    URL_MINIO_FINESS: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/finess/latest/finess.csv"
    URL_ORGANISME_FORMATION: str = "https://dgefp.opendatasoft.com/api/explore/v2.1/catalog/datasets/liste-publique-des-of-v2/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    URL_MINIO_ORGANISME_FORMATION: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/formation/latest/formation.csv"
    URL_RGE: str = "https://data.ademe.fr/data-fair/api/v1/datasets/liste-des-entreprises-rge-2/lines?size=10000&select=siret%2Ccode_qualification"
    URL_MINIO_RGE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/rge/latest/rge.csv"
    URL_UAI: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/uai/latest/annuaire_uai.csv"
    URL_UNITE_LEGALE: str = "https://files.data.gouv.fr/insee-sirene/StockUniteLegale_utf8.zip"
    URL_MINIO_UNITE_LEGALE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee/sirene/stock/StockUniteLegale_utf8.zip"
    URL_UNITE_LEGALE_HISTORIQUE: str = "https://www.data.gouv.fr/fr/datasets/r/0835cd60-2c2a-497b-bc64-404de704ce89"
    URL_MINIO_UNITE_LEGALE_HISTORIQUE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/insee/sirene/historique/StockUniteLegaleHistorique_utf8.zip"
    URL_ESS_FRANCE: str = "https://www.data.gouv.fr/fr/datasets/r/57bc99ca-0432-4b46-8fcc-e76a35c9efaf"
    URL_MINIO_ESS_FRANCE: str = f"https://object.files.data.gouv.fr/opendata/ae/{AIRFLOW_ENV}/ess/latest/ess_france.csv"
    URL_CC_DARES: str = "https://travail-emploi.gouv.fr/IMG/xlsx/dares_donnes_identifiant_convention_collective_"
    URL_CC_KALI: str = "https://www.data.gouv.fr/fr/datasets/r/02b67492-5243-44e8-8dd1-0cb3f90f35ff"

    # DataGouv configuration
    DATAGOUV_URL: str = "https://www.data.gouv.fr"
    ORGA_REFERENCE: str = "646b7187b50b2a93b1ae3d45"
    DATAGOUV_SECRET_API_KEY: str = env_settings.DATAGOUV_SECRET_API_KEY



def create_folders():
    # loop to create all folders
    folders = [
        Settings.AIRFLOW_DAG_TMP,
        Settings.RNE_DB_TMP_FOLDER,
        Settings.RNE_FLUX_TMP_FOLDER,
        Settings.RNE_STOCK_TMP_FOLDER,
        Settings.METADATA_CC_TMP_FOLDER,
        Settings.INSEE_FLUX_TMP_FOLDER,
        Settings.UAI_TMP_FOLDER,
        Settings.BILANS_FINANCIERS_TMP_FOLDER,
        Settings.SPECTACLE_TMP_FOLDER,
        Settings.FINESS_TMP_FOLDER,
        Settings.RGE_TMP_FOLDER,
        Settings.FORMATION_TMP_FOLDER,
        Settings.ESS_TMP_FOLDER,
        Settings.COLTER_TMP_FOLDER,
        Settings.CC_TMP_FOLDER,
        Settings.MARCHE_INCLUSION_TMP_FOLDER,
        Settings.AGENCE_BIO_TMP_FOLDER,
        Settings.EGAPRO_TMP_FOLDER,
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)


create_folders()
