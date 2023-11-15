from airflow.models import Variable

TMP_FOLDER = "/tmp/"
DAG_FOLDER = "dag_datalake_sirene/"
DAG_NAME = "insert-elk-sirene"
DATA_DIR = TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/"
SIRENE_DATABASE_LOCATION = DATA_DIR + "sirene.db"
DIRIG_DATABASE_LOCATION = DATA_DIR + "rne.db"
AIRFLOW_DAG_HOME = "/opt/airflow/dags/"
ELASTIC_BULK_SIZE = 1500

AIRFLOW_URL = Variable.get("AIRFLOW_URL")
AIO_URL = Variable.get("AIO_URL")
COLOR_URL = Variable.get("COLOR_URL")
ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD")
ELASTIC_URL = Variable.get("ELASTIC_URL")
ELASTIC_USER = Variable.get("ELASTIC_USER")
MINIO_BUCKET = Variable.get("MINIO_BUCKET")
MINIO_BUCKET_DATA_PIPELINE = Variable.get("MINIO_BUCKET_DATA_PIPELINE")
MINIO_PASSWORD = Variable.get("MINIO_PASSWORD")
MINIO_URL = Variable.get("MINIO_URL")
MINIO_USER = Variable.get("MINIO_USER")
ENV = Variable.get("ENV")
