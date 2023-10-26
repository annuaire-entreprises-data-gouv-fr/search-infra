from airflow.models import Variable
import json

# Global
AIRFLOW_DAG_HOME = Variable.get("AIRFLOW_DAG_HOME", "/opt/airflow/dags/")
AIRFLOW_DAG_TMP = Variable.get("AIRFLOW_DAG_TMP", "/tmp/")
AIRFLOW_ENV = Variable.get("AIRFLOW_ENV", "dev")
AIRFLOW_URL = Variable.get("AIRFLOW_URL", "")

# Notification
TCHAP_ANNUAIRE_WEBHOOK = Variable.get("TCHAP_ANNUAIRE_WEBHOOK", "")
TCHA_ANNUAIRE_ROOM_ID = Variable.get("TCHA_ANNUAIRE_ROOM_ID", "")
EMAIL_LIST = Variable.get("EMAIl_LIST", "")

# Minio
MINIO_URL = Variable.get("MINIO_URL", "object.files.data.gouv.fr")
MINIO_BUCKET = Variable.get("MINIO_BUCKET", "")
MINIO_USER = Variable.get("MINIO_USER", "")
MINIO_PASSWORD = Variable.get("MINIO_PASSWORD", "")

# RNE
RNE_FTP_URL = Variable.get("RNE_FTP_URL", "")
RNE_AUTH = json.loads(Variable.get("RNE_AUTH", "[]"))
