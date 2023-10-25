from airflow.models import Variable

# Global
AIRFLOW_DAG_HOME = Variable.get("AIRFLOW_DAG_HOME", "/opt/airflow/dags/")
AIRFLOW_DAG_TMP = Variable.get("AIRFLOW_DAG_TMP", "/tmp/")
AIRFLOW_ENV = Variable.get("AIRFLOW_ENV", "dev")
AIRFLOW_URL = Variable.get("AIRFLOW_URL", "")

# Mattermost
MATTERMOST_DATAGOUV_DATAENG = Variable.get("MATTERMOST_DATAGOUV_DATAENG", "")
MATTERMOST_DATAGOUV_DATAENG_TEST = Variable.get("MATTERMOST_DATAGOUV_DATAENG_TEST", "")

# Minio
MINIO_URL = Variable.get("MINIO_URL", "object.files.data.gouv.fr")
MINIO_BUCKET = Variable.get("MINIO_BUCKET", "")
MINIO_USER = Variable.get("MINIO_USER", "")
MINIO_PASSWORD = Variable.get("MINIO_PASSWORD", "")

# RNE
RNE_FTP_URL = Variable.get("RNE_FTP_URL", "")
