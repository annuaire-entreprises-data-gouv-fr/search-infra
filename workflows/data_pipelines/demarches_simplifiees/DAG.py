from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from config import EMAIL_LIST
from main import run_processor

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="get_dossiers_demarches_simplifiees",
    default_args=default_args,
    start_date=datetime(2023, 10, 18),
    # Is there a reason this is commented?
    # schedule_interval="0 1 * * *",  # Run every day at 1 AM
    catchup=False,
    max_active_runs=1,
    # A 30 days timeout seems far fetched, but it's the default value
    dagrun_timeout=timedelta(days=30),
    tags=["demarches_simplifiees", "dossiers"],
    params={},
) as dag:
    process_task = PythonOperator(
        task_id="process_demarche", python_callable=run_processor
    )
