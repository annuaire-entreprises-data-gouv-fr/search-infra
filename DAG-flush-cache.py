from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dag_datalake_sirene.task_functions import flush_cache

DAG_NAME = "flush-cache"
EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")
REDIS_HOST = "redis"
REDIS_PORT = "6379"
REDIS_DB = "0"
REDIS_PASSWORD = Variable.get("REDIS_PASSWORD")


default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval="0 23 10 * *",
    start_date=days_ago(10),
    dagrun_timeout=timedelta(minutes=60 * 8),
    tags=["flush cache"],
) as dag:
    flush_cache = PythonOperator(
        task_id="flush_cache",
        provide_context=True,
        python_callable=flush_cache,
        op_args=(
            REDIS_HOST,
            REDIS_PORT,
            REDIS_DB,
            REDIS_PASSWORD,
        ),
    )

    success_email_body = f"""
    Hi, <br><br>
    Flush cache ***{ENV}*** DAG has been executed successfully at {datetime.now()}.
    """

    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_LIST,
        subject=f"Airflow Success: DAG-{ENV}!",
        html_content=success_email_body,
        dag=dag,
    )

    send_email.set_upstream(flush_cache)
