from datetime import datetime, timedelta

from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import DAG, Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago


DAG_NAME = "restart-api"
EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")
PATH_AIO = Variable.get("PATH_AIO")

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
    tags=["restart-api"],
) as dag:
    restart_aio_container = SSHOperator(
        ssh_conn_id="SERVER",
        task_id="execute_aio_container",
        command=f"cd {PATH_AIO} "
        f"&& docker stop aio"
        f"&& docker-compose -f docker-compose-aio.yml up --build -d --force",
        cmd_timeout=60,
        dag=dag,
    )

    success_email_body = f"""
    Hi, <br><br>
    Restarting API ***{ENV}*** DAG has been executed successfully at
     {datetime.now()}.
    """

    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_LIST,
        subject=f"Airflow Success: DAG-{ENV}!",
        html_content=success_email_body,
        dag=dag,
    )

    send_email.set_upstream(restart_aio_container)
