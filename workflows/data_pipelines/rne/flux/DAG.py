from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dag_datalake_sirene.config import EMAIL_LIST, RNE_FLUX_TMP_FOLDER
from dag_datalake_sirene.workflows.data_pipelines.rne.flux.flux_tasks import (
    get_every_day_flux,
    send_notification_failure_mattermost,
    send_notification_success_mattermost,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="get_flux_rne",
    default_args=default_args,
    start_date=datetime(2023, 10, 18),
    schedule="0 1 * * *",  # Run every day at 1 AM
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(days=30),
    on_failure_callback=send_notification_failure_mattermost,
    tags=["rne", "flux"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {RNE_FLUX_TMP_FOLDER} && mkdir -p {RNE_FLUX_TMP_FOLDER}",
    )

    get_daily_flux_rne = PythonOperator(
        task_id="get_every_day_flux",
        python_callable=get_every_day_flux,
    )

    clean_outputs = BashOperator(
        task_id="clean_outputs",
        bash_command=f"rm -rf {RNE_FLUX_TMP_FOLDER}",
    )

    send_notification_success_mattermost = PythonOperator(
        task_id="send_notification_success_mattermost",
        python_callable=send_notification_success_mattermost,
    )

    get_daily_flux_rne.set_upstream(clean_previous_outputs)
    clean_outputs.set_upstream(get_daily_flux_rne)
    send_notification_success_mattermost.set_upstream(clean_outputs)
