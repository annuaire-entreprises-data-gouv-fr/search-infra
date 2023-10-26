from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from dag_datalake_sirene.data_pipelines.rne.flux.flux_tasks import (
    TMP_FOLDER,
    get_every_day_flux,
    send_notification_failure_tchap,
    send_notification_success_tchap,
)


with DAG(
    dag_id="get_flux_rne",
    start_date=datetime(2023, 10, 18),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 20)),
    on_failure_callback=send_notification_failure_tchap,
    tags=["api", "rne", "flux"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    get_daily_flux_rne = PythonOperator(
        task_id="get_every_day_flux",
        python_callable=get_every_day_flux,
    )

    send_notification_success_tchap = PythonOperator(
        task_id="send_notification_success_tchap",
        python_callable=send_notification_success_tchap,
    )

    get_daily_flux_rne.set_upstream(clean_previous_outputs)
    send_notification_success_tchap.set_upstream(get_daily_flux_rne)
