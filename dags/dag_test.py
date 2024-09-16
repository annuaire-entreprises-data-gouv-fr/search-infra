import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator



with DAG(
    dag_id="test_dag",
    start_date=datetime.datetime(2024, 4, 24, hour=1),
    schedule="@hourly",
    catchup=False,
):
    EmptyOperator(task_id="empty_operator")
