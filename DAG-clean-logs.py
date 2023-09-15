from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import DAG, Variable
from datetime import datetime, timedelta
import os
import logging
import shutil


EMAIL_LIST = Variable.get("EMAIL_LIST")
ENV = Variable.get("ENV")

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now() - timedelta(days=1),  # Set the start_date to yesterday
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# Define the Python function to delete old logs and directories
def delete_old_logs_and_directories():
    log_dir = "/opt/airflow/logs"
    cutoff_date = datetime.now() - timedelta(days=15)

    # Check if the directory exists
    if os.path.exists(log_dir):
        for root, dirs, files in os.walk(log_dir, topdown=False):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                # Check if the directory modification time is older than the cutoff date
                if os.path.getmtime(dir_path) < cutoff_date.timestamp():
                    # Recursively remove directory and its contents
                    shutil.rmtree(dir_path)
                    logging.info(f"Deleted directory: {dir_path}")
            for file in files:
                file_path = os.path.join(root, file)
                # Check if the file modification time is older than the cutoff date
                if os.path.getmtime(file_path) < cutoff_date.timestamp():
                    os.remove(file_path)
                    logging.info(f"Deleted file: {file_path}")
    else:
        logging.error(f"Log directory not found: {log_dir}")


# Define the DAG
with DAG(
    "delete_airflow_logs",
    default_args=default_args,
    description="Delete Airflow logs older than 15 days",
    schedule_interval="0 0 * * 1",  # run every Monday at midnight (UTC)
    start_date=datetime(2023, 9, 15),
    catchup=False,  # False to ignore past runs
    max_active_runs=1,  # Allow only one execution at a time
) as dag:
    # Create a PythonOperator to execute the cleanup function
    delete_logs_task = PythonOperator(
        task_id="delete_logs",
        python_callable=delete_old_logs_and_directories,
        dag=dag,
    )

    success_email_body = f"""
    Hi, <br><br>
    delete-logs-{ENV} DAG has been executed successfully at {datetime.now()}.
    """

    send_email = EmailOperator(
        task_id="send_email",
        to=EMAIL_LIST,
        subject=f"Airflow Success: DAG-{ENV}!",
        html_content=success_email_body,
        dag=dag,
    )

    # Set the task dependency
    send_email.set_upstream(delete_logs_task)
