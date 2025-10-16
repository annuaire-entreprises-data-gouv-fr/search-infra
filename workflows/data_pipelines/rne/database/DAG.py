from datetime import datetime, timedelta

from airflow.sdk import dag, task

from data_pipelines_annuaire.config import EMAIL_LIST, RNE_DB_TMP_FOLDER
from data_pipelines_annuaire.workflows.data_pipelines.rne.database.task_functions import (
    check_db_count,
    create_db,
    get_latest_db,
    process_flux_json_files,
    process_stock_json_files,
    remove_duplicates,
    upload_db_to_object_storage,
    upload_latest_date_rne_object_storage,
    get_start_date,
    send_notification_mattermost,
)

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    tags=["rne", "database"],
    default_args=default_args,
    schedule="0 2 * * *",  # Run daily at 2 am
    start_date=datetime(2023, 10, 11),
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 200)),
    params={},
    catchup=False,
)
def fill_rne_database():
    @task.bash
    def clean_outputs() -> str:
        return f"rm -rf {RNE_DB_TMP_FOLDER} && mkdir -p {RNE_DB_TMP_FOLDER}"

    return (
        clean_outputs()
        >> get_start_date()
        >> create_db()
        >> get_latest_db()
        >> process_stock_json_files()
        >> process_flux_json_files()
        >> remove_duplicates()
        >> check_db_count()
        >> upload_db_to_object_storage()
        >> upload_latest_date_rne_object_storage()
        >> clean_outputs()
        >> send_notification_mattermost()
    )


fill_rne_database()
