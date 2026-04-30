from datetime import datetime, timedelta

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from airflow.sdk import dag

from data_pipelines_annuaire.config import (
    EMAIL_LIST,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.helpers.flush_cache import flush_redis_cache

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    tags=["maintenance", "flush cache only"],
    default_args=default_args,
    schedule="0 23 10 * *",
    start_date=datetime(2026, 1, 1),
    dagrun_timeout=timedelta(minutes=5),
    params={},
    catchup=False,
    on_failure_callback=[Notification(), SmtpNotifier(to=EMAIL_LIST)],
    on_success_callback=Notification(),
    max_active_runs=1,
)
def flush_cache_only():
    return flush_redis_cache(REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD)


flush_cache_only()
