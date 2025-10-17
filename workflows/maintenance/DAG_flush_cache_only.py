from datetime import timedelta

import pendulum
from airflow.sdk import dag

from dag_datalake_sirene.config import (
    EMAIL_LIST,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)
from dag_datalake_sirene.helpers.flush_cache import flush_cache

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    tags=["maintenance", "flush cache only"],
    default_args=default_args,
    schedule="0 23 10 * *",
    start_date=pendulum.today("UTC").add(days=-10),
    dagrun_timeout=timedelta(minutes=5),
    params={},
    catchup=False,
)
def flush_cache_only():
    flush_cache(
        REDIS_HOST,
        REDIS_PORT,
        REDIS_DB,
        REDIS_PASSWORD,
    )


flush_cache_only()
