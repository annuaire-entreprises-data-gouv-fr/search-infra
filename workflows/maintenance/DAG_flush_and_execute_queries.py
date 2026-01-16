from datetime import timedelta

import pendulum
from airflow.sdk import dag

from data_pipelines_annuaire.config import (
    EMAIL_LIST,
    REDIS_DB,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)
from data_pipelines_annuaire.helpers.execute_slow_queries import (
    execute_slow_requests,
)
from data_pipelines_annuaire.helpers.flush_cache import flush_redis_cache

default_args = {
    "depends_on_past": False,
    "email": EMAIL_LIST,
    "email_on_failure": True,
}


@dag(
    tags=["maintenance", "flush cache and execute queries"],
    default_args=default_args,
    schedule="0 23 10 * *",
    start_date=pendulum.today("UTC").add(days=-10),
    dagrun_timeout=timedelta(minutes=10),
    params={},
    catchup=False,
)
def flush_cache_and_execute_queries():
    return (
        flush_redis_cache(REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD)
        >> execute_slow_requests()
    )


flush_cache_and_execute_queries()
