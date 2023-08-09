import logging

from dag_datalake_sirene.elasticsearch.create_index import ElasticCreateIndex
from dag_datalake_sirene.task_functions.global_variables import (
    ELASTIC_BULK_SIZE,
    ELASTIC_PASSWORD,
    ELASTIC_USER,
    ELASTIC_URL,
)


def create_elastic_index(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = f"siren-{next_color}"
    logging.info(f"******************** Index to create: {elastic_index}")
    create_index = ElasticCreateIndex(
        elastic_url=ELASTIC_URL,
        elastic_index=elastic_index,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
        elastic_bulk_size=ELASTIC_BULK_SIZE,
    )
    create_index.execute()
