import logging

import pandas as pd
from airflow.models import Variable
from dag_datalake_sirene.data_aggregation.collectivite_territoriale import (
    generate_updates_colter,
    generate_updates_elu,
)
from dag_datalake_sirene.data_aggregation.convention_collective import (
    generate_updates_convcollective,
)
from dag_datalake_sirene.data_aggregation.entrepreneur_spectacle import (
    generate_updates_spectacle,
)
from dag_datalake_sirene.data_aggregation.finess import generate_updates_finess
from dag_datalake_sirene.data_aggregation.rge import generate_updates_rge
from dag_datalake_sirene.data_aggregation.uai import generate_updates_uai
from elasticsearch import helpers
from elasticsearch_dsl import connections

ELASTIC_PASSWORD = Variable.get("ELASTIC_PASSWORD")
ELASTIC_URL = Variable.get("ELASTIC_URL")
ELASTIC_USER = Variable.get("ELASTIC_USER")
ENV = Variable.get("ENV")


def update_elasticsearch_with_new_data(
    type_file,
    new_file,
    error_file,
    select_color,
    **kwargs,
) -> None:
    if select_color == "current":
        color = kwargs["ti"].xcom_pull(key="current_color", task_ids="get_colors")
    if select_color == "next":
        color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")

    df = pd.read_csv(new_file, dtype=str)
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )
    elastic_connection = connections.get_connection()
    list_errors = []
    list_success = []

    if type_file == "rge":
        generations = generate_updates_rge(df, color)
    if type_file == "spectacle":
        generations = generate_updates_spectacle(df, color)
    if type_file == "convcollective":
        generations = generate_updates_convcollective(df, color)
    if type_file == "uai":
        generations = generate_updates_uai(df, color)
    if type_file == "finess":
        generations = generate_updates_finess(df, color)
    if type_file == "colter":
        generations = generate_updates_colter(df, color)
    if type_file == "elu":
        generations = generate_updates_elu(df, color)

    for success, details in helpers.parallel_bulk(
        elastic_connection, generations, chunk_size=500, raise_on_error=False,
            request_timeout=30,
    ):
        if not success:
            list_errors.append(details["update"]["_id"])
        else:
            list_success.append(details["update"]["_id"])

    logging.info("%s siren non trouvé.", str(len(list_errors)))
    logging.info("%s documents indexés", str(len(list_success)))
    logging.info("Extrait %s", ", ".join(list_success[:10]))

    with open("/".join(new_file.split("/")[:-1]) + "/" + error_file, "w") as fp:
        fp.write("\n".join(list_errors))
