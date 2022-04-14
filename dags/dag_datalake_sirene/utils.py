import json
import logging
import os
import requests

from dotenv import load_dotenv
from operators.elastic_create_siren import ElasticCreateSirenOperator
from operators.elastic_fill_siren import ElasticFillSirenOperator
from operators.papermill_minio import PapermillMinioOperator

load_dotenv()


def get_next_color(**kwargs):
    try:
        response = requests.get(os.getenv("AIO_URL") + "/colors")
        next_color = json.loads(response.content)["NEXT_COLOR"]
    except requests.exceptions.RequestException:
        next_color = "blue"
    logging.info(f"Next color: {next_color}")
    kwargs["ti"].xcom_push(key="next_color", value=next_color)


def format_sirene_notebook(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_next_color")
    elastic_index = "siren-" + next_color

    format_notebook = PapermillMinioOperator(
        task_id="format_sirene_notebook",
        input_nb=f'{os.getenv("AIRFLOW_DAG_HOME")}{os.getenv("DAG_FOLDER")}process-data-before-indexation.ipynb',
        output_nb="latest.ipynb",
        tmp_path=f'{os.getenv("TMP_FOLDER")}{os.getenv("DAG_FOLDER")}{os.getenv("DAG_NAME")}/',
        minio_url=os.getenv("MINIO_URL"),
        minio_bucket=os.getenv("MINIO_BUCKET"),
        minio_user=os.getenv("MINIO_USER"),
        minio_password=os.getenv("MINIO_PASSWORD"),
        minio_output_filepath=f'{os.getenv("DAG_FOLDER")}'
        + f'{os.getenv("DAG_NAME")}'
        + "/latest/format_sirene_notebook/",
        parameters={
            "msgs": "Ran from Airflow latest !",
            "DATA_DIR": f'{os.getenv("TMP_FOLDER")}{os.getenv("DAG_FOLDER")}{os.getenv("DAG_NAME")}/data/',
            "OUTPUT_DATA_FOLDER": f'{os.getenv("TMP_FOLDER")}{os.getenv("DAG_FOLDER")}{os.getenv("DAG_NAME")}/output/',
            "ELASTIC_INDEX": elastic_index,
        },
    )
    format_notebook.execute(dict())


def create_elastic_siren(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_next_color")
    elastic_index = "siren-" + next_color
    create_index = ElasticCreateSirenOperator(
        task_id="create_elastic_index",
        elastic_url=os.getenv("ELASTIC_URL"),
        elastic_index=elastic_index,
        elastic_user=os.getenv("ELASTIC_USER"),
        elastic_password=os.getenv("ELASTIC_PASSWORD"),
    )
    create_index.execute(dict())


def fill_siren(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_next_color")
    elastic_index = "siren-" + next_color

    all_deps = [
        *"-0".join(list(str(x) for x in range(0, 10))).split("-")[1:],
        *list(str(x) for x in range(10, 20)),
        *["2A", "2B"],
        *list(str(x) for x in range(21, 95)),
        *"-7510".join(list(str(x) for x in range(0, 10))).split("-")[1:],
        *"-751".join(list(str(x) for x in range(10, 21))).split("-")[1:],
        *[""],
    ]
    all_deps.remove("75")

    for dep in all_deps:
        print(
            f'{os.getenv("DAG_FOLDER")}\
            {os.getenv("DAG_NAME")}\
            /latest/'
            + elastic_index
            + "_"
            + dep
            + ".csv"
        )
        fill_elastic = ElasticFillSirenOperator(
            task_id="fill_elastic_index",
            elastic_url=os.getenv("ELASTIC_URL"),
            elastic_index=elastic_index,
            elastic_user=os.getenv("ELASTIC_USER"),
            elastic_password=os.getenv("ELASTIC_PASSWORD"),
            elastic_bulk_size=1500,
            minio_url=os.getenv("MINIO_URL"),
            minio_bucket=os.getenv("MINIO_BUCKET"),
            minio_user=os.getenv("MINIO_USER"),
            minio_password=os.getenv("MINIO_PASSWORD"),
            minio_filepath=f'{os.getenv("DAG_FOLDER")}\
            {os.getenv("DAG_NAME")}\
            /latest/format_sirene_notebook/output/'
            + elastic_index
            + "_"
            + dep
            + ".csv",
        )
        fill_elastic.execute(dict())
