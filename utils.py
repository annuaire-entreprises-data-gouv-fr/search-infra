import json
import logging
import os
from urllib.request import urlopen

from dotenv import load_dotenv
from minio import Minio
from operators.elastic_create_siren import ElasticCreateSirenOperator
from operators.elastic_fill_siren import ElasticFillSirenOperator
from operators.papermill_minio_siren import PapermillMinioSirenOperator

load_dotenv()

AIRFLOW_URL = os.getenv("AIRFLOW_URL")
COLOR_URL = os.getenv("COLOR_URL")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_URL = os.getenv("ELASTIC_URL")
ELASTIC_USER = os.getenv("ELASTIC_USER")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD")
MINIO_URL = os.getenv("MINIO_URL")
MINIO_USER = os.getenv("MINIO_USER")
DAG_FOLDER = os.getenv("DAG_FOLDER")
DAG_NAME = os.getenv("DAG_NAME")
AIRFLOW_DAG_HOME = os.getenv("AIRFLOW_DAG_HOME")
TMP_FOLDER = os.getenv("TMP_FOLDER")
ENV = os.getenv("ENV")


def get_colors(**kwargs):
    try:
        with urlopen(COLOR_URL) as url:
            data = json.loads(url.read().decode())
            next_color = data["NEXT_COLOR"]
            current_color = data["CURRENT_COLOR"]
            logging.info(f"******************** Next color from file: {next_color}")
            kwargs["ti"].xcom_push(key="next_color", value=next_color)
            kwargs["ti"].xcom_push(key="current_color", value=current_color)
    except BaseException as error:
        raise Exception(f"******************** Ouuups Error: {error}")


def format_sirene_notebook(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = "siren-" + next_color

    format_notebook = PapermillMinioSirenOperator(
        task_id="format_sirene_notebook",
        input_nb=AIRFLOW_DAG_HOME + DAG_FOLDER + "process-data-before-indexation.ipynb",
        output_nb="latest" + ENV + ".ipynb",
        tmp_path=TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET,
        minio_user=MINIO_USER,
        minio_password=MINIO_PASSWORD,
        minio_output_filepath=DAG_FOLDER
        + DAG_NAME
        + "/"
        + ENV
        + "/format_sirene_notebook/",
        parameters={
            "msgs": "Ran from Airflow " + ENV + " !",
            "DATA_DIR": TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/data/",
            "OUTPUT_DATA_FOLDER": TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/output/",
            "LABELS_FOLDER": TMP_FOLDER + DAG_FOLDER + DAG_NAME + "/labels/",
            "ELASTIC_INDEX": elastic_index,
        },
    )
    format_notebook.execute(dict())


def create_elastic_siren(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    elastic_index = "siren-" + next_color
    logging.info(f"******************** Index to create: {elastic_index}")
    create_index = ElasticCreateSirenOperator(
        task_id="create_elastic_index",
        elastic_url=ELASTIC_URL,
        elastic_index=elastic_index,
        elastic_user=ELASTIC_USER,
        elastic_password=ELASTIC_PASSWORD,
    )
    create_index.execute(dict())


def fill_siren(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
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
            DAG_FOLDER + DAG_NAME + "/" + ENV + "/" + elastic_index + "_" + dep + ".csv"
        )
        fill_elastic = ElasticFillSirenOperator(
            task_id="fill_elastic_index",
            elastic_url=ELASTIC_URL,
            elastic_index=elastic_index,
            elastic_user=ELASTIC_USER,
            elastic_password=ELASTIC_PASSWORD,
            elastic_bulk_size=1500,
            minio_url=MINIO_URL,
            minio_bucket=MINIO_BUCKET,
            minio_user=MINIO_USER,
            minio_password=MINIO_PASSWORD,
            minio_filepath=DAG_FOLDER
            + DAG_NAME
            + "/"
            + ENV
            + "/format_sirene_notebook/output/"
            + elastic_index
            + "_"
            + dep
            + ".csv",
        )
        fill_elastic.execute(dict())

    doc_count = fill_elastic.count_docs(dict())
    kwargs["ti"].xcom_push(key="doc_count", value=doc_count)


def check_elastic_index(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(key="doc_count", task_ids="fill_elastic_siren")
    logging.info(f"******************** Documents indexed: {doc_count}")
    if float(doc_count) < 20e6:
        raise ValueError(
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed."
        )


def update_color_file(**kwargs):
    next_color = kwargs["ti"].xcom_pull(key="next_color", task_ids="get_colors")
    current_color = kwargs["ti"].xcom_pull(key="current_color", task_ids="get_colors")
    colors = {"CURRENT_COLOR": next_color, "NEXT_COLOR": current_color}
    logging.info(f"******************** Next color configuration: {colors}")

    with open("colors.json", "w") as write_file:
        json.dump(colors, write_file)
    minio_filepath = f"ae/colors-{ENV}.json"
    minio_url = MINIO_URL
    minio_bucket = MINIO_BUCKET
    minio_user = MINIO_USER
    minio_password = MINIO_PASSWORD

    # start client
    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=True,
    )

    # check if bucket exists.
    found = client.bucket_exists(minio_bucket)
    if found:
        client.fput_object(
            bucket_name=minio_bucket,
            object_name=minio_filepath,
            file_path="colors.json",
            content_type="application/json",
        )
