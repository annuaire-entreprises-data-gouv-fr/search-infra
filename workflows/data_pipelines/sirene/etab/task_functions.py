import requests
import logging
from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.config import (
    INSEE_TMP_FOLDER,
    URL_ETABLISSEMENTS,
    URL_ETABLISSEMENTS_HISTORIQUE,
)
from dag_datalake_sirene.helpers.tchap import send_message


def download_stock_etab():
    logging.info(f"Downloading Etablissements stock: {URL_ETABLISSEMENTS}")
    r = requests.get(URL_ETABLISSEMENTS, allow_redirects=True)
    open(f"{INSEE_TMP_FOLDER}etab/StockEtablissement_utf8.zip", "wb").write(r.content)


def download_historique_etab():
    r = requests.get(URL_ETABLISSEMENTS_HISTORIQUE, allow_redirects=True)
    open(f"{INSEE_TMP_FOLDER}etab/StockEtablissementHistorique_utf8.zip", "wb").write(
        r.content
    )


def send_stock_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": f"{INSEE_TMP_FOLDER}etab/",
                "source_name": "StockEtablissement_utf8.zip",
                "dest_path": "insee/sirene/stock/",
                "dest_name": "StockEtablissement_utf8.zip",
            },
        ],
    )


def send_historique_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": f"{INSEE_TMP_FOLDER}etab/",
                "source_name": "StockEtablissementHistorique_utf8.zip",
                "dest_path": "insee/sirene/historique/",
                "dest_name": "StockEtablissementHistorique_utf8.zip",
            },
        ],
    )


def send_notification(ti):
    send_message(
        "\U0001F7E2 Données stock et historqiue Sirene établissements mises à jour."
    )
