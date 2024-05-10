import requests

from dag_datalake_sirene.helpers.minio_helpers import minio_client
from dag_datalake_sirene.config import (
    INSEE_TMP_FOLDER,
    URL_UNITE_LEGALE,
    URL_UNITE_LEGALE_HISTORIQUE,
)
from dag_datalake_sirene.helpers.tchap import send_message


def download_stock_ul():
    r = requests.get(URL_UNITE_LEGALE, allow_redirects=True)
    open(f"{INSEE_TMP_FOLDER}ul/StockUniteLegale_utf8.zip", "wb").write(r.content)


def download_historique_ul():
    r = requests.get(URL_UNITE_LEGALE_HISTORIQUE, allow_redirects=True)
    open(f"{INSEE_TMP_FOLDER}ul/StockUniteLegaleHistorique_utf8.zip", "wb").write(
        r.content
    )


def send_stock_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": f"{INSEE_TMP_FOLDER}ul/",
                "source_name": "StockUniteLegale_utf8.zip",
                "dest_path": "insee/sirene/stock/",
                "dest_name": "StockUniteLegale_utf8.zip",
            },
        ],
    )


def send_historique_file_to_minio():
    minio_client.send_files(
        list_files=[
            {
                "source_path": f"{INSEE_TMP_FOLDER}ul/",
                "source_name": "StockUniteLegaleHistorique_utf8.zip",
                "dest_path": "insee/sirene/historique/",
                "dest_name": "StockUniteLegaleHistorique_utf8.zip",
            },
        ],
    )


def send_notification(ti):
    send_message(
        "\U0001F7E2 Données Stock et Historqiue Sirene mises à jour.\n"
        "- Données stockées sur Minio"
    )
