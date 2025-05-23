import logging
from typing import Literal

from airflow.decorators import task
from airflow.operators.python import get_current_context

from dag_datalake_sirene.config import (
    CURRENT_MONTH,
    PREVIOUS_MONTH,
    URL_STOCK_ETABLISSEMENTS,
)
from dag_datalake_sirene.helpers.utils import is_url_valid
from dag_datalake_sirene.workflows.data_pipelines.sirene.stock.config import (
    STOCK_SIRENE_CONFIG,
)


def get_datasets_urls(month_period: Literal["current", "previous"]) -> list[str]:
    if month_period == "current":
        month = CURRENT_MONTH
    elif month_period == "previous":
        month = PREVIOUS_MONTH
    else:
        raise NotImplementedError("Only 'current' and 'previous' are supported.")

    urls: list[str] = [
        # Flux files are not required to be available
        # For example, the first of the month no flux is expected
        STOCK_SIRENE_CONFIG.url_minio
        + "StockEtablissementHistorique_"
        + month
        + ".zip",
        STOCK_SIRENE_CONFIG.url_minio + "StockUniteLegale_" + month + ".zip",
        STOCK_SIRENE_CONFIG.url_minio + "StockUniteLegaleHistorique_" + month + ".zip",
        URL_STOCK_ETABLISSEMENTS[month_period].replace("geo_siret", ""),
    ]

    logging.info(f"URLs to check for {month_period} month: {urls}")

    return urls


def check_sirene_datasets_availability(
    month_period: Literal["current", "previous"],
) -> bool:
    """ "
    Check if all Sirene datasets are available for the given month.
    """
    urls_status = {url: is_url_valid(url) for url in get_datasets_urls(month_period)}
    are_urls_ok = all(urls_status.values())
    if not are_urls_ok:
        logging.info(f"Some of {month_period} month's Sirene data are unavailable:")
        for url, is_ok in urls_status.items():
            if not is_ok:
                logging.info(f"\t{url}")
        return False
    return True


@task
def determine_sirene_date() -> bool:
    """
    Determine the date of the latest Sirene data available.
    All files must be available for the given month.
    Otherwise it will raise a warning and use the previous month.
    """

    sirene_processing_month: str = ""
    if check_sirene_datasets_availability("current"):
        logging.info("Using current month Sirene data.")
        sirene_processing_month = CURRENT_MONTH
    elif check_sirene_datasets_availability("previous"):
        logging.warning("Using previous month Sirene data.")
        sirene_processing_month = PREVIOUS_MONTH
    else:
        raise ValueError(
            "Some of current and previous month's Sirene data are unavailable."
            "Please check the data sources."
        )

    ti = get_current_context()["ti"]
    ti.xcom_push(
        key="sirene_processing_month",
        value=sirene_processing_month,
    )

    return sirene_processing_month == CURRENT_MONTH


def get_sirene_processing_month() -> str:
    ti = get_current_context()["ti"]
    return ti.xcom_pull(task_ids="determine_sirene_date", key="sirene_processing_month")
