import logging
from typing import Literal

from airflow.sdk import get_current_context, task

from data_pipelines_annuaire.config import (
    CURRENT_MONTH,
    PREVIOUS_MONTH,
)
from data_pipelines_annuaire.helpers import Notification
from data_pipelines_annuaire.helpers.utils import is_url_valid
from data_pipelines_annuaire.workflows.data_pipelines.sirene.stock.config import (
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
        # Files not required:
        # - Flux because the first of the month file is expected to be missing.
        #   And the flux DAG is the one triggering the ETL DAG anyway.
        # - Sirene géocodé à des fins statistiques because the file is published
        #   around the 21st so we use a fallback strategy instead.
        STOCK_SIRENE_CONFIG.url_object_storage
        + "StockEtablissement_"
        + month
        + "_with_gps.zip",
        STOCK_SIRENE_CONFIG.url_object_storage
        + "StockEtablissementHistorique_"
        + month
        + ".zip",
        STOCK_SIRENE_CONFIG.url_object_storage + "StockUniteLegale_" + month + ".zip",
        STOCK_SIRENE_CONFIG.url_object_storage
        + "StockUniteLegaleHistorique_"
        + month
        + ".zip",
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

    ti = get_current_context()["ti"]

    sirene_processing_month: str = ""
    if check_sirene_datasets_availability("current"):
        logging.info("Using current month Sirene data.")
        sirene_processing_month = CURRENT_MONTH
    elif check_sirene_datasets_availability("previous"):
        logging.warning("Using previous month Sirene data.")
        sirene_processing_month = PREVIOUS_MONTH
        ti.xcom_push(
            key=Notification.notification_xcom_key,
            value=f"⚠️ Using Sirene data from previous month: {PREVIOUS_MONTH}. ⚠️",
        )
    else:
        raise ValueError(
            "Some of current and previous month's Sirene data are unavailable."
            "Please check the data sources."
        )

    ti.xcom_push(
        key="sirene_processing_month",
        value=sirene_processing_month,
    )

    return sirene_processing_month == CURRENT_MONTH


def get_sirene_processing_month() -> str:
    ti = get_current_context()["ti"]
    return ti.xcom_pull(task_ids="determine_sirene_date", key="sirene_processing_month")
