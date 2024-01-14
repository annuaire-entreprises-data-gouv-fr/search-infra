import json
import logging
from urllib.request import urlopen
from datetime import datetime
from dag_datalake_sirene.config import (
    COLOR_URL,
    COLOR_IS_DAILY,
)


def get_next_color():
    if COLOR_IS_DAILY:
        current_date = datetime.today().strftime("%Y%m%d")
        return current_date

    try:
        with urlopen(COLOR_URL, timeout=5) as url:
            data = json.loads(url.read().decode())
            next_color = data["NEXT_COLOR"]
            logging.info(f"******************** Color file URL: {COLOR_URL}")
            logging.info(f"******************** Next color from file: {next_color}")
            return next_color
    except BaseException as error:
        raise Exception(f"******************** Ouuups Error: {error}")


NEXT_COLOR = get_next_color()


def get_colors(**kwargs):
    try:
        with urlopen(COLOR_URL, timeout=5) as url:
            data = json.loads(url.read().decode())
            next_color = data["NEXT_COLOR"]
            current_color = data["CURRENT_COLOR"]
            logging.info(f"******************** Color file URL: {COLOR_URL}")
            logging.info(f"******************** Next color from file: {next_color}")

            if COLOR_IS_DAILY:
                kwargs["ti"].xcom_push(key="next_color", value=NEXT_COLOR)
            else:
                kwargs["ti"].xcom_push(key="next_color", value=next_color)

            kwargs["ti"].xcom_push(key="current_color", value=current_color)
    except BaseException as error:
        raise Exception(f"******************** Ouuups Error: {error}")
