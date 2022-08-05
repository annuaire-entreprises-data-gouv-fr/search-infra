import json
import logging
from urllib.request import urlopen

from airflow.models import Variable

COLOR_URL = Variable.get("COLOR_URL")


def get_next_color():
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
