import logging
from typing import Literal

import requests

from data_pipelines_annuaire.config import (
    AIRFLOW_ENV,
    TCHAP_BOT_DEPLOIEMENT_ROOM_ID,
    TCHAP_ENDPOINT,
    TCHAP_OPI_DATA_TOKEN,
)
from data_pipelines_annuaire.helpers.utils import html_to_text


def send_message(
    text: str,
    tchap_message_type: Literal["text", "notice"] = "text",
    room_id: str = TCHAP_BOT_DEPLOIEMENT_ROOM_ID,
) -> None:
    """
    Send a message to a Tchap channel.

    Args:
        text (str): Text to send to a channel. Has to be formatted with html.
        tchap_message_type (Literal["text", "notice"], optional):
            A notice type will not trigger a notification on Tchap but if the user is mentionned
            A text type message will trigger the usual Tchap notification
        room_id (str, optional): the full Tchap room ID
    """
    if AIRFLOW_ENV != "prod":
        return None
    room_url = f"{TCHAP_ENDPOINT}/{room_id}/send/m.room.message"
    data = {
        "msgtype": f"m.{tchap_message_type}",
        "body": html_to_text(text),
        "formatted_body": text,
    }

    try:
        response = requests.post(
            room_url,
            headers={"Authorization": f"Bearer {TCHAP_OPI_DATA_TOKEN}"},
            json=data,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message: {e}")
        raise Exception(f"Failed to send message: {e}")
