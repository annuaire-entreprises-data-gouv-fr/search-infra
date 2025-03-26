import logging
from typing import Any

import requests

from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    MATTERMOST_WEBHOOK,
)


def send_message(
    text: str,
    endpoint_url: str = MATTERMOST_WEBHOOK,
    image_url: str | None = None,
) -> None:
    """Send a message to a Mattermost channel.

    Args:
        text (str): Text to send to a channel
        endpoint_url (str): URL of the Mattermost webhook
        image_url (Optional[str], optional): Url of an image to link
        with your text. Defaults to None.
    """
    if AIRFLOW_ENV != "prod":
        return None

    data: dict[str, Any] = {"text": text}
    if image_url:
        data["props"] = {"attachments": [{"image_url": image_url}]}

    try:
        response = requests.post(endpoint_url, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Mattermost message: {e}")
        raise Exception(f"Failed to send Mattermost message: {e}")


def send_notification_failure_mattermost(context):
    dag_id = context["dag"].dag_id  # Get the dag_id from the context
    message = f":red_circle: Fail DAG: {dag_id}!!!!"
    send_message(message)
