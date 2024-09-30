import requests
import logging
from dag_datalake_sirene.config import (
    AIRFLOW_ENV,
    TCHAP_ANNUAIRE_WEBHOOK,
    TCHAP_ANNUAIRE_ROOM_ID,
)


def send_message(
    text,
    endpoint_url=None,
    image_url=None,
):
    """Send a message to a Tchap channel

    Args:
        endpoint_url (str): URL of the Tchap endpoint (for bot)
        text (str): Text to send to a channel
        image_url (Optional[str], optional): Url of an image to link
        with your text. Defaults to None.
    """
    if not endpoint_url:
        if AIRFLOW_ENV == "prod":
            endpoint_url = TCHAP_ANNUAIRE_WEBHOOK
        else:
            return None
    data = {"roomId": TCHAP_ANNUAIRE_ROOM_ID, "message": text}
    if image_url:
        data["attachments"] = [{"image_url": image_url}]

    try:
        response = requests.post(endpoint_url, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send message: {e}")
        raise Exception(f"Failed to send message: {e}")


def send_notification_failure_tchap(context):
    dag_id = context["dag"].dag_id  # Get the dag_id from the context
    message = f"\U0001F534 Fail DAG: {dag_id}!!!!"
    send_message(message)
