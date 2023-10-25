import requests
from dag_datalake_sirene.data_pipelines.config import (
    AIRFLOW_ENV,
    MATTERMOST_DATAGOUV_DATAENG,
    MATTERMOST_DATAGOUV_DATAENG_TEST,
)


def send_message(
    text,
    endpoint_url=None,
    image_url=None,
):
    """Send a message to a mattermost channel

    Args:
        endpoint_url (str): URL of the mattermost endpoint (for bot)
        text (str): Text to send to a channel
        image_url (Optional[str], optional): Url of an image to link
        with your text. Defaults to None.
    """
    if not endpoint_url:
        if AIRFLOW_ENV == "dev":
            endpoint_url = MATTERMOST_DATAGOUV_DATAENG_TEST
        if AIRFLOW_ENV == "prod":
            endpoint_url = MATTERMOST_DATAGOUV_DATAENG
    data = {}
    data["text"] = text
    if image_url:
        data["attachments"] = [{"image_url": image_url}]

    r = requests.post(endpoint_url, json=data)
    print(endpoint_url)
    print(data)
    print(r.status_code)
    assert r.status_code == 200
