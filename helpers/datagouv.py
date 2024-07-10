from typing import Optional, TypedDict
import requests
import os
from dag_datalake_sirene.config import (
    DATAGOUV_URL,
    DATAGOUV_SECRET_API_KEY,
)

datagouv_session = requests.Session()
datagouv_session.headers.update({"X-API-KEY": DATAGOUV_SECRET_API_KEY})


class File(TypedDict):
    dest_path: str
    dest_name: str


def get_resource(
    resource_id: str,
    file_to_store: File,
):
    """Download a resource in data.gouv.fr

    Args:
        resource_id (str): ID of the resource
        file_to_store (File): Dictionnary containing `dest_path` and
        `dest_name` where to store downloaded resource

    """
    with datagouv_session.get(
        f"{DATAGOUV_URL}/fr/datasets/r/{resource_id}", stream=True
    ) as r:
        r.raise_for_status()
        os.makedirs(os.path.dirname(file_to_store["dest_path"]), exist_ok=True)
        with open(
            f"{file_to_store['dest_path']}{file_to_store['dest_name']}", "wb"
        ) as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def get_dataset_or_resource_metadata(
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Retrieve dataset or resource metadata from data.gouv.fr

    Args:
        dataset_id (str): ID ot the dataset
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset metadata which will be
        returned. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}"
    r = datagouv_session.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        return {"message": "error", "status": r.status_code}


def post_resource(
    file_to_upload: File,
    dataset_id: str,
    resource_id: Optional[str] = None,
    resource_payload: Optional[dict] = None,
):
    """Upload a resource in data.gouv.fr

    Args:
        file_to_upload (File): Dictionnary containing `dest_path` and
        `dest_name` where resource to upload is stored
        dataset_id (str): ID of the dataset where to store resource
        resource_id (Optional[str], optional): ID of the resource where
        to upload file. If it is a new resource, let it to None.
        Defaults to None.
        resource_payload (Optional[dict], optional): payload to update the resource's
        metadata.
        Defaults to None (then the id is retrieved when the resource is created)

    Returns:
        json: return API result in a dictionnary
    """
    if not file_to_upload["dest_path"].endswith("/"):
        file_to_upload["dest_path"] += "/"
    files = {
        "file": open(
            f"{file_to_upload['dest_path']}{file_to_upload['dest_name']}",
            "rb",
        )
    }
    if resource_id:
        url = (
            f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
            "upload/"
        )
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/upload/"
    r = datagouv_session.post(url, files=files)
    r.raise_for_status()
    if not resource_id:
        resource_id = r.json()["id"]
        print("Resource was given this id:", resource_id)
        url = (
            f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
            "upload/"
        )
    if resource_id and resource_payload:
        r_put = datagouv_session.put(url.replace("upload/", ""), json=resource_payload)
        r_put.raise_for_status()
    return r
