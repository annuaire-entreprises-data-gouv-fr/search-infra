import logging
import os
from typing import Optional, Tuple, TypedDict

import requests

from dag_datalake_sirene.config import (
    DATA_GOUV_BASE_URL,
    DATAGOUV_SECRET_API_KEY,
    DATAGOUV_URL,
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
        f"{DATAGOUV_URL}/datasets/r/{resource_id}", stream=True
    ) as r:
        r.raise_for_status()
        os.makedirs(os.path.dirname(file_to_store["dest_path"]), exist_ok=True)
        with open(
            f"{file_to_store['dest_path']}{file_to_store['dest_name']}", "wb"
        ) as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def get_dataset_or_resource_metadata(
    dataset_id: str = "",
    resource_id: str = "",
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
        url = f"{DATAGOUV_URL}/api/2/datasets/resources/{resource_id}/"
    elif dataset_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}"
    else:
        raise ValueError("No argument provided.")
    r = datagouv_session.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        return {"message": "error", "status": r.status_code}


def get_resource_metadata(resource_id: str):
    url = f"{DATAGOUV_URL}/api/2/datasets/resources/{resource_id}"
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
        logging.info("Resource was given this id:", resource_id)
        url = (
            f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
            "upload/"
        )
    if resource_id and resource_payload:
        r_put = datagouv_session.put(url.replace("upload/", ""), json=resource_payload)
        r_put.raise_for_status()
    return r


def fetch_last_modified_date(resource_id: str) -> str:
    """
    Fetch the 'last_modified' date of a resource from data.gouv.fr.

    Args:
        resource_id (str): The ID of the resource from data.gouv.fr.

    Returns:
        str: The 'last_modified' date of the resource.

    Raises:
        ValueError: If the last modified date is not found in the resource metadata.
    """
    try:
        # Fetch the resource metadata
        metadata = get_resource_metadata(resource_id)

        # Extract the 'last_modified' date
        date_last_modified = metadata.get("resource", {}).get("last_modified")
        if not date_last_modified:
            raise ValueError("Last modified date not found in resource metadata.")
        return date_last_modified

    except Exception as e:
        raise RuntimeError(f"Failed to fetch last modified date: {e}")


def fetch_last_resource_from_dataset(
    dataset_url: str, resource_extension: str = "csv"
) -> Tuple[str, str]:
    """
    Fetch the last resource URL of a dataset with a specific extension.

    Args:
        dataset_url (str): URL of the data.gouv dataset.
        resource_extension (str, optional): File extension of the resource to look for. Defaults to csv.

    Returns:
        Tuple[str, str]:
            - first element is the resource id
            - second element is the resource url
    """
    response = requests.get(dataset_url)
    if response.ok:
        dataset_metadata = response.json()
    else:
        logging.error(f"Error fetching dataset metadata from {dataset_url}")
        response.raise_for_status()

    resource_id, _ = max(
        (
            (resource["id"], resource["last_modified"])
            for resource in dataset_metadata.get("resources", [])
            if resource["format"] == resource_extension
        ),
        default=(None, None),
        key=lambda x: x[1],  # Sort by 'last_modified'
    )
    if resource_id:
        return resource_id, f"{DATA_GOUV_BASE_URL}{resource_id}"

    raise ValueError(f"No CSV resource found for dataset at {dataset_url}.")
