import gzip
from unittest.mock import MagicMock

import pytest

from data_pipelines_annuaire.config import AIRFLOW_ENV
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient


def _make_client(keys):
    """Build an ObjectStorageClient with a mocked boto3 client.

    `keys` is the list of object keys returned by the paginator. `download_file`
    writes a gzip file (containing the key's date) at the requested path so the
    decompression step can be exercised.
    """
    client = ObjectStorageClient.__new__(ObjectStorageClient)
    client.bucket = "test-bucket"

    boto_client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": k} for k in keys]}]
    boto_client.get_paginator.return_value = paginator

    def fake_download(bucket, key, dest):
        with gzip.open(dest, "wb") as f:
            f.write(key.encode())

    boto_client.download_file.side_effect = fake_download
    client.client = boto_client
    return client, boto_client


def test_get_latest_database_prefixes_env_and_picks_latest(tmp_path):
    keys = [
        f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-01-01.db.gz",
        f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-03-15.db.gz",
        f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-02-10.db.gz",
    ]
    client, boto_client = _make_client(keys)
    local_path = tmp_path / "sirene.db"

    result = client.get_latest_database("sirene/database/", str(local_path))

    assert result == "2024-03-15T00:00:00"

    _, paginate_kwargs = boto_client.get_paginator.return_value.paginate.call_args
    assert paginate_kwargs["Prefix"] == f"ae/{AIRFLOW_ENV}/sirene/database/"

    downloaded_key = boto_client.download_file.call_args[0][1]
    assert downloaded_key.endswith("sirene_2024-03-15.db.gz")


def test_get_latest_database_decompresses_and_removes_gz(tmp_path):
    key = f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-03-15.db.gz"
    client, _ = _make_client([key])
    local_path = tmp_path / "sirene.db"

    client.get_latest_database("sirene/database/", str(local_path))

    assert local_path.exists()
    assert not (tmp_path / "sirene.db.gz").exists()
    assert local_path.read_bytes() == key.encode()


def test_get_latest_database_ignores_non_dated_gz_files(tmp_path):
    keys = [
        f"ae/{AIRFLOW_ENV}/sirene/database/latest.db.gz",
        f"ae/{AIRFLOW_ENV}/sirene/database/sirene_backup.db.gz",
        f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-02-10.db.gz",
    ]
    client, boto_client = _make_client(keys)
    local_path = tmp_path / "sirene.db"

    result = client.get_latest_database("sirene/database/", str(local_path))

    assert result == "2024-02-10T00:00:00"
    downloaded_key = boto_client.download_file.call_args[0][1]
    assert downloaded_key.endswith("sirene_2024-02-10.db.gz")


def test_get_latest_database_raises_when_only_non_dated_files(tmp_path):
    keys = [f"ae/{AIRFLOW_ENV}/sirene/database/latest.db.gz"]
    client, _ = _make_client(keys)

    with pytest.raises(Exception, match="No database file was found"):
        client.get_latest_database("sirene/database/", str(tmp_path / "sirene.db"))


def test_get_latest_database_raises_when_missing(tmp_path):
    client, _ = _make_client([])

    with pytest.raises(Exception, match="No database file was found"):
        client.get_latest_database("sirene/database/", str(tmp_path / "sirene.db"))
