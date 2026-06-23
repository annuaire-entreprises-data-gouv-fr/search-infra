import gzip
import io
import shutil
from unittest.mock import MagicMock, patch

import pytest

from data_pipelines_annuaire.config import AIRFLOW_ENV
from data_pipelines_annuaire.helpers.object_storage import ObjectStorageClient

pigz_required = pytest.mark.skipif(
    shutil.which("pigz") is None, reason="pigz binary is not installed"
)


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


def _make_upload_client():
    """Build a client whose upload_fileobj decompresses what it receives.

    The uploaded gzip stream is gunzipped and stashed so the test can assert
    the bytes round-trip exactly as a standard gzip member.
    """
    client = ObjectStorageClient.__new__(ObjectStorageClient)
    client.bucket = "test-bucket"

    captured = {}

    def fake_upload_fileobj(fileobj, bucket, key, ExtraArgs=None):
        captured["bucket"] = bucket
        captured["key"] = key
        captured["extra_args"] = ExtraArgs
        captured["content"] = gzip.decompress(fileobj.read())

    boto_client = MagicMock()
    boto_client.upload_fileobj.side_effect = fake_upload_fileobj
    client.client = boto_client
    return client, captured


@pigz_required
def test_upload_compressed_file_roundtrips_as_gzip(tmp_path):
    source = tmp_path / "sirene.db"
    payload = b"SQLite format 3\x00" + b"some database bytes" * 1000
    source.write_bytes(payload)

    client, captured = _make_upload_client()

    client.upload_compressed_file(
        source_file_path=str(source),
        object_storage_path="sirene/database/",
        dest_name="sirene_2024-03-15.db.gz",
    )

    assert captured["content"] == payload
    assert (
        captured["key"] == f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-03-15.db.gz"
    )
    assert captured["extra_args"]["ACL"] == "public-read"
    assert captured["extra_args"]["ContentEncoding"] == "gzip"


def test_upload_compressed_file_raises_when_dest_name_not_gz(tmp_path):
    source = tmp_path / "sirene.db"
    source.write_bytes(b"SQLite format 3\x00")

    client, _ = _make_upload_client()

    with pytest.raises(Exception, match="does not end with .gz"):
        client.upload_compressed_file(
            source_file_path=str(source),
            object_storage_path="sirene/database/",
            dest_name="sirene_2024-03-15.db",
        )


def test_upload_compressed_file_raises_when_pigz_fails(tmp_path):
    source = tmp_path / "sirene.db"
    source.write_bytes(b"SQLite format 3\x00")

    client = ObjectStorageClient.__new__(ObjectStorageClient)
    client.bucket = "test-bucket"
    boto_client = MagicMock()
    # Drain the stream without decompressing so the failure path is reached
    boto_client.upload_fileobj.side_effect = (
        lambda fileobj, bucket, key, ExtraArgs=None: fileobj.read()
    )
    client.client = boto_client

    fake_proc = MagicMock()
    fake_proc.stdout = io.BytesIO(b"")
    fake_proc.stderr = io.BytesIO(b"pigz: abort: out of memory")
    fake_proc.returncode = 1

    with patch(
        "data_pipelines_annuaire.helpers.object_storage.subprocess.Popen",
        return_value=fake_proc,
    ):
        with pytest.raises(RuntimeError, match="pigz failed"):
            client.upload_compressed_file(
                source_file_path=str(source),
                object_storage_path="sirene/database/",
                dest_name="sirene_2024-03-15.db.gz",
            )

    # The possibly-partial object must be deleted so it cannot be picked up later
    boto_client.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key=f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-03-15.db.gz",
    )


def test_upload_compressed_file_deletes_object_when_upload_fails(tmp_path):
    source = tmp_path / "sirene.db"
    source.write_bytes(b"SQLite format 3\x00")

    client = ObjectStorageClient.__new__(ObjectStorageClient)
    client.bucket = "test-bucket"
    boto_client = MagicMock()
    boto_client.upload_fileobj.side_effect = RuntimeError("connection dropped")
    client.client = boto_client

    fake_proc = MagicMock()
    fake_proc.stdout = io.BytesIO(b"")
    fake_proc.stderr = io.BytesIO(b"")
    fake_proc.returncode = 0

    with patch(
        "data_pipelines_annuaire.helpers.object_storage.subprocess.Popen",
        return_value=fake_proc,
    ):
        with pytest.raises(RuntimeError, match="connection dropped"):
            client.upload_compressed_file(
                source_file_path=str(source),
                object_storage_path="sirene/database/",
                dest_name="sirene_2024-03-15.db.gz",
            )

    fake_proc.kill.assert_called_once()
    boto_client.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key=f"ae/{AIRFLOW_ENV}/sirene/database/sirene_2024-03-15.db.gz",
    )


def test_upload_compressed_file_raises_when_source_missing(tmp_path):
    client, _ = _make_upload_client()

    with pytest.raises(Exception, match="does not exist"):
        client.upload_compressed_file(
            source_file_path=str(tmp_path / "missing.db"),
            object_storage_path="sirene/database/",
            dest_name="sirene_2024-03-15.db.gz",
        )
