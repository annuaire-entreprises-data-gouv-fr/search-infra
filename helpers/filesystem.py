from __future__ import annotations

import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Literal

import dag_datalake_sirene.helpers.minio_helpers as minio


class LocalFile:
    def __init__(
        self,
        path: str,
        default_value_if_not_exists: str | None = None,
    ) -> None:
        """
        Initialize a LocalFile object.

        Args:
            path (str): The path to the file.
            default_value_if_not_exists (str, optional): When provided, and if the file does not exist,
                the file will be created with this content. Defaults to None.

        Raises:
            FileNotFoundError: If the file doesn't exist and create_if_not_exists is False.
        """

        self.path = Path(path)
        self.filepath = str(self.path.parent) + "/"
        self.filename = str(self.path.name)

        if not self.path.exists() and default_value_if_not_exists:
            self.path.write_text(default_value_if_not_exists)
            logging.info(f"Created file {self.path.absolute()} with default content.")

        self.assert_exists()

    def read_lines(
        self,
        mode: Literal["r", "rb"],
    ) -> list[str]:
        with self.path.open(mode) as f:
            return f.readlines()

    def write_lines(
        self,
        lines: list[str],
        mode: Literal["w", "wb", "a", "ab", "r+", "rb+", "w+", "wb+", "a+", "ab+"],
    ) -> None:
        with self.path.open(mode) as f:
            f.writelines(lines)

    def read(
        self,
        mode: Literal["r", "rb"],
        default: str = "",
    ) -> str:
        with self.path.open(mode) as f:
            output = f.read()
        return output if output else default

    def write(
        self,
        lines: str,
        mode: Literal["w", "wb", "a", "ab", "r+", "rb+", "w+", "wb+", "a+", "ab+"],
    ) -> None:
        with self.path.open(mode) as f:
            f.write(lines)

    def assert_exists(self) -> None:
        if not self.path.exists():
            raise FileNotFoundError(f"{self.path.absolute()} doesn't exist")

    def upload_to_minio(
        self,
        minio_path: str,
        minio_filename: str | None = None,
    ) -> minio.MinIOFile:
        if minio_filename is None:
            minio_filename = self.filename

        if not minio_path.endswith("/"):
            logging.warning(
                f"minio_path should end with a '/', adding one to {minio_path}"
            )
            minio_path += "/"

        minio.MinIOClient().send_files(
            [
                minio.File(
                    source_path=self.filepath,
                    source_name=self.filename,
                    dest_path=minio_path,
                    dest_name=minio_filename,
                    content_type=None,
                )
            ]
        )

        # Wait for the file to be uploaded before creating MinIOFile object
        time.sleep(15)

        return minio.MinIOFile(minio_path + minio_filename)

    def delete(self) -> None:
        self.path.unlink()


class JsonSerializer:
    def get_content_type(self):
        return "application/json"

    def serialize(self, data):
        return json.dumps(data)

    def unserialize(self, data):
        return json.loads(data)


class Filesystem:
    def __init__(self, dirpath, serializer, tmp_dirpath="/tmp"):
        self.client = minio.MinIOClient()
        self.dirpath = dirpath
        self.serializer = serializer
        self.tmp_dirpath = tmp_dirpath

    def read(self, filename, local_path=None):
        if local_path is None:
            _, local_path = tempfile.mkstemp()

        try:
            self.client.get_object_minio(self.dirpath, filename, local_path)
        except Exception as e:
            logging.error(e)
            return None

        content = None

        with open(local_path, "r") as file:
            content = file.read()
            unserialized = self.serializer.unserialize(content)

        return unserialized

    def write(self, filename, data):
        with tempfile.NamedTemporaryFile(mode="w+", dir=self.tmp_dirpath) as tmp_file:
            serialized = self.serializer.serialize(data)
            tmp_file.write(serialized)
            tmp_file.flush()

            self.client.put_object_minio(
                tmp_file.name[len(self.tmp_dirpath) :],
                f"{self.dirpath}{filename}",
                self.tmp_dirpath,
                self.serializer.get_content_type(),
            )
