import json
import tempfile


class JsonSerializer:
    def get_content_type(self):
        return "application/json"

    def serialize(self, data):
        return json.dumps(data)

    def unserialize(self, data):
        return json.loads(data)


class Filesystem:
    def __init__(self, client, dirpath, serializer, tmp_dirpath="/tmp"):
        self.client = client
        self.dirpath = dirpath
        self.serializer = serializer
        self.tmp_dirpath = tmp_dirpath

    def read(self, filename, local_path=None):
        if local_path is None:
            _, local_path = tempfile.mkstemp()

        try:
            self.client.get_object_minio(self.dirpath, filename, local_path)
        except S3Error as e:
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
