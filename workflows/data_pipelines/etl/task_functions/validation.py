import json
import logging
import os

from dag_datalake_sirene.config import (
    AIRFLOW_ETL_DATA_DIR,
    DATABASE_VALIDATION_MINIO_PATH,
)
from dag_datalake_sirene.helpers.filesystem import LocalFile
from dag_datalake_sirene.helpers.minio_helpers import MinIOFile
from dag_datalake_sirene.helpers.notification import monitoring_logger
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient


def validate_table(
    table_name: str, datatabase_location: str, validations: list, file_alias: str = ""
) -> None:
    """
    Test table against values stored on Minio.
    Raise an error if the test fails.
    """
    for validation in validations:
        if validation not in ["row_count"]:
            raise ValueError(f"Validation {validation} not implemented")

    file_name = f"{table_name}__{file_alias}__stats"

    remote_path = os.path.join(
        DATABASE_VALIDATION_MINIO_PATH,
        os.path.basename(datatabase_location),
        f"{file_name}.json",
    )
    local_path = os.path.join(
        f"{AIRFLOW_ETL_DATA_DIR}",
        f"{file_name}.json",
    )
    try:
        minio_stats_file = MinIOFile(remote_path)
        local_stats_file = minio_stats_file.download_to(local_path=local_path)
    except FileNotFoundError as _:
        logging.warning(
            f"Validation file not found for table {table_name} in MinIO: '{remote_path}'."
            " Creating it from scratch. Stats will be reset."
        )
        local_stats_file = LocalFile(
            path=local_path,
            default_value_if_not_exists="{}",
        )

    last_stats = local_stats_file.read(mode="r")
    previous_data = json.loads(last_stats)

    if "row_count" in validations:
        previous_data = validate_row_count(
            table_name=table_name,
            datatabase_location=datatabase_location,
            previous_data=previous_data,
        )
        monitoring_logger(key=file_name, value=previous_data["row_count"])

    local_stats_file.write(lines=json.dumps(previous_data), mode="w")
    local_stats_file.upload_to_minio(minio_path=os.path.dirname(remote_path) + "/")
    logging.info(f"Test passed for table {table_name} in {datatabase_location}")
    local_stats_file.delete()


def validate_row_count(
    table_name: str,
    datatabase_location: str,
    previous_data: dict,
) -> dict:
    """
    Row count should be at least 98% of the previous row count.
    """

    with SqliteClient(datatabase_location) as sqlite_client:
        current_row_count = sqlite_client.get_table_count(table_name)

    previous_row_count = previous_data.get(
        "row_count", 0
    )  # 0 so it initializes the first run

    if current_row_count < 0.98 * previous_row_count:
        raise ValueError(
            f"Row count validation failed: current={current_row_count}, previous={previous_row_count}"
        )
    previous_data["row_count"] = current_row_count
    return previous_data
