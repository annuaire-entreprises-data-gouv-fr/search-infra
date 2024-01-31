import logging
import time
from datetime import datetime
from elasticsearch_dsl import connections

from dag_datalake_sirene.config import (
    AIRFLOW_ELK_DAG_NAME,
    ELASTIC_URL,
    ELASTIC_USER,
    ELASTIC_PASSWORD,
    ELASTIC_SNAPSHOT_REPOSITORY,
    ELASTIC_SNAPSHOT_MAX_REVISIONS,
    ELASTIC_SNAPSHOT_MINIO_STATE_PATH,
)
from dag_datalake_sirene.helpers.minio_helpers import minio_client


def update_minio_current_index_version(**kwargs):
    current_date = datetime.today().strftime("%Y%m%d%H%M%S")
    content = minio_client.read_json_file(
        ELASTIC_SNAPSHOT_MINIO_STATE_PATH, "current.json"
    )

    if content is None:
        content = {}

    if "current" in content:
        content["previous"] = content["current"]

    content["current"] = {
        "file": f"daily/{current_date}.json",
        "index": kwargs["ti"].xcom_pull(
            key="elastic_index",
            task_ids="get_next_index_name",
            dag_id=AIRFLOW_ELK_DAG_NAME,
            include_prior_dates=True,
        ),
        "snapshot": kwargs["ti"].xcom_pull(
            key="snapshot_name",
            task_ids="snapshot_elastic_index",
        ),
    }

    minio_client.write_json_file(
        ELASTIC_SNAPSHOT_MINIO_STATE_PATH, f"daily/{current_date}.json", content
    )
    minio_client.write_json_file(
        ELASTIC_SNAPSHOT_MINIO_STATE_PATH, "current.json", content
    )


def rollback_minio_current_index_version(**kwargs):
    content = minio_client.read_json_file(
        ELASTIC_SNAPSHOT_MINIO_STATE_PATH, "current.json"
    )

    if content is None:
        raise Exception("No previous version found")

    previous = minio_client.read_json_file(
        ELASTIC_SNAPSHOT_MINIO_STATE_PATH, content["current"]["file"]
    )

    if previous is None:
        raise Exception("No previous version found")

    snapshots = elastic_connection.snapshot.get(
        repository=ELASTIC_SNAPSHOT_REPOSITORY,
        snapshot=previous["current"]["snapshot"],
        ignore_unavailable=True,
    )

    if len(snapshots) == 0:
        raise Exception(
            f"The snapshot {previous['current']['snapshot']} no longer exists on Elasticsearch"
        )

    logging.info(f"Rolling back to {content['current']['file']}")
    minio_client.write_json_file(
        ELASTIC_SNAPSHOT_MINIO_STATE_PATH, "current.json", previous
    )

    kwargs["ti"].xcom_push(key="elastic_index", value=previous["current"]["index"])


def snapshot_elastic_index(**kwargs):
    """
    Create and save Elastic index snapshot in MinIO

    https://www.elastic.co/guide/en/elasticsearch/reference/7.17/snapshot-restore.html
    """

    elastic_index = kwargs["ti"].xcom_pull(
        key="elastic_index",
        task_ids="get_next_index_name",
        dag_id=AIRFLOW_ELK_DAG_NAME,
        include_prior_dates=True,
    )

    current_date = datetime.today().strftime("%Y%m%d%H%M%S")
    snapshot_name = f"siren-{current_date}"

    logging.info(
        f"Snapshot {elastic_index} into {ELASTIC_SNAPSHOT_REPOSITORY}/{snapshot_name}"
    )

    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )

    elastic_connection = connections.get_connection()

    elastic_connection.snapshot.create(
        repository=ELASTIC_SNAPSHOT_REPOSITORY,
        snapshot=snapshot_name,
        body={"indices": [elastic_index], "include_global_state": False},
        wait_for_completion=False,
    )

    waited_for = 0
    interval = 5
    timeout = 7200

    while waited_for < timeout:
        time.sleep(interval)
        waited_for += interval

        snapshots = elastic_connection.snapshot.get(
            repository=ELASTIC_SNAPSHOT_REPOSITORY,
            snapshot=snapshot_name,
            ignore_unavailable=True,
        )

        if len(snapshots["snapshots"]) > 0:
            if snapshots["snapshots"][0]["state"] == "SUCCESS":
                kwargs["ti"].xcom_push(key="snapshot_name", value=snapshot_name)

                return

            if snapshots["snapshots"][0]["state"] != "IN_PROGRESS":
                raise Exception("The snapshot failed")

    raise Exception("The snapshot is taking too long")


def delete_old_snapshots(**kwargs):
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
    )

    elastic_connection = connections.get_connection()

    snapshots = elastic_connection.snapshot.get(
        repository=ELASTIC_SNAPSHOT_REPOSITORY,
        snapshot="*",
        ignore_unavailable=True,
    )

    snapshots = list(
        sorted(snapshots["snapshots"], key=lambda snapshot: snapshot["start_time"])
    )
    snapshots_to_remove = snapshots[:-ELASTIC_SNAPSHOT_MAX_REVISIONS]

    for snapshot in snapshots_to_remove:
        logging.info(
            f"Deleting snapshot {snapshot['snapshot']} from {ELASTIC_SNAPSHOT_REPOSITORY}"
        )

        try:
            elastic_connection.snapshot.delete(
                repository=ELASTIC_SNAPSHOT_REPOSITORY, snapshot=snapshot["snapshot"]
            )
        except Exception as e:
            logging.error(f"Failed to delete the snapshot {snapshot['snapshot']}: {e}")