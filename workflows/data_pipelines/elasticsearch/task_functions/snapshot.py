import logging
import time
from datetime import datetime

from airflow.sdk import get_current_context, task
from elasticsearch_dsl import connections

from data_pipelines_annuaire.config import (
    AIRFLOW_ELK_DAG_NAME,
    AIRFLOW_ENV,
    ELASTIC_PASSWORD,
    ELASTIC_SNAPSHOT_MAX_REVISIONS,
    ELASTIC_SNAPSHOT_OBJECT_STORAGE_STATE_PATH,
    ELASTIC_SNAPSHOT_REPOSITORY,
    ELASTIC_URL,
    ELASTIC_USER,
)
from data_pipelines_annuaire.helpers.filesystem import (
    Filesystem,
    JsonSerializer,
)

filesystem = Filesystem(
    f"ae/{AIRFLOW_ENV}/{ELASTIC_SNAPSHOT_OBJECT_STORAGE_STATE_PATH}/",
    JsonSerializer(),
)


@task
def update_object_storage_current_index_version():
    """
    An history of any new successfully indexed siren index is kept on a object storage bucket and stored inside a "daily" folder.

    The file structure is as follow:
        current:
            file: reference the daily object storage file containing the current state
            index: the index name that should be restored by each downstream server
            snapshot: the name of the Elasticsearch snapshot where the index to restore can be found

        previous:
            file: reference the daily object storage file containing the previous state that can be used to rollback the live index
            index: name of the previous live index
            snapshot: the name of the Elasticsearch snapshot containing the previous index

    A "current.json" file is also uploaded and can be used by any downstream server to restore the new live index.

    The snapshot/restore process of the new index is as follow :
        1. Airflow : create and index a date-versioned siren index
        2. Airflow : create a date-versioned Elasticsearch snapshot containing the new date-versioned siren index
        3. this function : upload a daily object storage file containing the new state
        4. this function : upload the current.json object storage file
        5. Any downstream server : read the current.json file and import the indicated current['index'] using the indicated['snapshot']
    """

    current_date = datetime.today().strftime("%Y%m%d%H%M%S")
    content = filesystem.read("current.json")

    if content is None:
        content = {}

    if "current" in content:
        content["previous"] = content["current"]

    ti = get_current_context()["ti"]
    content["current"] = {
        "file": f"daily/{current_date}.json",
        "index": ti.xcom_pull(
            key="elastic_index",
            task_ids="get_next_index_name",
            dag_id=AIRFLOW_ELK_DAG_NAME,
            include_prior_dates=True,
        ),
        "snapshot": ti.xcom_pull(
            key="snapshot_name",
            task_ids="snapshot_elastic_index",
        ),
    }

    filesystem.write(f"daily/{current_date}.json", content)
    filesystem.write("current.json", content)


@task
def rollback_object_storage_current_index_version():
    content = filesystem.read("current.json")

    if content is None:
        raise Exception("No previous version found")

    previous = filesystem.read(content["current"]["file"])

    if previous is None:
        raise Exception("No previous version found")

    elastic_connection = connections.get_connection()

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
    filesystem.write("current.json", previous)

    ti = get_current_context()["ti"]
    ti.xcom_push(key="elastic_index", value=previous["current"]["index"])


@task
def snapshot_elastic_index():
    """
    Create and save Elastic index snapshot in the object storage.

    https://www.elastic.co/guide/en/elasticsearch/reference/7.17/snapshot-restore.html
    """

    ti = get_current_context()["ti"]
    previous_elastic_index = ti.xcom_pull(
        key="elastic_index",
        task_ids="get_next_index_name",
        dag_id=AIRFLOW_ELK_DAG_NAME,
        include_prior_dates=True,
    )

    # Ensure elastic_index is a string
    if isinstance(previous_elastic_index, list):
        elastic_index = max(previous_elastic_index)
    else:
        elastic_index = previous_elastic_index

    ti.xcom_push(key="elastic_index", value=elastic_index)

    logging.info(
        "elastic_index has to be a string"
        f"\nprevious_elastic_index type: {type(previous_elastic_index)}, value: {previous_elastic_index}"
        f"\nelastic_index type: {type(elastic_index)}, value: {elastic_index}"
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
                ti.xcom_push(key="snapshot_name", value=snapshot_name)
                return

            if snapshots["snapshots"][0]["state"] != "IN_PROGRESS":
                raise Exception("The snapshot failed")

    raise Exception("The snapshot is taking too long")


@task
def delete_old_snapshots():
    connections.create_connection(
        hosts=[ELASTIC_URL],
        http_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        retry_on_timeout=True,
        timeout=60,
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
