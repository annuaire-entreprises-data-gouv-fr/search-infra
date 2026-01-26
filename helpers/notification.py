import logging
from enum import Enum

import requests

from data_pipelines_annuaire.config import (
    AIRFLOW_API_BASE_URL,
    AIRFLOW_API_BEARER_TOKEN,
    AIRFLOW_ENV,
)
from data_pipelines_annuaire.helpers import mattermost


def monitoring_logger(key: str, value: int) -> None:
    """
    Send logs to Kibana with the specified key and metric.
    Kibana expect an info log with the following format:
        ::STATS:: KEY:XXX VALUE:1234

    Args:
        key (str): The key for the log entry.
        metric (int): The metric value for the log entry.
    """
    logging.info(f"::STATS:: KEY:{key} VALUE:{value}")


class Notification:
    """
    Class to manage and send end of DAG notifications to Mattermost.

    Methods:
        send_notification_mattermost() -> None:
            Sends a notification to Mattermost with the following format:
                🔴 dagA: Données
                - N rows were updated.
                - task2(failed)

    Usage:
        Add the following parameters to a DAG definition:
            >> on_failure_callback=Notification.send_notification_mattermost,
            >> on_success_callback=Notification.send_notification_mattermost,

        [Optional] In the relevant @task, use the following code to provide additional
        context for the notification:
            >> from data_pipelines_annuaire.helpers import Notification
            >> ti.xcom_push(key=Notification.notification_xcom_key, value=error_message)
    """

    notification_xcom_key = "notification_message"

    class Status(str, Enum):
        SUCCESS = ":large_green_circle:"
        WARNING = ":large_orange_circle:"
        RUNNING = ":arrow_forward:"
        FAILURE = ":red_circle:"

    def __init__(self, context) -> None:
        self.ti = context["ti"]
        dag_run = context.get("dag_run")
        self.dag_id = dag_run.dag_id
        self.run_id = dag_run.run_id

        if dag_run.state == "success":
            self.status = self.Status.SUCCESS
        elif dag_run.state == "failed":
            self.status = self.Status.FAILURE
        elif dag_run.state == "running":
            self.status = self.Status.RUNNING
        else:
            self.status = self.Status.WARNING

        self.status_name = self.status.name

    def _get_task_instances(self):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {AIRFLOW_API_BEARER_TOKEN}",
        }

        url = f"http://{AIRFLOW_API_BASE_URL}/api/v2/dags/{self.dag_id}/dagRuns/{self.run_id}/taskInstances"
        tasks_resp = requests.get(
            url,
            headers=headers,
        )
        tasks_resp.raise_for_status()

        return sorted(
            tasks_resp.json()["task_instances"],
            key=lambda ti: (ti.get("end_date") is None, ti.get("end_date", "")),
        )

    def generate_notification_message(self) -> str:
        additional_messages = self.get_dag_additional_messages()
        if additional_messages:
            additional_messages_str = "\n" + "\n".join(additional_messages)
        else:
            additional_messages_str = ""
        return f"{self.status.value} airflow : {self.dag_id} {additional_messages_str}"

    def get_dag_additional_messages(self) -> list[str]:
        """
        Generate a message from all the "notification_message" keys and failed tasks.
        """
        notification_messages = []
        task_instances = self._get_task_instances()
        for task in task_instances:
            notification_message = self.ti.xcom_pull(
                task_ids=task["task_id"], key=self.notification_xcom_key
            )
            if notification_message is not None:
                notification_messages.append(f"- {notification_message}")
            elif notification_message is None and task["state"] == "failed":
                # Add warning emoji only if the overall DAG run
                # is successful to increase visibility
                warning_emoji = (
                    ":warning: " if self.status == self.Status.SUCCESS else ""
                )
                notification_messages.append(
                    f"- {warning_emoji}{task['task_id']}({task['state']})"
                )

        return notification_messages

    def send_mattermost_notification(self) -> None:
        if AIRFLOW_ENV != "prod":
            return None
        message = self.generate_notification_message()
        logging.info(f"Notification sent to Mattermost:\n{message}")
        mattermost.send_message(message)

    @classmethod
    def send_notification_mattermost(cls, context):
        Notification(context).send_mattermost_notification()
