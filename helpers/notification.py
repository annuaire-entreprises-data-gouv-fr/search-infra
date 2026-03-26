import logging
from enum import Enum

from airflow.sdk import BaseNotifier

from data_pipelines_annuaire.helpers import AirflowApiClient, tchap


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


class Notification(BaseNotifier):
    """
    Class to manage and send end of DAG notifications to Tchap.
    Must read documentation: https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/notifications.html

    Methods:
        notify() -> None:
            Sends a notification to Tchap with the following format:
                🔴 dagA: Données
                - N rows were updated.
                - task2(failed)

    Usage:
        Add the following parameters to a DAG definition:
            >> on_failure_callback=Notification(),
            >> on_success_callback=Notification(),

        [Optional] In the relevant @task, use the following code to provide additional
        context for the notification:
            >> from data_pipelines_annuaire.helpers import Notification
            >> ti.xcom_push(key=Notification.notification_xcom_key, value=error_message)
    """

    notification_xcom_key = "notification_message"

    class Status(str, Enum):
        SUCCESS = "🟢"
        WARNING = "🟠"
        RUNNING = "▶️"
        FAILURE = "🔴"

    def notify(self, context):
        if "ti" not in context or "dag_run" not in context:
            raise ValueError(f"ti or dag_run not available in contex:\n{context}")

        ti = context["ti"]
        dag_run = context["dag_run"]
        dag_id = dag_run.dag_id
        run_id = dag_run.run_id

        tchap_message_type = "text"
        if dag_run.state == "success":
            status = self.Status.SUCCESS
            tchap_message_type = "notice"
        elif dag_run.state == "running":
            status = self.Status.RUNNING
            tchap_message_type = "notice"
        elif dag_run.state == "failed":
            status = self.Status.FAILURE
        else:
            status = self.Status.WARNING

        task_instances = AirflowApiClient().get_task_instances(dag_id, run_id)

        additional_messages = []
        for task in task_instances:
            notification_message = ti.xcom_pull(
                task_ids=task["task_id"], key=self.notification_xcom_key
            )
            if notification_message is not None:
                additional_messages.append(notification_message)
            elif notification_message is None and task["state"] == "failed":
                # Add warning emoji only if the overall DAG run
                # is successful to increase visibility
                warning_emoji = "⚠️ " if status == self.Status.SUCCESS else ""
                additional_messages.append(
                    f"{warning_emoji}{task['task_id']}({task['state']})"
                )

        if additional_messages:
            additional_messages_str = (
                "<ul>"
                + "".join(f"<li>{message}</li>" for message in additional_messages)
                + "</ul>"
            )
        else:
            additional_messages_str = ""
        message = f"<p>{status.value} airflow : {dag_id}</p>{additional_messages_str}"

        tchap.send_message(message, tchap_message_type)
