import logging
from enum import Enum

from airflow.sdk import BaseNotifier

from data_pipelines_annuaire.helpers import AirflowApiClient, mattermost


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
    Class to manage and send end of DAG notifications to Mattermost.
    Must read documentation: https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/notifications.html

    Methods:
        notify() -> None:
            Sends a notification to Mattermost with the following format:
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
        SUCCESS = ":large_green_circle:"
        WARNING = ":large_orange_circle:"
        RUNNING = ":arrow_forward:"
        FAILURE = ":red_circle:"

    def notify(self, context):
        if "ti" not in context or "dag_run" not in context:
            raise ValueError(f"ti or dag_run not available in contex:\n{context}")

        ti = context["ti"]
        dag_run = context["dag_run"]
        dag_id = dag_run.dag_id
        run_id = dag_run.run_id

        if dag_run.state == "success":
            status = self.Status.SUCCESS
        elif dag_run.state == "failed":
            status = self.Status.FAILURE
        elif dag_run.state == "running":
            status = self.Status.RUNNING
        else:
            status = self.Status.WARNING

        task_instances = AirflowApiClient().get_task_instances(dag_id, run_id)

        additional_messages = []
        for task in task_instances:
            notification_message = ti.xcom_pull(
                task_ids=task["task_id"], key=self.notification_xcom_key
            )
            if notification_message is not None:
                additional_messages.append(f"- {notification_message}")
            elif notification_message is None and task["state"] == "failed":
                # Add warning emoji only if the overall DAG run
                # is successful to increase visibility
                warning_emoji = ":warning: " if status == self.Status.SUCCESS else ""
                additional_messages.append(
                    f"- {warning_emoji}{task['task_id']}({task['state']})"
                )

        if additional_messages:
            additional_messages_str = "\n" + "\n".join(additional_messages)
        else:
            additional_messages_str = ""
        message = f"{status.value} airflow : {dag_id} {additional_messages_str}"

        mattermost.send_message(message)
