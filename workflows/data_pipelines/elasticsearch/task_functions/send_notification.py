from dag_datalake_sirene.helpers.mattermost import send_message
from dag_datalake_sirene.helpers.tchap import send_message_tchap


def send_notification_success_mattermost(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count", task_ids="fill_elastic_siren_index"
    )
    send_message(
        f"🟢 Données :"
        f"\nDAG d'indexation a été exécuté avec succès."
        f"\n - Nombre de documents indexés : {doc_count}"
    )


def send_notification_failure_mattermost(context):
    send_message("🔴 Données :\nFail DAG d'indexation!!!!")


def send_notification_success_tchap(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count", task_ids="fill_elastic_siren_index"
    )
    send_message_tchap()(
        f"\U0001f7e2 Données :"
        f"\nDAG d'indexation a été exécuté avec succès."
        f"\n - Nombre de documents indexés : {doc_count}"
    )


def send_notification_failure_tchap(context):
    send_message_tchap("\U0001f534 Données :\nFail DAG d'indexation!!!!")
