from dag_datalake_sirene.helpers.tchap import send_message


def send_notification_success_tchap(**kwargs):
    send_message(
        "\U0001F7E2 Données :" "\nDAG de preprocessing a été exécuté avec succès."
    )


def send_notification_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG de preprocessing sirene!!!!")
