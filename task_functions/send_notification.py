from dag_datalake_sirene.utils.tchap import send_message


def send_notification_preprocessing_success_tchap(**kwargs):
    send_message(
        "\U0001F7E2 Données :" "\nDAG de preprocessing a été exécuté avec succès."
    )


def send_notification_indexing_success_tchap(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count", task_ids="fill_elastic_siren_index"
    )
    send_message(
        f"\U0001F7E2 Données :"
        f"\nDAG d'indexation a été exécuté avec succès."
        f"\n - Nombre de documents indexés : {doc_count}"
    )


def send_notification_preprocessing_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG de preprocessing sirene!!!!")


def send_notification_indexation_failure_tchap(context):
    send_message("\U0001F534 Données :" "\nFail DAG d'indexation!!!!")
