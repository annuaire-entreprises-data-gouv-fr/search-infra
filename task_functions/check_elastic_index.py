import logging


def check_elastic_index(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count", task_ids="fill_elastic_siren_index"
    )
    logging.info(f"******************** Documents indexed: {doc_count}")

    if float(doc_count) < 26000000:
        raise ValueError(
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed."
        )
