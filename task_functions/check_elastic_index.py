import logging


def check_elastic_index(**kwargs):
    doc_count = kwargs["ti"].xcom_pull(
        key="doc_count_sirene", task_ids="fill_elastic_index_sirene"
    )
    count_sieges = kwargs["ti"].xcom_pull(
        key="count_sieges", task_ids="create_siege_only_table"
    )

    logging.info(f"******************** Documents indexed: {doc_count}")

    if float(count_sieges) - float(doc_count) > 100000:
        raise ValueError(
            f"*******The data has not been correctly indexed: "
            f"{doc_count} documents indexed instead of {count_sieges}."
        )
