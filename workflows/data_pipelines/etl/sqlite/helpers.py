import logging


from dag_datalake_sirene.helpers.sqlite_client import SqliteClient


from dag_datalake_sirene.config import (
    AIRFLOW_ETL_DATA_DIR,
    SIRENE_DATABASE_LOCATION,
)


def drop_table(name):
    return f"""DROP TABLE IF EXISTS {name}"""


def create_unique_index(index_name, table_name, column):
    return f"""CREATE UNIQUE INDEX {index_name}
        ON {table_name} ({column});"""


def create_index(index_name, table_name, column):
    return f"""CREATE INDEX {index_name}
        ON {table_name} ({column});"""


def get_distinct_column_count(table, column):
    return f"""SELECT count(DISTINCT {column}) FROM {table};"""


def get_table_count(name):
    return f"""SELECT COUNT() FROM {name};"""


def create_and_fill_table_model(
    table_name,
    create_table_query,
    create_index_func,
    index_name,
    index_column,
    preprocess_table_data,
):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(drop_table(table_name))
    sqlite_client.execute(create_table_query)
    sqlite_client.execute(create_index_func(index_name, table_name, index_column))
    if table_name == "convention_collective":
        sqlite_client.execute(
            create_index("index_siren_convention_collective", table_name, "siren")
        )
    df_table = preprocess_table_data(data_dir=AIRFLOW_ETL_DATA_DIR)
    df_table.to_sql(table_name, sqlite_client.db_conn, if_exists="append", index=False)
    del df_table
    for row in sqlite_client.execute(get_table_count(table_name)):
        logging.info(
            f"************ {row} total records have been added to the "
            f"{table_name} table!"
        )
    sqlite_client.commit_and_close_conn()


def create_table_model(
    table_name,
    create_table_query,
    create_index_func,
    index_name,
    index_column,
):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(drop_table(table_name))
    sqlite_client.execute(create_table_query)
    sqlite_client.execute(create_index_func(index_name, table_name, index_column))
    return sqlite_client


def create_only_index(
    table_name,
    create_index_func,
    index_name,
    index_column,
):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(create_index_func(index_name, table_name, index_column))
    sqlite_client.commit_and_close_conn()


def execute_query(
    query,
):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(query)
    sqlite_client.commit_and_close_conn()
