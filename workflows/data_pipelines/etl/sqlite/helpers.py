from dag_datalake_sirene.config import (
    SIRENE_DATABASE_LOCATION,
)
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient


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


def execute_query(
    query,
):
    sqlite_client = SqliteClient(SIRENE_DATABASE_LOCATION)
    sqlite_client.execute(query)
    sqlite_client.commit_and_close_conn()
