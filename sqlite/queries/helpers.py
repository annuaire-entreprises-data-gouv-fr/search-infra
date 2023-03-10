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
