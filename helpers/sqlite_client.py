import logging
import sqlite3


class SqliteClient:
    # Connect to database
    def __init__(self, db_location, timeout=30) -> None:
        self.db_location = db_location
        self.db_conn = sqlite3.connect(self.db_location, timeout=timeout)
        logging.info(
            f"*********** Connecting to database {self.db_location}! " f"***********"
        )
        self.db_cursor = self.db_conn.cursor()

    def commit_and_close_conn(self) -> None:
        self.db_conn.commit()
        self.db_conn.close()

    def execute(self, query, params=None) -> sqlite3.Cursor:
        if params:
            return self.db_cursor.execute(query, params)
        return self.db_cursor.execute(query)

    def executemany(self, query, params) -> sqlite3.Cursor:
        return self.db_cursor.executemany(query, params)

    def execute_script(self, query) -> sqlite3.Cursor:
        return self.db_cursor.executescript(query)

    def connect_to_another_db(self, db_to_connect, db_alias) -> None:
        self.execute(f"ATTACH DATABASE '{db_to_connect}' AS '{db_alias}'")

    def detach_database(self, db_alias) -> None:
        self.execute(f"DETACH DATABASE '{db_alias}'")

    def drop_table(self, table_name: str) -> None:
        self.execute(f"DROP TABLE IF EXISTS {table_name};")

    def get_table_count(self, table_name: str) -> sqlite3.Cursor:
        return self.execute(f"SELECT COUNT(*) FROM {table_name};")
