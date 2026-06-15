import logging
import os
import sqlite3

import pandas as pd

from data_pipelines_annuaire.helpers.filesystem import LocalFile


class SqliteClient:
    """
    A client for interacting with a SQLite database.

    Attributes:
        db_location (str): The file path to the SQLite database.
        db_folder (str): The directory containing the SQLite database file.
        db_conn (sqlite3.Connection): The SQLite database connection object.
        db_cursor (sqlite3.Cursor): The SQLite database cursor object.

    Args:
        db_location (str): The file path to the SQLite database. The database file will be created if it does not exist.
        timeout (int, optional): The timeout duration for database operations. Defaults to 30 seconds.

    Example:
        ```python

        db_location = "/path/to/database.db"
        with SqliteClient(db_location) as sqlite_client:
            # Create a table
            sqlite_client.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
            sqlite_client.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))

            # Query the table
            result = sqlite_client.execute("SELECT * FROM users")
            for row in result:
                print(row)

            # Get the count of rows in the table
            row_count = sqlite_client.get_table_count('users')
            print(f"Total users: {row_count}")
        ```
    """

    def __init__(self, db_location, timeout=30) -> None:
        self.db_location = db_location

        # SQLite creates the database if it does not exist but not the parent folders
        self.db_folder = os.path.dirname(self.db_location)
        if not os.path.exists(self.db_folder):
            os.makedirs(self.db_folder)

        self.db_conn = sqlite3.connect(self.db_location, timeout=timeout)
        logging.info(
            f"*********** Connecting to database {self.db_location}! ***********"
        )
        self.db_cursor = self.db_conn.cursor()

    def __enter__(self) -> "SqliteClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type:
            # Exception raised, rollback the transaction
            self.db_conn.rollback()
        else:
            self.db_conn.commit()
        self.db_conn.close()

    def commit_and_close_conn(self) -> None:
        self.db_conn.commit()
        self.db_conn.close()

    def execute(self, query, params=None) -> sqlite3.Cursor:
        if params:
            return self.db_cursor.execute(query, params)
        return self.db_cursor.execute(query)

    def execute_many(self, query, params) -> sqlite3.Cursor:
        return self.db_cursor.executemany(query, params)

    def execute_script(self, query) -> sqlite3.Cursor:
        return self.db_cursor.executescript(query)

    def connect_to_another_db(self, db_to_connect, db_alias) -> None:
        self.execute(f"ATTACH DATABASE '{db_to_connect}' AS '{db_alias}'")

    def detach_database(self, db_alias) -> None:
        self.execute(f"DETACH DATABASE '{db_alias}'")

    def drop_table(self, table_name: str) -> None:
        self.execute(f"DROP TABLE IF EXISTS {table_name};")

    def get_table_count(self, table_name: str) -> int:
        result = self.execute(f"SELECT COUNT(*) FROM {table_name};")
        return result.fetchone()[0]

    def to_csv(self, query: str, file_fullpath: str) -> LocalFile:
        """Génère un fichier CSV à partir d'une requête SQL exécutée sur la base SQLite."""
        df = pd.read_sql_query(query, self.db_conn)

        df.to_csv(f"{file_fullpath}", index=False)
        logging.info(f"File {file_fullpath} created with {len(df)} rows.")

        return LocalFile(file_fullpath)
