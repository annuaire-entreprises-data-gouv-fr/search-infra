import logging
import sqlite3


class SqliteClient:
    # Connect to database
    def __init__(self, db_location):
        self.db_location = db_location
        self.db_conn = sqlite3.connect(self.db_location)
        logging.info(
            f"*********** Connecting to database {self.db_location}! " f"***********"
        )
        self.db_cursor = self.db_conn.cursor()

    def commit_and_close_conn(self):
        self.db_conn.commit()
        self.db_conn.close()

    def execute(self, query):
        return self.db_cursor.execute(query)
