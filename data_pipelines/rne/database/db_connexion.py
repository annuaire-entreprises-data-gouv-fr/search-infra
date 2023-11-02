import sqlite3


def connect_to_db(database_location):
    connection = sqlite3.connect(database_location)
    cursor = connection.cursor()
    return connection, cursor
