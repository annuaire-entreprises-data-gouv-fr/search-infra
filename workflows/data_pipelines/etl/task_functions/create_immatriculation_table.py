from airflow.decorators import task

from dag_datalake_sirene.config import (
    RNE_DATABASE_LOCATION,
    SIRENE_DATABASE_LOCATION,
)
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient


@task
def copy_immatriculation_table():
    # Connect to the destination database
    sqlite_client_siren = SqliteClient(SIRENE_DATABASE_LOCATION)

    # Attach the RNE database
    sqlite_client_siren.connect_to_another_db(RNE_DATABASE_LOCATION, "db_rne")

    table_name = "immatriculation"

    # Create table with the same structure as the source table immatriculation
    sqlite_client_siren.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {table_name} AS
    SELECT * FROM db_rne.{table_name} WHERE 1=0
    """
    )

    # Insert distinct rows directly
    sqlite_client_siren.execute(
        f"""
    INSERT INTO {table_name}
    SELECT DISTINCT * FROM db_rne.{table_name}
    """
    )

    sqlite_client_siren.execute(
        f"""CREATE INDEX IF NOT EXISTS idx_siren_immat
        ON {table_name} (siren);"""
    )
    sqlite_client_siren.db_conn.commit()

    # Detach the source database
    sqlite_client_siren.detach_database("db_rne")

    # Commit changes and close the connection
    sqlite_client_siren.commit_and_close_conn()
