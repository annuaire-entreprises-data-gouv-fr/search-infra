import logging
import pandas as pd

from dag_datalake_sirene.helpers.sqlite_client import SqliteClient
from dag_datalake_sirene.config import DataSourceConfig


class DataWarehouseProcessor:
    """
    Class for creating a SQLite data warehouse from all the preprocessed open data sources.
    """

    def __init__(self, config: DataSourceConfig) -> None:
        self.config = config

    def etl_get_preprocessed_data(self) -> pd.DataFrame:
        if self.config.url_minio:
            return pd.read_csv(self.config.url_minio, dtype=str)
        else:
            raise ValueError("No MinIO URL provided in the configuration.")

    def etl_create_table(self, sqlite_client: SqliteClient) -> None:
        """
        Creates a table in the SQLite database and inserts the preprocessed data.

        Args:
            sqlite_client (SqliteClient): The SQLite client to use.
        """
        sqlite_client.drop_table(self.config.name)
        if self.config.table_ddl:
            sqlite_client.execute_script(self.config.table_ddl)
        else:
            raise ValueError("No table DDL provided in the configuration.")
        df_table = self.etl_get_preprocessed_data()
        df_table.to_sql(
            self.config.name, sqlite_client.db_conn, if_exists="append", index=False
        )
        del df_table
        for row in sqlite_client.get_table_count(self.config.name):
            logging.info(
                f"************ {row} total records have been added to the "
                f"{self.config.name} table!"
            )
        sqlite_client.commit_and_close_conn()
