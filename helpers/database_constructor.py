import logging

import pandas as pd

from dag_datalake_sirene.config import DataSourceConfig
from dag_datalake_sirene.helpers.sqlite_client import SqliteClient


class DatabaseTableConstructor:
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

    def etl_create_table(self, db_location: str) -> None:
        """
        Creates a table in the SQLite database from the CSV output stored on minio.

        Configurations:
            - self.config.table_ddl (str): The DDL to create the table in SQLite.
            - self.config.name (str): The name of the table to create.

        Args:
            sqlite_client (SqliteClient): The SQLite client to use.
        """
        if not self.config.table_ddl:
            raise ValueError("No table DDL provided in the configuration.")
        if not self.config.name:
            raise ValueError("No table name provided in the configuration.")

        df_table = self.etl_get_preprocessed_data()

        with SqliteClient(db_location) as sqlite_client:
            logging.info(f"Creating {self.config.name} table..")
            sqlite_client.drop_table(self.config.name)
            sqlite_client.execute_script(self.config.table_ddl)
            df_table.to_sql(
                self.config.name,
                sqlite_client.db_conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10000,
            )
            row_count = sqlite_client.get_table_count(self.config.name)

        logging.info(
            f"************ {row_count} total records have been added to the "
            f"{self.config.name} table!"
        )
