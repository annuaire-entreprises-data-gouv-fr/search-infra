import logging

import pandas as pd

from data_pipelines_annuaire.config import DataSourceConfig
from data_pipelines_annuaire.helpers.sqlite_client import SqliteClient


class DatabaseTableConstructor:
    """
    Class for creating a SQLite data warehouse from all the preprocessed open data sources.
    """

    def __init__(self, config: DataSourceConfig) -> None:
        self.config = config

    def etl_get_preprocessed_data(self) -> pd.DataFrame:
        if self.config.url_object_storage:
            return pd.read_csv(self.config.url_object_storage, dtype=str)
        else:
            raise ValueError("No object storage URL provided in the configuration.")

    def etl_create_table(self, db_location: str) -> None:
        """
        Creates a table in the SQLite database from the CSV output stored on the object storage.

        Configurations:
            - self.config.table_ddl (str): The DDL to create the table in SQLite.
            - self.config.name (str): The name of the table to create.

        Args:
            db_location (str): The SQLite database location to target.
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

        if self.config.post_processing_queries:
            self._etl_post_processing(db_location)

    def _etl_post_processing(self, db_location: str) -> None:
        """
        Execute queries after the table creation.
        Especially useful to apply updates requiring joins on other tables.

        Configurations:
            - self.config.post_processing_queries (list[str]):
                The queries to execute in order.
            - self.config.name (str): The name of the table to update.

        Args:
            db_location (str): The SQLite database location to target.
        """
        if not self.config.post_processing_queries:
            raise ValueError(
                "No post-processing queries provided in the configuration."
            )
        if not self.config.name:
            raise ValueError("No table name provided in the configuration.")
        queries = self.config.post_processing_queries
        if isinstance(queries, str):
            queries = [queries]

        with SqliteClient(db_location) as sqlite_client:
            logging.info(
                f"Execution post-processing queries on {self.config.name} table.."
            )
            previous_total = 0
            for i, query in enumerate(queries):
                sqlite_client.execute(query)
                current_total = sqlite_client.db_conn.total_changes
                logging.info(
                    f"post-processing query {i + 1}: {current_total - previous_total} rows updated"
                )
                previous_total = current_total
