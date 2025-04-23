import logging
from typing import Optional

from elasticsearch_dsl import Index, connections

from dag_datalake_sirene.workflows.data_pipelines.elasticsearch.mapping_index import (
    StructureMapping,
)


class ElasticCreateIndex:
    """
    Create elasticsearch Index
    :param elastic_url: endpoint url of elasticsearch
    :type elastic_url: str
    :param elastic_index: index to create
    :type elastic_index: str
    :param elastic_index_shards: number of shards for index
    :type elastic_index_shards: int
    :param elastic_user: user for elasticsearch
    :type elastic_user: str
    :param elastic_password: password for elasticsearch
    :type elastic_password: str
    """

    def __init__(
        self,
        *,
        elastic_url: Optional[str] = None,
        elastic_index: Optional[str] = None,
        elastic_user: Optional[str] = None,
        elastic_password: Optional[str] = None,
        elastic_bulk_size: Optional[int] = 1500,
        **kwargs,
    ) -> None:
        self.elastic_url = elastic_url
        self.elastic_index = elastic_index
        self.elastic_user = elastic_user
        self.elastic_password = elastic_password
        self.elastic_bulk_size = elastic_bulk_size

        # initiate the default connection to elasticsearch
        connections.create_connection(
            hosts=[self.elastic_url],
            http_auth=(self.elastic_user, self.elastic_password),
            retry_on_timeout=True,
        )

        self.elastic_connection = connections.get_connection()
        self.elastic_health = self.elastic_connection.cluster.health()
        self.elastic_status = self.elastic_health["status"]
        self.elastic_mapping = self.elastic_connection.indices.get_mapping()

        logging.info("Elasticsearch connection initiated!")

    def check_health(self):
        if self.elastic_status not in ("green", "yellow"):
            raise Exception(
                f"Cluster status is {self.elastic_status}, not green nor yellow!!"
            )
        else:
            logging.info(f"Cluster status is functional: {self.elastic_status}")

    def execute(self):
        self.check_health()

        if not self.elastic_url:
            raise ValueError("Please provide elasticsearch url endpoint")

        # if self.elastic_index_shards is not None:
        if Index(self.elastic_index).exists():
            logging.info(f"Index  {self.elastic_index} already exists! Deleting...")
            Index(self.elastic_index).delete()
            logging.info(f"Index {self.elastic_index} deleted!")
        logging.info(f"Creating {self.elastic_index} index!")
        # Create the mapping in elasticsearch
        StructureMapping.init(index=self.elastic_index)
