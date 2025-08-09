from typing import Dict, Type, List
from ..connectors.base import BaseConnector
from ..connectors.rdbms.postgresql import PostgreSQLConnector
from ..connectors.rdbms.mysql import MySQLConnector
from ..connectors.cloud.s3 import S3Connector
from ..connectors.streaming.kafka import KafkaConnector
from ..connectors.nosql.mongodb import MongoDBConnector
from ..connectors.warehouse.snowflake import SnowflakeConnector


class ConnectorRegistry:
    """Registry for all available connectors"""

    def __init__(self):
        self._connectors: Dict[str, Type[BaseConnector]] = {}
        self._register_default_connectors()

    def _register_default_connectors(self):
        """Register all default connectors"""
        # RDBMS
        self.register("postgresql", PostgreSQLConnector)
        self.register("mysql", MySQLConnector)

        # Cloud Storage
        self.register("s3", S3Connector)

        # Streaming
        self.register("kafka", KafkaConnector)

        # NoSQL
        self.register("mongodb", MongoDBConnector)

        # Data Warehouses
        self.register("snowflake", SnowflakeConnector)

    def register(self, name: str, connector_class: Type[BaseConnector]):
        """Register a new connector"""
        self._connectors[name] = connector_class

    def get_connector(self, name: str) -> Type[BaseConnector]:
        """Get a connector class by name"""
        if name not in self._connectors:
            raise ValueError(f"Connector '{name}' not found")
        return self._connectors[name]

    def list_connectors(self) -> List[Dict[str, str]]:
        """List all available connectors"""
        return [
            {
                "name": name,
                "type": connector_class.connector_type.value,
                "supported_formats": [fmt.value for fmt in connector_class.supported_formats]
            }
            for name, connector_class in self._connectors.items()
        ]

    def create_connector(self, name: str, config: Dict, credentials: Dict) -> BaseConnector:
        """Create a connector instance"""
        connector_class = self.get_connector(name)
        return connector_class(config, credentials)


# Global registry instance
connector_registry = ConnectorRegistry()
