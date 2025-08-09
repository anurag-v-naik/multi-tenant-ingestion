from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import logging
import time
from datetime import datetime


class ConnectorError(Exception):
    """Custom exception for connector-related errors"""
    pass


class BaseConnector(ABC):
    """Abstract base class for all data connectors"""

    def __init__(self, config: Dict[str, Any], tenant_id: str):
        self.config = config
        self.tenant_id = tenant_id
        self.logger = logging.getLogger(f"{self.__class__.__name__}:{tenant_id}")
        self.connection = None
        self.is_connected = False
        self.last_health_check = None

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to data source"""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to data source"""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test if connection is working"""
        pass

    @abstractmethod
    def extract_data(self, query: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Extract data from source based on query"""
        pass

    @abstractmethod
    def get_schema(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """Get schema information"""
        pass

    @abstractmethod
    def get_table_list(self) -> List[str]:
        """Get list of available tables/collections"""
        pass

    def validate_tenant_isolation(self) -> bool:
        """Ensure tenant data isolation - override in subclasses if needed"""
        # Default implementation - log tenant access
        self.logger.info(f"Accessing data for tenant: {self.tenant_id}")
        return True

    def health_check(self) -> Dict[str, Any]:
        """Perform health check on the connector"""
        try:
            start_time = time.time()
            is_healthy = self.test_connection()
            response_time = time.time() - start_time

            self.last_health_check = datetime.utcnow()

            return {
                "healthy": is_healthy,
                "response_time_ms": round(response_time * 1000, 2),
                "last_check": self.last_health_check.isoformat(),
                "connector_type": self.__class__.__name__,
                "tenant_id": self.tenant_id
            }
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return {
                "healthy": False,
                "error": str(e),
                "connector_type": self.__class__.__name__,
                "tenant_id": self.tenant_id
            }

    def get_metadata(self) -> Dict[str, Any]:
        """Get connector metadata"""
        return {
            "connector_type": self.__class__.__name__,
            "tenant_id": self.tenant_id,
            "config_keys": list(self.config.keys()),
            "is_connected": self.is_connected,
            "last_health_check": self.last_health_check.isoformat() if self.last_health_check else None
        }
