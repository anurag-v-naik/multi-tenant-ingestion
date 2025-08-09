import aiomysql
from typing import Dict, Any, List, Optional
from ...connectors.base import BaseConnector, ConnectorType, DataFormat, ValidationResult, TestResult, DataPreview
from pydantic import BaseModel, Field
import time


class MySQLConfig(BaseModel):
    host: str = Field(..., description="Database host")
    port: int = Field(3306, description="Database port")
    database: str = Field(..., description="Database name")
    charset: str = Field("utf8mb4", description="Character set")
    autocommit: bool = Field(True, description="Auto commit transactions")


class MySQLCredentials(BaseModel):
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")


class MySQLConnector(BaseConnector):

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.RDBMS

    @property
    def supported_formats(self) -> List[DataFormat]:
        return [DataFormat.JSON, DataFormat.CSV]

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        return MySQLConfig.model_json_schema()

    @classmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        return MySQLCredentials.model_json_schema()

    async def connect( self) -> aiomysql.Connection:
        config = MySQLConfig(**self.config)
        credentials = MySQLCredentials(**self.credentials)
        conn = await aiomysql.connect(
            host=config.host,
            port=config.port,
            user=credentials.username,
            password=credentials.password,
            db=config.database,
            charset=config.charset,
            autocommit=config.autocommit
        )
        return conn