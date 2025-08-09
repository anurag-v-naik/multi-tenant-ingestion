from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from enum import Enum


class ConnectorType(str, Enum):
    RDBMS = "rdbms"
    CLOUD_STORAGE = "cloud_storage"
    STREAMING = "streaming"
    NOSQL = "nosql"
    DATA_WAREHOUSE = "data_warehouse"
    FILE_SYSTEM = "file_system"


class ConnectionStatus(str, Enum):
    ACTIVE = "active"
    TESTING = "testing"
    FAILED = "failed"
    DISABLED = "disabled"


class DataFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    DELIMITED = "delimited"
    XML = "xml"
    BINARY = "binary"


class ConfigSchema(BaseModel):
    """Base configuration schema for connectors"""
    name: str
    description: Optional[str] = None
    tags: List[str] = []


class CredentialSchema(BaseModel):
    """Base credential schema for connectors"""
    pass


class ValidationResult(BaseModel):
    is_valid: bool
    message: str
    details: Optional[Dict[str, Any]] = None


class TestResult(BaseModel):
    success: bool
    message: str
    latency_ms: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = None


class DataPreview(BaseModel):
    columns: List[Dict[str, Any]]
    rows: List[Dict[str, Any]]
    total_rows: Optional[int] = None
    sample_size: int


class BaseConnector(ABC):
    """Base connector interface"""

    def __init__(self, config: Dict[str, Any], credentials: Dict[str, Any]):
        self.config = config
        self.credentials = credentials
        self._connection = None

    @property
    @abstractmethod
    def connector_type(self) -> ConnectorType:
        """Return the connector type"""
        pass

    @property
    @abstractmethod
    def supported_formats(self) -> List[DataFormat]:
        """Return supported data formats"""
        pass

    @classmethod
    @abstractmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        """Return JSON schema for configuration"""
        pass

    @classmethod
    @abstractmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        """Return JSON schema for credentials"""
        pass

    @abstractmethod
    async def connect(self) -> bool:
        try:
            config = MySQLConfig(**self.config)
            credentials = MySQLCredentials(**self.credentials)

            self._connection = await aiomysql.connect(
                host=config.host,
                port=config.port,
                user=credentials.username,
                password=credentials.password,
                db=config.database,
                charset=config.charset,
                autocommit=config.autocommit
            )
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MySQL: {str(e)}")

    async def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    async def test_connection(self) -> TestResult:
        start_time = time.time()
        try:
            await self.connect()
            cursor = await self._connection.cursor()
            await cursor.execute("SELECT VERSION()")
            result = await cursor.fetchone()
            await cursor.close()
            await self.disconnect()

            latency = int((time.time() - start_time) * 1000)
            return TestResult(
                success=True,
                message="Connection successful",
                latency_ms=latency,
                metadata={"version": result[0]}
            )
        except Exception as e:
            return TestResult(
                success=False,
                message=f"Connection failed: {str(e)}"
            )

    async def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        try:
            MySQLConfig(**config)
            return ValidationResult(is_valid=True, message="Configuration is valid")
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Invalid configuration: {str(e)}")

    async def get_schema(self) -> Dict[str, Any]:
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()

        # Get tables
        await cursor.execute("SHOW TABLES")
        tables_result = await cursor.fetchall()

        tables = {}
        for (table_name,) in tables_result:
            # Get columns for each table
            await cursor.execute(f"DESCRIBE `{table_name}`")
            columns_result = await cursor.fetchall()

            tables[table_name] = {
                'name': table_name,
                'type': table_type,
                'columns': []
            }

            for col_name, data_type, is_nullable, default_val in columns_result:
                tables[table_name]['columns'].append({
                    'name': col_name,
                    'type': data_type,
                    'nullable': is_nullable == 'YES',
                    'default': default_val
                })

        await cursor.close()

        return {
            'database': config.database,
            'schema': config.schema,
            'warehouse': config.warehouse,
            'tables': list(tables.values())
        }

    async def preview_data(self, limit: int = 100, table_name: str = None) -> DataPreview:
        if not hasattr(self, '_connection'):
            await self.connect()

        config = SnowflakeConfig(**self.config)
        cursor = await self._connection.cursor()

        if not table_name:
            # Get first table
            await cursor.execute(f"SHOW TABLES IN SCHEMA {config.schema}")
            tables = await cursor.fetchall()
            if not tables:
                raise ValueError("No tables found")
            table_name = tables[0][1]  # Table name is in second column

        # Get column info
        await cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{config.schema}' 
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION
        """)
        columns_info = await cursor.fetchall()

        # Get sample data
        await cursor.execute(f'SELECT * FROM "{config.schema}"."{table_name}" LIMIT {limit}')
        rows = await cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Get total count
        await cursor.execute(f'SELECT COUNT(*) FROM "{config.schema}"."{table_name}"')
        total_rows = (await cursor.fetchone())[0]

        await cursor.close()

        return DataPreview(
            columns=[{'name': col[0], 'type': col[1]} for col in columns_info],
            rows=[dict(zip(column_names, row)) for row in rows],
            total_rows=total_rows,
            sample_size=len(rows)
        )

    async def read_data(self, query: Optional[str] = None, table_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        if not hasattr(self, '_connection'):
            await self.connect()

        cursor = await self._connection.cursor()

        if query:
            await cursor.execute(query)
        elif table_name:
            config = SnowflakeConfig(**self.config)
            await cursor.execute(f'SELECT * FROM "{config.schema}"."{table_name}"')
        else:
            raise ValueError("Either query or table_name must be provided")

        rows = await cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        await cursor.close()

        return [dict(zip(column_names, row)) for row in rows]
