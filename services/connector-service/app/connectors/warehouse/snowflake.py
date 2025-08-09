import snowflake.connector.aio
from typing import Dict, Any, List, Optional
from ...connectors.base import BaseConnector, ConnectorType, DataFormat, ValidationResult, TestResult, DataPreview
from pydantic import BaseModel, Field
import time


class SnowflakeConfig(BaseModel):
    account: str = Field(..., description="Snowflake account identifier")
    warehouse: str = Field(..., description="Warehouse name")
    database: str = Field(..., description="Database name")
    schema: str = Field("PUBLIC", description="Schema name")
    role: Optional[str] = Field(None, description="Role name")


class SnowflakeCredentials(BaseModel):
    username: str = Field(..., description="Username")
    password: str = Field(..., description="Password")


class SnowflakeConnector(BaseConnector):

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.DATA_WAREHOUSE

    @property
    def supported_formats(self) -> List[DataFormat]:
        return [DataFormat.JSON, DataFormat.CSV, DataFormat.PARQUET]

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        return SnowflakeConfig.model_json_schema()

    @classmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        return SnowflakeCredentials.model_json_schema()

    async def connect(self) -> bool:
        try:
            config = SnowflakeConfig(**self.config)
            credentials = SnowflakeCredentials(**self.credentials)

            connection_params = {
                'account': config.account,
                'user': credentials.username,
                'password': credentials.password,
                'warehouse': config.warehouse,
                'database': config.database,
                'schema': config.schema,
            }

            if config.role:
                connection_params['role'] = config.role

            self._connection = await snowflake.connector.aio.connect(**connection_params)
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {str(e)}")

    async def disconnect(self):
        if hasattr(self, '_connection') and self._connection:
            await self._connection.close()

    async def test_connection(self) -> TestResult:
        start_time = time.time()
        try:
            await self.connect()

            cursor = await self._connection.cursor()
            await cursor.execute("SELECT CURRENT_VERSION()")
            result = await cursor.fetchone()
            await cursor.close()

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
            SnowflakeConfig(**config)
            return ValidationResult(is_valid=True, message="Configuration is valid")
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Invalid configuration: {str(e)}")

    async def get_schema(self) -> Dict[str, Any]:
        if not hasattr(self, '_connection'):
            await self.connect()

        config = SnowflakeConfig(**self.config)
        cursor = await self._connection.cursor()

        # Get tables in the schema
        await cursor.execute(f"""
            SELECT TABLE_NAME, TABLE_TYPE 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{config.schema}'
        """)
        tables_result = await cursor.fetchall()

        tables = {}
        for table_name, table_type in tables_result:
            # Get columns for each table
            await cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = '{config.schema}' 
                AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION
            """)
            columns_result = await cursor.fetchall()

            tables[table_name] = {
                self) -> bool:
        """Establish connection"""
        pass

    @abstractmethod
    async def disconnect(self):
        """Close connection"""
        pass

    @abstractmethod
    async def test_connection(self) -> TestResult:
        """Test the connection"""
        pass

    @abstractmethod
    async def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        """Validate configuration"""
        pass

    @abstractmethod
    async def get_schema(self) -> Dict[str, Any]:
        """Get data source schema/metadata"""
        pass

    @abstractmethod
    async def preview_data(self, limit: int = 100) -> DataPreview:
        """Preview data from source"""
        pass

    @abstractmethod
    async def read_data(self, query: Optional[str] = None, **kwargs) -> Any:
        """Read data from source"""
        pass

    async def write_data(self, data: Any, **kwargs) -> bool:
        """Write data to destination (optional)"""
        raise NotImplementedError("Write operation not supported by this connector")
