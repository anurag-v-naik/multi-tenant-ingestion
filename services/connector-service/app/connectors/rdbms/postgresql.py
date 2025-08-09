import asyncpg
import json
from typing import Dict, Any, List, Optional
from ...connectors.base import BaseConnector, ConnectorType, DataFormat, ValidationResult, TestResult, DataPreview
from pydantic import BaseModel, Field
import time


class PostgreSQLConfig(BaseModel):
    host: str = Field(..., description="Database host")
    port: int = Field(5432, description="Database port")
    database: str = Field(..., description="Database name")
    schema: str = Field("public", description="Schema name")
    ssl_mode: str = Field("prefer", description="SSL mode")
    connection_timeout: int = Field(30, description="Connection timeout in seconds")
    query_timeout: int = Field(300, description="Query timeout in seconds")


class PostgreSQLCredentials(BaseModel):
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")


class PostgreSQLConnector(BaseConnector):

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.RDBMS

    @property
    def supported_formats(self) -> List[DataFormat]:
        return [DataFormat.JSON, DataFormat.CSV]

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        return PostgreSQLConfig.model_json_schema()

    @classmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        return PostgreSQLCredentials.model_json_schema()

    async def connect(self) -> bool:
        try:
            config = PostgreSQLConfig(**self.config)
            credentials = PostgreSQLCredentials(**self.credentials)

            self._connection = await asyncpg.connect(
                host=config.host,
                port=config.port,
                database=config.database,
                user=credentials.username,
                password=credentials.password,
                ssl=config.ssl_mode,
                timeout=config.connection_timeout
            )
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {str(e)}")

    async def disconnect(self):
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def test_connection(self) -> TestResult:
        start_time = time.time()
        try:
            await self.connect()
            result = await self._connection.fetchval("SELECT version()")
            await self.disconnect()

            latency = int((time.time() - start_time) * 1000)
            return TestResult(
                success=True,
                message="Connection successful",
                latency_ms=latency,
                metadata={"version": result}
            )
        except Exception as e:
            return TestResult(
                success=False,
                message=f"Connection failed: {str(e)}"
            )

    async def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        try:
            PostgreSQLConfig(**config)
            return ValidationResult(is_valid=True, message="Configuration is valid")
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Invalid configuration: {str(e)}")

    async def get_schema(self) -> Dict[str, Any]:
        if not self._connection:
            await self.connect()

        config = PostgreSQLConfig(**self.config)

        # Get tables and their columns
        query = """
        SELECT 
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.column_default,
            c.ordinal_position
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name
        WHERE t.table_schema = $1
        ORDER BY t.table_name, c.ordinal_position
        """

        rows = await self._connection.fetch(query, config.schema)

        tables = {}
        for row in rows:
            table_name = row['table_name']
            if table_name not in tables:
                tables[table_name] = {
                    'name': table_name,
                    'columns': []
                }

            tables[table_name]['columns'].append({
                'name': row['column_name'],
                'type': row['data_type'],
                'nullable': row['is_nullable'] == 'YES',
                'default': row['column_default'],
                'position': row['ordinal_position']
            })

        return {
            'schema': config.schema,
            'tables': list(tables.values())
        }

    async def preview_data(self, limit: int = 100, table_name: str = None) -> DataPreview:
        if not self._connection:
            await self.connect()

        if not table_name:
            schema = await self.get_schema()
            if not schema['tables']:
                raise ValueError("No tables found")
            table_name = schema['tables'][0]['name']

        config = PostgreSQLConfig(**self.config)

        # Get column info
        columns_query = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
        """
        columns = await self._connection.fetch(columns_query, config.schema, table_name)

        # Get sample data
        data_query = f'SELECT * FROM "{config.schema}"."{table_name}" LIMIT $1'
        rows = await self._connection.fetch(data_query, limit)

        # Get total count
        count_query = f'SELECT COUNT(*) FROM "{config.schema}"."{table_name}"'
        total_rows = await self._connection.fetchval(count_query)

        return DataPreview(
            columns=[{'name': col['column_name'], 'type': col['data_type']} for col in columns],
            rows=[dict(row) for row in rows],
            total_rows=total_rows,
            sample_size=len(rows)
        )

    async def read_data(self, query: Optional[str] = None, table_name: str = None, **kwargs) -> List[Dict[str, Any]]:
        if not self._connection:
            await self.connect()

        if query:
            rows = await self._connection.fetch(query)
        elif table_name:
            config = PostgreSQLConfig(**self.config)
            query = f'SELECT * FROM "{config.schema}"."{table_name}"'
            rows = await self._connection.fetch(query)
        else:
            raise ValueError("Either query or table_name must be provided")

        return [dict(row) for row in rows]
