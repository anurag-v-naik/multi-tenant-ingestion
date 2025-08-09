from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, List, Optional
from ...connectors.base import BaseConnector, ConnectorType, DataFormat, ValidationResult, TestResult, DataPreview
from pydantic import BaseModel, Field
import time


class MongoDBConfig(BaseModel):
    host: str = Field(..., description="MongoDB host")
    port: int = Field(27017, description="MongoDB port")
    database: str = Field(..., description="Database name")
    collection: str = Field(..., description="Collection name")
    connection_timeout_ms: int = Field(30000, description="Connection timeout in milliseconds")


class MongoDBCredentials(BaseModel):
    username: Optional[str] = Field(None, description="Username")
    password: Optional[str] = Field(None, description="Password")
    auth_source: str = Field("admin", description="Authentication database")


class MongoDBConnector(BaseConnector):

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.NOSQL

    @property
    def supported_formats(self) -> List[DataFormat]:
        return [DataFormat.JSON]

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        return MongoDBConfig.model_json_schema()

    @classmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        return MongoDBCredentials.model_json_schema()

    async def connect(self) -> bool:
        try:
            config = MongoDBConfig(**self.config)
            credentials = MongoDBCredentials(**self.credentials)

            # Build connection URI
            if credentials.username and credentials.password:
                uri = f"mongodb://{credentials.username}:{credentials.password}@{config.host}:{config.port}/{config.database}?authSource={credentials.auth_source}"
            else:
                uri = f"mongodb://{config.host}:{config.port}/{config.database}"

            self._client = AsyncIOMotorClient(
                uri,
                serverSelectionTimeoutMS=config.connection_timeout_ms
            )
            self._db = self._client[config.database]
            self._collection = self._db[config.collection]

            # Test connection
            await self._client.admin.command('ping')
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {str(e)}")

    async def disconnect(self):
        if hasattr(self, '_client') and self._client:
            self._client.close()

    async def test_connection(self) -> TestResult:
        start_time = time.time()
        try:
            await self.connect()

            # Get server info
            server_info = await self._client.server_info()

            latency = int((time.time() - start_time) * 1000)
            return TestResult(
                success=True,
                message="Connection successful",
                latency_ms=latency,
                metadata={"version": server_info.get("version")}
            )
        except Exception as e:
            return TestResult(
                success=False,
                message=f"Connection failed: {str(e)}"
            )

    async def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        try:
            MongoDBConfig(**config)
            return ValidationResult(is_valid=True, message="Configuration is valid")
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Invalid configuration: {str(e)}")

    async def get_schema(self) -> Dict[str, Any]:
        if not hasattr(self, '_db'):
            await self.connect()

        config = MongoDBConfig(**self.config)

        # Get collection stats
        stats = await self._db.command("collStats", config.collection)

        # Sample documents to infer schema
        sample_docs = []
        async for doc in self._collection.find().limit(10):
            sample_docs.append(doc)

        # Infer field types from sample
        fields = {}
        for doc in sample_docs:
            for key, value in doc.items():
                if key not in fields:
                    fields[key] = set()
                fields[key].add(type(value).__name__)

        schema_fields = [
            {'name': field, 'types': list(types)}
            for field, types in fields.items()
        ]

        return {
            'database': config.database,
            'collection': config.collection,
            'document_count': stats.get('count', 0),
            'fields': schema_fields
        }

    async def preview_data(self, limit: int = 100) -> DataPreview:
        if not hasattr(self, '_collection'):
            await self.connect()

        # Get sample documents
        documents = []
        async for doc in self._collection.find().limit(limit):
            # Convert ObjectId to string for JSON serialization
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            documents.append(doc)

        # Infer columns from first document
        columns = []
        if documents:
            for key, value in documents[0].items():
                columns.append({'name': key, 'type': type(value).__name__})

        # Get total count
        total_count = await self._collection.count_documents({})

        return DataPreview(
            columns=columns,
            rows=documents,
            total_rows=total_count,
            sample_size=len(documents)
        )

    async def read_data(self, query: Optional[Dict[str, Any]] = None, limit: Optional[int] = None, **kwargs) -> List[
        Dict[str, Any]]:
        if not hasattr(self, '_collection'):
            await self.connect()

        query = query or {}

        cursor = self._collection.find(query)
        if limit:
            cursor = cursor.limit(limit)

        documents = []
        async for doc in cursor:
            # Convert ObjectId to string
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            documents.append(doc)

        return documents

    async def write_data(self, data: Any, **kwargs) -> bool:
        if not hasattr(self, '_collection'):
            await self.connect()

        if isinstance(data, list):
            await self._collection.insert_many(data)
        else:
            await self._collection.insert_one(data)

        return True