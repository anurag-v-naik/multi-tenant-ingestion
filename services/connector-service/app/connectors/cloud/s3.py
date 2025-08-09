import boto3
import pandas as pd
from typing import Dict, Any, List, Optional
from ...connectors.base import BaseConnector, ConnectorType, DataFormat, ValidationResult, TestResult, DataPreview
from pydantic import BaseModel, Field
import time
import io


class S3Config(BaseModel):
    bucket_name: str = Field(..., description="S3 bucket name")
    prefix: str = Field("", description="Object prefix/folder path")
    region: str = Field("us-east-1", description="AWS region")
    endpoint_url: Optional[str] = Field(None, description="Custom S3 endpoint URL")


class S3Credentials(BaseModel):
    access_key_id: str = Field(..., description="AWS access key ID")
    secret_access_key: str = Field(..., description="AWS secret access key")
    session_token: Optional[str] = Field(None, description="AWS session token")


class S3Connector(BaseConnector):

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.CLOUD_STORAGE

    @property
    def supported_formats(self) -> List[DataFormat]:
        return [DataFormat.CSV, DataFormat.JSON, DataFormat.PARQUET]

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        return S3Config.model_json_schema()

    @classmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        return S3Credentials.model_json_schema()

    async def connect(self) -> bool:
        try:
            config = S3Config(**self.config)
            credentials = S3Credentials(**self.credentials)

            self._client = boto3.client(
                's3',
                aws_access_key_id=credentials.access_key_id,
                aws_secret_access_key=credentials.secret_access_key,
                aws_session_token=credentials.session_token,
                region_name=config.region,
                endpoint_url=config.endpoint_url
            )
            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to S3: {str(e)}")

    async def disconnect(self):
        self._client = None

    async def test_connection(self) -> TestResult:
        start_time = time.time()
        try:
            await self.connect()
            config = S3Config(**self.config)

            # Test by listing bucket contents
            response = self._client.list_objects_v2(
                Bucket=config.bucket_name,
                Prefix=config.prefix,
                MaxKeys=1
            )

            latency = int((time.time() - start_time) * 1000)
            return TestResult(
                success=True,
                message="Connection successful",
                latency_ms=latency,
                metadata={"bucket": config.bucket_name, "objects_found": response.get('KeyCount', 0)}
            )
        except Exception as e:
            return TestResult(
                success=False,
                message=f"Connection failed: {str(e)}"
            )

    async def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        try:
            S3Config(**config)
            return ValidationResult(is_valid=True, message="Configuration is valid")
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Invalid configuration: {str(e)}")

    async def get_schema(self) -> Dict[str, Any]:
        if not self._client:
            await self.connect()

        config = S3Config(**self.config)

        # List objects in bucket
        paginator = self._client.get_paginator('list_objects_v2')
        objects = []

        for page in paginator.paginate(Bucket=config.bucket_name, Prefix=config.prefix):
            for obj in page.get('Contents', []):
                objects.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'storage_class': obj.get('StorageClass', 'STANDARD')
                })

        return {
            'bucket': config.bucket_name,
            'prefix': config.prefix,
            'objects': objects[:100]  # Limit to first 100 objects
        }

    async def preview_data(self, limit: int = 100, object_key: str = None) -> DataPreview:
        if not self._client:
            await self.connect()

        config = S3Config(**self.config)

        if not object_key:
            # Find first suitable object
            response = self._client.list_objects_v2(
                Bucket=config.bucket_name,
                Prefix=config.prefix,
                MaxKeys=10
            )

            suitable_objects = [
                obj['Key'] for obj in response.get('Contents', [])
                if any(obj['Key'].lower().endswith(ext) for ext in ['.csv', '.json', '.parquet'])
            ]

            if not suitable_objects:
                raise ValueError("No suitable data files found")

            object_key = suitable_objects[0]

        # Read and preview file
        response = self._client.get_object(Bucket=config.bucket_name, Key=object_key)
        content = response['Body'].read()

        if object_key.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content), nrows=limit)
        elif object_key.lower().endswith('.json'):
            df = pd.read_json(io.BytesIO(content), lines=True).head(limit)
        elif object_key.lower().endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(content)).head(limit)
        else:
            raise ValueError(f"Unsupported file format: {object_key}")

        columns = [{'name': col, 'type': str(df[col].dtype)} for col in df.columns]
        rows = df.to_dict('records')

        return DataPreview(
            columns=columns,
            rows=rows,
            total_rows=None,  # Can't determine without reading entire file
            sample_size=len(rows)
        )

    async def read_data(self, object_key: str = None, **kwargs) -> List[Dict[str, Any]]:
        if not self._client:
            await self.connect()

        config = S3Config(**self.config)

        if not object_key:
            raise ValueError("object_key must be provided for S3 connector")

        response = self._client.get_object(Bucket=config.bucket_name, Key=object_key)
        content = response['Body'].read()

        if object_key.lower().endswith('.csv'):
            df = pd.read_csv(io.BytesIO(content))
        elif object_key.lower().endswith('.json'):
            df = pd.read_json(io.BytesIO(content), lines=True)
        elif object_key.lower().endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(content))
        else:
            raise ValueError(f"Unsupported file format: {object_key}")

        return df.to_dict('records')
