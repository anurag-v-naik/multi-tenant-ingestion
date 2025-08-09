from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from typing import Dict, Any, List, Optional
from ...connectors.base import BaseConnector, ConnectorType, DataFormat, ValidationResult, TestResult, DataPreview
from pydantic import BaseModel, Field
import json
import time


class KafkaConfig(BaseModel):
    bootstrap_servers: List[str] = Field(..., description="Kafka bootstrap servers")
    topic: str = Field(..., description="Kafka topic name")
    security_protocol: str = Field("PLAINTEXT", description="Security protocol")
    consumer_group_id: str = Field("connector-group", description="Consumer group ID")
    auto_offset_reset: str = Field("earliest", description="Auto offset reset")


class KafkaCredentials(BaseModel):
    username: Optional[str] = Field(None, description="SASL username")
    password: Optional[str] = Field(None, description="SASL password")
    ssl_ca_location: Optional[str] = Field(None, description="SSL CA certificate location")


class KafkaConnector(BaseConnector):

    @property
    def connector_type(self) -> ConnectorType:
        return ConnectorType.STREAMING

    @property
    def supported_formats(self) -> List[DataFormat]:
        return [DataFormat.JSON, DataFormat.AVRO]

    @classmethod
    def get_config_schema(cls) -> Dict[str, Any]:
        return KafkaConfig.model_json_schema()

    @classmethod
    def get_credential_schema(cls) -> Dict[str, Any]:
        return KafkaCredentials.model_json_schema()

    async def connect(self) -> bool:
        try:
            config = KafkaConfig(**self.config)
            credentials = KafkaCredentials(**self.credentials)

            kafka_config = {
                'bootstrap_servers': config.bootstrap_servers,
                'security_protocol': config.security_protocol,
            }

            if credentials.username and credentials.password:
                kafka_config.update({
                    'sasl_mechanism': 'PLAIN',
                    'sasl_plain_username': credentials.username,
                    'sasl_plain_password': credentials.password,
                })

            if credentials.ssl_ca_location:
                kafka_config['ssl_ca_location'] = credentials.ssl_ca_location

            self._admin_client = KafkaAdminClient(**kafka_config)
            self._producer = KafkaProducer(
                **kafka_config,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            self._consumer = KafkaConsumer(
                config.topic,
                **kafka_config,
                group_id=config.consumer_group_id,
                auto_offset_reset=config.auto_offset_reset,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )

            return True
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Kafka: {str(e)}")

    async def disconnect(self):
        if hasattr(self, '_producer') and self._producer:
            self._producer.close()
        if hasattr(self, '_consumer') and self._consumer:
            self._consumer.close()
        if hasattr(self, '_admin_client') and self._admin_client:
            self._admin_client.close()

    async def test_connection(self) -> TestResult:
        start_time = time.time()
        try:
            await self.connect()
            config = KafkaConfig(**self.config)

            # Test by listing topics
            metadata = self._admin_client.list_topics()
            topics = list(metadata.topics.keys())

            latency = int((time.time() - start_time) * 1000)
            return TestResult(
                success=True,
                message="Connection successful",
                latency_ms=latency,
                metadata={"topics_available": len(topics), "target_topic": config.topic}
            )
        except Exception as e:
            return TestResult(
                success=False,
                message=f"Connection failed: {str(e)}"
            )

    async def validate_config(self, config: Dict[str, Any]) -> ValidationResult:
        try:
            KafkaConfig(**config)
            return ValidationResult(is_valid=True, message="Configuration is valid")
        except Exception as e:
            return ValidationResult(is_valid=False, message=f"Invalid configuration: {str(e)}")

    async def get_schema(self) -> Dict[str, Any]:
        if not hasattr(self, '_admin_client'):
            await self.connect()

        config = KafkaConfig(**self.config)

        # Get topic metadata
        metadata = self._admin_client.describe_topics([config.topic])
        topic_info = metadata[config.topic]

        return {
            'topic': config.topic,
            'partitions': len(topic_info.partitions),
            'replication_factor': len(topic_info.partitions[0].replicas) if topic_info.partitions else 0,
            'bootstrap_servers': config.bootstrap_servers
        }

    async def preview_data(self, limit: int = 100) -> DataPreview:
        if not hasattr(self, '_consumer'):
            await self.connect()

        messages = []
        for message in self._consumer:
            messages.append({
                'partition': message.partition,
                'offset': message.offset,
                'timestamp': message.timestamp,
                'value': message.value
            })

            if len(messages) >= limit:
                break

        # Infer schema from first few messages
        columns = []
        if messages:
            sample_value = messages[0]['value']
            if isinstance(sample_value, dict):
                columns = [{'name': key, 'type': type(value).__name__} for key, value in sample_value.items()]
            else:
                columns = [{'name': 'value', 'type': type(sample_value).__name__}]

        return DataPreview(
            columns=columns,
            rows=messages,
            total_rows=None,  # Unknown for streaming
            sample_size=len(messages)
        )

    async def read_data(self, max_messages: int = 1000, timeout_ms: int = 10000, **kwargs) -> List[Dict[str, Any]]:
        if not hasattr(self, '_consumer'):
            await self.connect()

        messages = []
        start_time = time.time()

        while len(messages) < max_messages and (time.time() - start_time) * 1000 < timeout_ms:
            message_batch = self._consumer.poll(timeout_ms=1000)
            for topic_partition, msgs in message_batch.items():
                for message in msgs:
                    messages.append({
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value': message.value
                    })

                    if len(messages) >= max_messages:
                        break
                if len(messages) >= max_messages:
                    break

        return messages

    async def write_data(self, data: Any, **kwargs) -> bool:
        if not hasattr(self, '_producer'):
            await self.connect()

        config = KafkaConfig(**self.config)

        if isinstance(data, list):
            for record in data:
                self._producer.send(config.topic, value=record)
        else:
            self._producer.send(config.topic, value=data)

        self._producer.flush()
        return True
