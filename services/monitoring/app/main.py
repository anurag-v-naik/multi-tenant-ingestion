# services/monitoring/app/main.py
"""
Comprehensive monitoring, health checks, and alerting system
for multi-tenant data ingestion platform.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import uuid
from dataclasses import dataclass, asdict
from contextlib import asynccontextmanager

import aiohttp
import boto3
import psutil
import redis
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client import CollectorRegistry, multiprocess, REGISTRY
import structlog
from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Boolean, JSON, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import UUID
from pydantic import BaseModel, Field
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Database setup
Base = declarative_base()


# Enums
class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class ServiceType(str, Enum):
    PIPELINE = "pipeline"
    CATALOG = "catalog"
    CONNECTOR = "connector"
    DATA_QUALITY = "data_quality"
    UI = "ui"
    DATABASE = "database"
    REDIS = "redis"
    DATABRICKS = "databricks"
    S3 = "s3"


# Prometheus Metrics
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint', 'status'])
REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration', ['method', 'endpoint'])
ACTIVE_CONNECTIONS = Gauge('active_database_connections', 'Active database connections')
MEMORY_USAGE = Gauge('memory_usage_bytes', 'Memory usage in bytes', ['service'])
CPU_USAGE = Gauge('cpu_usage_percent', 'CPU usage percentage', ['service'])
PIPELINE_EXECUTIONS = Counter('pipeline_executions_total', 'Total pipeline executions', ['organization', 'status'])
DATA_INGESTED = Counter('data_ingested_bytes_total', 'Total data ingested in bytes', ['organization', 'source'])
ERROR_COUNT = Counter('errors_total', 'Total errors', ['service', 'error_type'])


# Database Models
class HealthCheck(Base):
    __tablename__ = "health_checks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_name = Column(String(100), nullable=False, index=True)
    service_type = Column(String(50), nullable=False)
    organization_id = Column(UUID(as_uuid=True), nullable=True, index=True)
    status = Column(String(20), nullable=False)
    response_time_ms = Column(Float, nullable=False)
    details = Column(JSON, default=dict)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(200), nullable=False)
    severity = Column(String(20), nullable=False)
    service_name = Column(String(100), nullable=False)
    organization_id = Column(UUID(as_uuid=True), nullable=True)
    message = Column(Text, nullable=False)
    details = Column(JSON, default=dict)
    is_active = Column(Boolean, default=True)
    acknowledged = Column(Boolean, default=False)
    acknowledged_by = Column(String(100), nullable=True)
    acknowledged_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)


class MetricSnapshot(Base):
    __tablename__ = "metric_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    metric_name = Column(String(100), nullable=False, index=True)
    service_name = Column(String(100), nullable=False)
    organization_id = Column(UUID(as_uuid=True), nullable=True)
    value = Column(Float, nullable=False)
    labels = Column(JSON, default=dict)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)


# Pydantic Models
class HealthCheckResult(BaseModel):
    service_name: str
    service_type: ServiceType
    status: HealthStatus
    response_time_ms: float
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class AlertCreate(BaseModel):
    name: str
    severity: AlertSeverity
    service_name: str
    organization_id: Optional[str] = None
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class SystemHealth(BaseModel):
    overall_status: HealthStatus
    services: List[HealthCheckResult]
    system_metrics: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)


@dataclass
class ServiceConfig:
    name: str
    type: ServiceType
    url: str
    health_endpoint: str = "/health"
    timeout: int = 30
    organization_specific: bool = False
    critical: bool = True


# Health Check Implementations
class HealthChecker:
    """Comprehensive health checking for all system components."""

    def __init__(self, db_session: Session, redis_client: redis.Redis):
        self.db = db_session
        self.redis = redis_client
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

        # Service configurations
        self.services = {
            "pipeline-service": ServiceConfig(
                name="pipeline-service",
                type=ServiceType.PIPELINE,
                url="http://pipeline-service:8000",
                critical=True
            ),
            "catalog-service": ServiceConfig(
                name="catalog-service",
                type=ServiceType.CATALOG,
                url="http://catalog-service:8001",
                critical=True
            ),
            "connector-service": ServiceConfig(
                name="connector-service",
                type=ServiceType.CONNECTOR,
                url="http://connector-service:8002",
                critical=True
            ),
            "data-quality-service": ServiceConfig(
                name="data-quality-service",
                type=ServiceType.DATA_QUALITY,
                url="http://data-quality-service:8003",
                critical=True
            ),
            "ui": ServiceConfig(
                name="ui",
                type=ServiceType.UI,
                url="http://ui:3000",
                health_endpoint="/api/health",
                critical=False
            )
        }

        # Initialize AWS clients
        self.cloudwatch = boto3.client('cloudwatch')
        self.secrets_manager = boto3.client('secretsmanager')

    async def check_all_services(self, organization_id: Optional[str] = None) -> SystemHealth:
        """Perform comprehensive health check of all services."""

        start_time = time.time()
        health_results = []

        # Check each service
        for service_config in self.services.values():
            try:
                result = await self.check_service_health(service_config, organization_id)
                health_results.append(result)
            except Exception as e:
                logger.error("Health check failed", service=service_config.name, error=str(e))
                health_results.append(HealthCheckResult(
                    service_name=service_config.name,
                    service_type=service_config.type,
                    status=HealthStatus.UNHEALTHY,
                    response_time_ms=0,
                    details={"error": str(e)}
                ))

        # Check infrastructure components
        infra_checks = await asyncio.gather(
            self.check_database_health(),
            self.check_redis_health(),
            self.check_s3_health(),
            self.check_databricks_health(organization_id),
            return_exceptions=True
        )

        for result in infra_checks:
            if isinstance(result, HealthCheckResult):
                health_results.append(result)
            elif isinstance(result, Exception):
                logger.error("Infrastructure health check failed", error=str(result))

        # Determine overall status
        overall_status = self._determine_overall_status(health_results)

        # Get system metrics
        system_metrics = await self._get_system_metrics()

        # Store health check results
        for result in health_results:
            health_check = HealthCheck(
                service_name=result.service_name,
                service_type=result.service_type,
                organization_id=organization_id,
                status=result.status,
                response_time_ms=result.response_time_ms,
                details=result.details
            )
            self.db.add(health_check)

        self.db.commit()

        total_time = (time.time() - start_time) * 1000
        logger.info("Health check completed",
                    overall_status=overall_status,
                    total_time_ms=total_time,
                    services_checked=len(health_results))

        return SystemHealth(
            overall_status=overall_status,
            services=health_results,
            system_metrics=system_metrics
        )

    async def check_service_health(self, config: ServiceConfig,
                                   organization_id: Optional[str] = None) -> HealthCheckResult:
        """Check health of a specific service."""

        start_time = time.time()

        try:
            headers = {}
            if organization_id and config.organization_specific:
                headers["X-Organization-ID"] = organization_id

            url = f"{config.url}{config.health_endpoint}"

            async with self.session.get(url, headers=headers) as response:
                response_time = (time.time() - start_time) * 1000

                if response.status == 200:
                    try:
                        data = await response.json()
                        status = HealthStatus.HEALTHY
                        details = data
                    except Exception:
                        status = HealthStatus.DEGRADED
                        details = {"message": "Service responding but invalid JSON"}
                else:
                    status = HealthStatus.UNHEALTHY
                    details = {"http_status": response.status, "message": await response.text()}

                return HealthCheckResult(
                    service_name=config.name,
                    service_type=config.type,
                    status=status,
                    response_time_ms=response_time,
                    details=details
                )

        except asyncio.TimeoutError:
            return HealthCheckResult(
                service_name=config.name,
                service_type=config.type,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=config.timeout * 1000,
                details={"error": "Request timeout"}
            )
        except Exception as e:
            return HealthCheckResult(
                service_name=config.name,
                service_type=config.type,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def check_database_health(self) -> HealthCheckResult:
        """Check PostgreSQL database health."""

        start_time = time.time()

        try:
            # Test connection
            result = self.db.execute("SELECT 1").fetchone()

            # Get connection stats
            connection_stats = self.db.execute("""
                SELECT 
                    count(*) as total_connections,
                    sum(case when state = 'active' then 1 else 0 end) as active_connections,
                    sum(case when state = 'idle' then 1 else 0 end) as idle_connections
                FROM pg_stat_activity 
                WHERE datname = current_database()
            """).fetchone()

            # Get database size
            db_size = self.db.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """).fetchone()

            response_time = (time.time() - start_time) * 1000

            return HealthCheckResult(
                service_name="postgresql",
                service_type=ServiceType.DATABASE,
                status=HealthStatus.HEALTHY,
                response_time_ms=response_time,
                details={
                    "total_connections": connection_stats[0],
                    "active_connections": connection_stats[1],
                    "idle_connections": connection_stats[2],
                    "database_size": db_size[0]
                }
            )

        except Exception as e:
            return HealthCheckResult(
                service_name="postgresql",
                service_type=ServiceType.DATABASE,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def check_redis_health(self) -> HealthCheckResult:
        """Check Redis health."""

        start_time = time.time()

        try:
            # Test basic operations
            test_key = f"health_check_{uuid.uuid4()}"
            await self.redis.set(test_key, "test", ex=60)
            value = await self.redis.get(test_key)
            await self.redis.delete(test_key)

            if value != b"test":
                raise Exception("Redis read/write test failed")

            # Get Redis info
            info = await self.redis.info()

            response_time = (time.time() - start_time) * 1000

            return HealthCheckResult(
                service_name="redis",
                service_type=ServiceType.REDIS,
                status=HealthStatus.HEALTHY,
                response_time_ms=response_time,
                details={
                    "version": info.get("redis_version"),
                    "connected_clients": info.get("connected_clients", 0),
                    "used_memory": info.get("used_memory_human"),
                    "uptime_seconds": info.get("uptime_in_seconds", 0)
                }
            )

        except Exception as e:
            return HealthCheckResult(
                service_name="redis",
                service_type=ServiceType.REDIS,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def check_s3_health(self) -> HealthCheckResult:
        """Check S3 connectivity and permissions."""

        start_time = time.time()

        try:
            s3_client = boto3.client('s3')

            # List buckets to test connectivity
            response = s3_client.list_buckets()
            bucket_count = len(response.get('Buckets', []))

            response_time = (time.time() - start_time) * 1000

            return HealthCheckResult(
                service_name="s3",
                service_type=ServiceType.S3,
                status=HealthStatus.HEALTHY,
                response_time_ms=response_time,
                details={
                    "accessible_buckets": bucket_count,
                    "region": s3_client.meta.region_name
                }
            )

        except Exception as e:
            return HealthCheckResult(
                service_name="s3",
                service_type=ServiceType.S3,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    async def check_databricks_health(self, organization_id: Optional[str] = None) -> HealthCheckResult:
        """Check Databricks workspace connectivity."""

        start_time = time.time()

        try:
            if not organization_id:
                return HealthCheckResult(
                    service_name="databricks",
                    service_type=ServiceType.DATABRICKS,
                    status=HealthStatus.UNKNOWN,
                    response_time_ms=0,
                    details={"message": "No organization specified"}
                )

            # Get Databricks credentials from Secrets Manager
            secret_name = f"multi-tenant-ingestion/{organization_id}/config"

            try:
                response = self.secrets_manager.get_secret_value(SecretId=secret_name)
                secret_data = json.loads(response['SecretString'])
                databricks_token = secret_data.get('databricks_token')
                workspace_url = secret_data.get('databricks_workspace_url')

                if not databricks_token or not workspace_url:
                    raise Exception("Missing Databricks credentials")

            except Exception as e:
                return HealthCheckResult(
                    service_name="databricks",
                    service_type=ServiceType.DATABRICKS,
                    status=HealthStatus.UNHEALTHY,
                    response_time_ms=(time.time() - start_time) * 1000,
                    details={"error": f"Failed to get credentials: {str(e)}"}
                )

            # Test Databricks API
            headers = {
                "Authorization": f"Bearer {databricks_token}",
                "Content-Type": "application/json"
            }

            url = f"{workspace_url}/api/2.0/clusters/list"

            async with self.session.get(url, headers=headers) as response:
                response_time = (time.time() - start_time) * 1000

                if response.status == 200:
                    data = await response.json()
                    cluster_count = len(data.get('clusters', []))

                    return HealthCheckResult(
                        service_name="databricks",
                        service_type=ServiceType.DATABRICKS,
                        status=HealthStatus.HEALTHY,
                        response_time_ms=response_time,
                        details={
                            "workspace_url": workspace_url,
                            "cluster_count": cluster_count
                        }
                    )
                else:
                    return HealthCheckResult(
                        service_name="databricks",
                        service_type=ServiceType.DATABRICKS,
                        status=HealthStatus.UNHEALTHY,
                        response_time_ms=response_time,
                        details={
                            "http_status": response.status,
                            "workspace_url": workspace_url
                        }
                    )

        except Exception as e:
            return HealthCheckResult(
                service_name="databricks",
                service_type=ServiceType.DATABRICKS,
                status=HealthStatus.UNHEALTHY,
                response_time_ms=(time.time() - start_time) * 1000,
                details={"error": str(e)}
            )

    def _determine_overall_status(self, results: List[HealthCheckResult]) -> HealthStatus:
        """Determine overall system health based on individual service results."""

        critical_services = [r for r in results if
                             self.services.get(r.service_name, ServiceConfig("", ServiceType.PIPELINE, "")).critical]

        # If any critical service is unhealthy, system is unhealthy
        unhealthy_critical = [r for r in critical_services if r.status == HealthStatus.UNHEALTHY]
        if unhealthy_critical:
            return HealthStatus.UNHEALTHY

        # If any critical service is degraded, system is degraded
        degraded_critical = [r for r in critical_services if r.status == HealthStatus.DEGRADED]
        if degraded_critical:
            return HealthStatus.DEGRADED

        # If all critical services are healthy, system is healthy
        return HealthStatus.HEALTHY

    async def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system-level metrics."""

        try:
            # CPU and Memory metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Network metrics
            network = psutil.net_io_counters()

            return {
                "cpu_percent": cpu_percent,
                "memory": {
                    "total": memory.total,
                    "available": memory.available,
                    "percent": memory.percent,
                    "used": memory.used
                },
                "disk": {
                    "total": disk.total,
                    "used": disk.used,
                    "free": disk.free,
                    "percent": (disk.used / disk.total) * 100
                },
                "network": {
                    "bytes_sent": network.bytes_sent,
                    "bytes_recv": network.bytes_recv,
                    "packets_sent": network.packets_sent,
                    "packets_recv": network.packets_recv
                }
            }
        except Exception as e:
            logger.error("Failed to get system metrics", error=str(e))
            return {"error": str(e)}

    async def close(self):
        """Clean up resources."""
        await self.session.close()


# Alert Manager
class AlertManager:
    """Manage alerts, notifications, and escalations."""

    def __init__(self, db_session: Session, redis_client: redis.Redis):
        self.db = db_session
        self.redis = redis_client
        self.notification_channels = []

        # Initialize notification channels
        self._setup_notification_channels()

    def _setup_notification_channels(self):
        """Setup notification channels (email, Slack, etc.)."""

        # Email notification
        smtp_config = {
            "host": "smtp.amazonaws.com",
            "port": 587,
            "username": "AKIAIOSFODNN7EXAMPLE",
            "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "from_email": "alerts@yourcompany.com"
        }

        self.notification_channels.append(EmailNotifier(smtp_config))

    async def create_alert(self, alert_data: AlertCreate) -> Alert:
        """Create and process a new alert."""

        # Check if similar alert already exists
        existing_alert = self.db.query(Alert).filter(
            Alert.name == alert_data.name,
            Alert.service_name == alert_data.service_name,
            Alert.organization_id == alert_data.organization_id,
            Alert.is_active == True
        ).first()

        if existing_alert:
            # Update existing alert
            existing_alert.message = alert_data.message
            existing_alert.details = alert_data.details
            existing_alert.created_at = datetime.utcnow()
            self.db.commit()
            return existing_alert

        # Create new alert
        alert = Alert(
            name=alert_data.name,
            severity=alert_data.severity,
            service_name=alert_data.service_name,
            organization_id=alert_data.organization_id,
            message=alert_data.message,
            details=alert_data.details
        )

        self.db.add(alert)
        self.db.commit()

        # Send notifications
        await self._send_notifications(alert)

        # Log alert creation
        logger.warning("Alert created",
                       alert_id=str(alert.id),
                       name=alert.name,
                       severity=alert.severity,
                       service=alert.service_name)

        return alert

    async def _send_notifications(self, alert: Alert):
        """Send alert notifications to configured channels."""

        for channel in self.notification_channels:
            try:
                await channel.send_alert(alert)
            except Exception as e:
                logger.error("Failed to send alert notification",
                             channel=type(channel).__name__,
                             alert_id=str(alert.id),
                             error=str(e))

    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> Alert:
        """Acknowledge an alert."""

        alert = self.db.query(Alert).filter(Alert.id == alert_id).first()
        if not alert:
            raise ValueError("Alert not found")

        alert.acknowledged = True
        alert.acknowledged_by = acknowledged_by
        alert.acknowledged_at = datetime.utcnow()

        self.db.commit()

        logger.info("Alert acknowledged",
                    alert_id=alert_id,
                    acknowledged_by=acknowledged_by)

        return alert

    async def resolve_alert(self, alert_id: str) -> Alert:
        """Resolve an alert."""

        alert = self.db.query(Alert).filter(Alert.id == alert_id).first()
        if not alert:
            raise ValueError("Alert not found")

        alert.is_active = False
        alert.resolved_at = datetime.utcnow()

        self.db.commit()

        logger.info("Alert resolved", alert_id=alert_id)

        return alert


# Notification Channels
class EmailNotifier:
    """Send alert notifications via email."""

    def __init__(self, smtp_config: Dict):
        self.smtp_config = smtp_config

    async def send_alert(self, alert: Alert):
        """Send alert via email."""

        try:
            # Create message
            msg = MimeMultipart()
            msg['From'] = self.smtp_config['from_email']
            msg['To'] = self._get_recipients(alert)
            msg['Subject'] = f"[{alert.severity.upper()}] {alert.name}"

            # Email body
            body = f"""
Alert: {alert.name}
Severity: {alert.severity.upper()}
Service: {alert.service_name}
Organization: {alert.organization_id or 'System'}
Message: {alert.message}
Time: {alert.created_at}

Details:
{json.dumps(alert.details, indent=2)}
            """

            msg.attach(MimeText(body, 'plain'))

            # Send email
            server = smtplib.SMTP(self.smtp_config['host'], self.smtp_config['port'])
            server.starttls()
            server.login(self.smtp_config['username'], self.smtp_config['password'])
            text = msg.as_string()
            server.sendmail(self.smtp_config['from_email'], msg['To'], text)
            server.quit()

            logger.info("Alert email sent", alert_id=str(alert.id))

        except Exception as e:
            logger.error("Failed to send alert email", alert_id=str(alert.id), error=str(e))
            raise

    def _get_recipients(self, alert: Alert) -> str:
        """Get email recipients based on alert severity and organization."""

        # This would typically pull from a configuration or database
        if alert.severity == AlertSeverity.CRITICAL:
            return "oncall@yourcompany.com"
        elif alert.severity == AlertSeverity.WARNING:
            return "engineering@yourcompany.com"
        else:
            return "monitoring@yourcompany.com"


# Metrics Collector
class MetricsCollector:
    """Collect and store custom metrics."""

    def __init__(self, db_session: Session):
        self.db = db_session

    async def record_metric(self, metric_name: str, value: float,
                            service_name: str, organization_id: Optional[str] = None,
                            labels: Dict[str, str] = None):
        """Record a custom metric."""

        metric = MetricSnapshot(
            metric_name=metric_name,
            service_name=service_name,
            organization_id=organization_id,
            value=value,
            labels=labels or {}
        )

        self.db.add(metric)
        self.db.commit()

    async def get_metrics(self, metric_name: str, service_name: str,
                          start_time: datetime, end_time: datetime,
                          organization_id: Optional[str] = None) -> List[MetricSnapshot]:
        """Retrieve metrics for a time range."""

        query = self.db.query(MetricSnapshot).filter(
            MetricSnapshot.metric_name == metric_name,
            MetricSnapshot.service_name == service_name,
            MetricSnapshot.timestamp >= start_time,
            MetricSnapshot.timestamp <= end_time
        )

        if organization_id:
            query = query.filter(MetricSnapshot.organization_id == organization_id)

        return query.order_by(MetricSnapshot.timestamp).all()


# FastAPI Application
app = FastAPI(title="Multi-Tenant Monitoring Service", version="1.0.0")


# Dependency injection
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_redis():
    return redis.Redis(host='redis', port=6379, decode_responses=True)


@app.on_event("startup")
async def startup_event():
    """Initialize monitoring services."""
    app.state.health_checker = HealthChecker(next(get_db()), get_redis())
    app.state.alert_manager = AlertManager(next(get_db()), get_redis())
    app.state.metrics_collector = MetricsCollector(next(get_db()))

    # Start background monitoring
    asyncio.create_task(background_monitoring())


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources."""
    if hasattr(app.state, 'health_checker'):
        await app.state.health_checker.close()


async def background_monitoring():
    """Background task for continuous monitoring."""

    while True:
        try:
            # Run health checks every 30 seconds
            health_result = await app.state.health_checker.check_all_services()

            # Create alerts for unhealthy services
            for service_result in health_result.services:
                if service_result.status == HealthStatus.UNHEALTHY:
                    await app.state.alert_manager.create_alert(AlertCreate(
                        name=f"Service Unhealthy: {service_result.service_name}",
                        severity=AlertSeverity.CRITICAL,
                        service_name=service_result.service_name,
                        message=f"Service {service_result.service_name} is unhealthy",
                        details=service_result.details
                    ))
                elif service_result.status == HealthStatus.DEGRADED:
                    await app.state.alert_manager.create_alert(AlertCreate(
                        name=f"Service Degraded: {service_result.service_name}",
                        severity=AlertSeverity.WARNING,
                        service_name=service_result.service_name,
                        message=f"Service {service_result.service_name} is degraded",
                        details=service_result.details
                    ))

            # Update Prometheus metrics
            for service_result in health_result.services:
                if service_result.status == HealthStatus.HEALTHY:
                    status_value = 1
                elif service_result.status == HealthStatus.DEGRADED:
                    status_value = 0.5
                else:
                    status_value = 0

                # Update service health gauge (would need to define this metric)
                # SERVICE_HEALTH.labels(service=service_result.service_name).set(status_value)

            await asyncio.sleep(30)

        except Exception as e:
            logger.error("Background monitoring failed", error=str(e))
            await asyncio.sleep(60)  # Wait longer on error


# API Endpoints
@app.get("/health", response_model=SystemHealth)
async def get_system_health(organization_id: Optional[str] = None):
    """Get comprehensive system health status."""
    return await app.state.health_checker.check_all_services(organization_id)


@app.get("/health/{service_name}")
async def get_service_health(service_name: str, organization_id: Optional[str] = None):
    """Get health status for a specific service."""

    service_config = app.state.health_checker.services.get(service_name)
    if not service_config:
        raise HTTPException(status_code=404, detail="Service not found")

    result = await app.state.health_checker.check_service_health(service_config, organization_id)
    return result


@app.get("/metrics")
async def get_prometheus_metrics():
    """Prometheus metrics endpoint."""
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.post("/alerts", response_model=Alert)
async def create_alert(alert_data: AlertCreate):
    """Create a new alert."""
    return await app.state.alert_manager.create_alert(alert_data)


@app.get("/alerts")
async def get_alerts(organization_id: Optional[str] = None,
                     active_only: bool = True,
                     limit: int = 100):
    """Get alerts with optional filtering."""

    query = app.state.alert_manager.db.query(Alert)

    if organization_id:
        query = query.filter(Alert.organization_id == organization_id)

    if active_only:
        query = query.filter(Alert.is_active == True)

    alerts = query.order_by(Alert.created_at.desc()).limit(limit).all()
    return alerts


@app.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str, acknowledged_by: str):
    """Acknowledge an alert."""
    return await app.state.alert_manager.acknowledge_alert(alert_id, acknowledged_by)


@app.post("/alerts/{alert_id}/resolve")
async def resolve_alert(alert_id: str):
    """Resolve an alert."""
    return await app.state.alert_manager.resolve_alert(alert_id)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8004)