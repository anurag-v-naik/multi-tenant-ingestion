# services/resource-manager/app/main.py
"""
Multi-tenant resource management system with quotas, isolation,
and automated provisioning/deprovisioning.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import uuid
from dataclasses import dataclass
import boto3
from botocore.exceptions import ClientError

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import create_engine, Column, String, DateTime, Float, Integer, Boolean, JSON, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy.dialects.postgresql import UUID
from pydantic import BaseModel, Field, validator
import redis
import structlog

logger = structlog.get_logger()

# Database setup
Base = declarative_base()

# Enums
class ResourceType(str, Enum):
    DBU_HOURS = "dbu_hours"
    STORAGE_GB = "storage_gb"
    API_CALLS = "api_calls"
    CONCURRENT_PIPELINES = "concurrent_pipelines"
    DATABRICKS_CLUSTERS = "databricks_clusters"
    S3_REQUESTS = "s3_requests"
    BANDWIDTH_GB = "bandwidth_gb"

class QuotaStatus(str, Enum):
    ACTIVE = "active"
    WARNING = "warning"
    EXCEEDED = "exceeded"
    SUSPENDED = "suspended"


class ProvisioningStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"


class IsolationLevel(str, Enum):
    SHARED = "shared"
    DEDICATED = "dedicated"
    ISOLATED = "isolated"


# Database Models
class Organization(Base):
    __tablename__ = "organizations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), unique=True, nullable=False, index=True)
    display_name = Column(String(200), nullable=False)
    isolation_level = Column(String(20), default=IsolationLevel.SHARED)
    compliance_level = Column(String(50), default="medium")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    quotas = relationship("ResourceQuota", back_populates="organization")
    usage_records = relationship("ResourceUsage", back_populates="organization")
    provisioning_tasks = relationship("ProvisioningTask", back_populates="organization")


class ResourceQuota(Base):
    __tablename__ = "resource_quotas"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    resource_type = Column(String(50), nullable=False)
    quota_limit = Column(Float, nullable=False)
    warning_threshold = Column(Float, default=0.8)  # 80% of limit
    reset_period = Column(String(20), default="monthly")  # hourly, daily, weekly, monthly
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="quotas")


class ResourceUsage(Base):
    __tablename__ = "resource_usage"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    resource_type = Column(String(50), nullable=False)
    usage_amount = Column(Float, nullable=False)
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)
    metadata = Column(JSON, default=dict)
    recorded_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="usage_records")


class QuotaViolation(Base):
    __tablename__ = "quota_violations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    resource_type = Column(String(50), nullable=False)
    quota_limit = Column(Float, nullable=False)
    actual_usage = Column(Float, nullable=False)
    violation_type = Column(String(20), nullable=False)  # warning, exceeded
    action_taken = Column(String(100), nullable=True)
    resolved = Column(Boolean, default=False)
    resolved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class ProvisioningTask(Base):
    __tablename__ = "provisioning_tasks"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    task_type = Column(String(50), nullable=False)  # create, update, delete
    resource_type = Column(String(50), nullable=False)
    status = Column(String(20), default=ProvisioningStatus.PENDING)
    configuration = Column(JSON, default=dict)
    result = Column(JSON, default=dict)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    organization = relationship("Organization", back_populates="provisioning_tasks")


class TenantIsolation(Base):
    __tablename__ = "tenant_isolation"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    resource_type = Column(String(50), nullable=False)
    resource_arn = Column(String(500), nullable=False)
    isolation_config = Column(JSON, default=dict)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# Pydantic Models
class QuotaCreate(BaseModel):
    resource_type: ResourceType
    quota_limit: float = Field(..., gt=0)
    warning_threshold: float = Field(default=0.8, ge=0, le=1)
    reset_period: str = Field(default="monthly", regex="^(hourly|daily|weekly|monthly)$")


class QuotaUpdate(BaseModel):
    quota_limit: Optional[float] = Field(None, gt=0)
    warning_threshold: Optional[float] = Field(None, ge=0, le=1)
    reset_period: Optional[str] = Field(None, regex="^(hourly|daily|weekly|monthly)$")
    is_active: Optional[bool] = None


class UsageRecord(BaseModel):
    resource_type: ResourceType
    usage_amount: float = Field(..., ge=0)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class OrganizationCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100, regex="^[a-z0-9-]+$")
    display_name: str = Field(..., min_length=1, max_length=200)
    isolation_level: IsolationLevel = IsolationLevel.SHARED
    compliance_level: str = Field(default="medium", regex="^(low|medium|high)$")
    initial_quotas: List[QuotaCreate] = Field(default_factory=list)


class ResourceQuotaStatus(BaseModel):
    resource_type: ResourceType
    quota_limit: float
    current_usage: float
    usage_percentage: float
    status: QuotaStatus
    warning_threshold: float
    reset_period: str
    next_reset: datetime


@dataclass
class TenantResources:
    """Container for all tenant-specific resources."""
    s3_bucket: str
    databricks_workspace_id: str
    unity_catalog_name: str
    iam_role_arn: str
    kms_key_arn: str
    vpc_endpoint_id: Optional[str] = None
    security_group_id: Optional[str] = None


# Resource Manager
class ResourceManager:
    """Manages multi-tenant resources, quotas, and isolation."""

    def __init__(self, db_session: Session, redis_client: redis.Redis):
        self.db = db_session
        self.redis = redis_client

        # AWS clients
        self.s3 = boto3.client('s3')
        self.iam = boto3.client('iam')
        self.kms = boto3.client('kms')
        self.ec2 = boto3.client('ec2')
        self.cloudwatch = boto3.client('cloudwatch')
        self.secrets_manager = boto3.client('secretsmanager')

        # Databricks client would be initialized here
        # self.databricks = DatabricksClient()

    async def create_organization(self, org_data: OrganizationCreate) -> Organization:
        """Create a new organization with full resource provisioning."""

        # Check if organization already exists
        existing_org = self.db.query(Organization).filter(
            Organization.name == org_data.name
        ).first()

        if existing_org:
            raise ValueError(f"Organization '{org_data.name}' already exists")

        # Create organization record
        organization = Organization(
            name=org_data.name,
            display_name=org_data.display_name,
            isolation_level=org_data.isolation_level,
            compliance_level=org_data.compliance_level
        )

        self.db.add(organization)
        self.db.flush()  # Get the ID

        try:
            # Create initial quotas
            for quota_data in org_data.initial_quotas:
                quota = ResourceQuota(
                    organization_id=organization.id,
                    resource_type=quota_data.resource_type,
                    quota_limit=quota_data.quota_limit,
                    warning_threshold=quota_data.warning_threshold,
                    reset_period=quota_data.reset_period
                )
                self.db.add(quota)

            # Create default quotas if none provided
            if not org_data.initial_quotas:
                await self._create_default_quotas(organization.id, org_data.compliance_level)

            self.db.commit()

            # Start resource provisioning
            await self._provision_organization_resources(organization)

            logger.info("Organization created successfully",
                        organization_id=str(organization.id),
                        name=org_data.name)

            return organization

        except Exception as e:
            self.db.rollback()
            logger.error("Failed to create organization",
                         name=org_data.name,
                         error=str(e))
            raise

    async def _create_default_quotas(self, organization_id: str, compliance_level: str):
        """Create default resource quotas based on compliance level."""

        # Define quota limits based on compliance level
        quota_templates = {
            "low": {
                ResourceType.DBU_HOURS: 1000,
                ResourceType.STORAGE_GB: 1000,
                ResourceType.API_CALLS: 100000,
                ResourceType.CONCURRENT_PIPELINES: 10,
                ResourceType.DATABRICKS_CLUSTERS: 5,
                ResourceType.S3_REQUESTS: 1000000,
                ResourceType.BANDWIDTH_GB: 100
            },
            "medium": {
                ResourceType.DBU_HOURS: 5000,
                ResourceType.STORAGE_GB: 10000,
                ResourceType.API_CALLS: 500000,
                ResourceType.CONCURRENT_PIPELINES: 25,
                ResourceType.DATABRICKS_CLUSTERS: 10,
                ResourceType.S3_REQUESTS: 5000000,
                ResourceType.BANDWIDTH_GB: 500
            },
            "high": {
                ResourceType.DBU_HOURS: 20000,
                ResourceType.STORAGE_GB: 100000,
                ResourceType.API_CALLS: 2000000,
                ResourceType.CONCURRENT_PIPELINES: 100,
                ResourceType.DATABRICKS_CLUSTERS: 50,
                ResourceType.S3_REQUESTS: 20000000,
                ResourceType.BANDWIDTH_GB: 2000
            }
        }

        limits = quota_templates.get(compliance_level, quota_templates["medium"])

        for resource_type, limit in limits.items():
            quota = ResourceQuota(
                organization_id=organization_id,
                resource_type=resource_type,
                quota_limit=limit,
                warning_threshold=0.8,
                reset_period="monthly"
            )
            self.db.add(quota)

    async def _provision_organization_resources(self, organization: Organization):
        """Provision all AWS and Databricks resources for an organization."""

        task = ProvisioningTask(
            organization_id=organization.id,
            task_type="create",
            resource_type="organization",
            configuration={
                "organization_name": organization.name,
                "isolation_level": organization.isolation_level,
                "compliance_level": organization.compliance_level
            }
        )

        self.db.add(task)
        self.db.commit()

        # Start provisioning in background
        asyncio.create_task(self._execute_provisioning_task(task.id))

    async def _execute_provisioning_task(self, task_id: str):
        """Execute a provisioning task."""

        task = self.db.query(ProvisioningTask).filter(
            ProvisioningTask.id == task_id
        ).first()

        if not task:
            logger.error("Provisioning task not found", task_id=task_id)
            return

        try:
            task.status = ProvisioningStatus.IN_PROGRESS
            task.started_at = datetime.utcnow()
            self.db.commit()

            if task.task_type == "create" and task.resource_type == "organization":
                resources = await self._provision_tenant_resources(task.organization_id)
                task.result = {
                    "s3_bucket": resources.s3_bucket,
                    "databricks_workspace_id": resources.databricks_workspace_id,
                    "unity_catalog_name": resources.unity_catalog_name,
                    "iam_role_arn": resources.iam_role_arn,
                    "kms_key_arn": resources.kms_key_arn
                }

            task.status = ProvisioningStatus.COMPLETED
            task.completed_at = datetime.utcnow()
            self.db.commit()

            logger.info("Provisioning task completed",
                        task_id=task_id,
                        organization_id=str(task.organization_id))

        except Exception as e:
            task.status = ProvisioningStatus.FAILED
            task.error_message = str(e)
            task.completed_at = datetime.utcnow()
            self.db.commit()

            logger.error("Provisioning task failed",
                         task_id=task_id,
                         error=str(e))

    async def _provision_tenant_resources(self, organization_id: str) -> TenantResources:
        """Provision all resources for a tenant."""

        org = self.db.query(Organization).filter(
            Organization.id == organization_id
        ).first()

        if not org:
            raise ValueError("Organization not found")

        org_name = org.name

        try:
            # Create S3 bucket with encryption
            bucket_name = f"multi-tenant-ingestion-{org_name}-{uuid.uuid4().hex[:8]}"

            self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': 'us-west-2'  # Replace with your region
                }
            )

            # Configure bucket encryption
            self.s3.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [{
                        'ApplyServerSideEncryptionByDefault': {
                            'SSEAlgorithm': 'aws:kms'
                        },
                        'BucketKeyEnabled': True
                    }]
                }
            )

            # Create bucket policy for tenant isolation
            bucket_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DenyInsecureConnections",
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:*",
                        "Resource": [
                            f"arn:aws:s3:::{bucket_name}",
                            f"arn:aws:s3:::{bucket_name}/*"
                        ],
                        "Condition": {
                            "Bool": {
                                "aws:SecureTransport": "false"
                            }
                        }
                    }
                ]
            }

            self.s3.put_bucket_policy(
                Bucket=bucket_name,
                Policy=json.dumps(bucket_policy)
            )

            # Create KMS key for tenant
            kms_response = self.kms.create_key(
                Description=f"KMS key for organization {org_name}",
                Usage='ENCRYPT_DECRYPT',
                KeySpec='SYMMETRIC_DEFAULT'
            )

            kms_key_arn = kms_response['KeyMetadata']['Arn']

            # Create alias for the key
            self.kms.create_alias(
                AliasName=f"alias/multi-tenant-{org_name}",
                TargetKeyId=kms_response['KeyMetadata']['KeyId']
            )

            # Create IAM role for tenant
            assume_role_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": ["databricks.amazonaws.com", "ecs-tasks.amazonaws.com"]
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            role_name = f"MultiTenantIngestion-{org_name}-Role"

            iam_response = self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                Description=f"IAM role for organization {org_name}"
            )

            iam_role_arn = iam_response['Role']['Arn']

            # Attach policies to the role
            tenant_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{bucket_name}",
                            f"arn:aws:s3:::{bucket_name}/*"
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kms:Encrypt",
                            "kms:Decrypt",
                            "kms:ReEncrypt*",
                            "kms:GenerateDataKey*",
                            "kms:DescribeKey"
                        ],
                        "Resource": kms_key_arn
                    }
                ]
            }

            policy_name = f"MultiTenantIngestion-{org_name}-Policy"

            self.iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(tenant_policy),
                Description=f"Policy for organization {org_name}"
            )

            self.iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn=f"arn:aws:iam::{boto3.Session().get_credentials().access_key}:policy/{policy_name}"
            )

            # Create Databricks workspace (placeholder - would use Databricks API)
            databricks_workspace_id = f"workspace-{org_name}-{uuid.uuid4().hex[:8]}"
            unity_catalog_name = f"{org_name}_catalog"

            # Store tenant isolation configuration
            isolation_config = TenantIsolation(
                organization_id=organization_id,
                resource_type="s3",
                resource_arn=f"arn:aws:s3:::{bucket_name}",
                isolation_config={
                    "bucket_policy": bucket_policy,
                    "kms_key_arn": kms_key_arn,
                    "iam_role_arn": iam_role_arn
                }
            )

            self.db.add(isolation_config)

            # Store secrets
            secret_value = {
                "s3_bucket": bucket_name,
                "kms_key_arn": kms_key_arn,
                "iam_role_arn": iam_role_arn,
                "databricks_workspace_id": databricks_workspace_id,
                "unity_catalog_name": unity_catalog_name
            }

            self.secrets_manager.create_secret(
                Name=f"multi-tenant-ingestion/{org_name}/resources",
                Description=f"Resource configuration for organization {org_name}",
                SecretString=json.dumps(secret_value)
            )

            self.db.commit()

            return TenantResources(
                s3_bucket=bucket_name,
                databricks_workspace_id=databricks_workspace_id,
                unity_catalog_name=unity_catalog_name,
                iam_role_arn=iam_role_arn,
                kms_key_arn=kms_key_arn
            )

        except Exception as e:
            logger.error("Failed to provision tenant resources",
                         organization_id=str(organization_id),
                         error=str(e))
            raise

    async def record_usage(self, organization_id: str, usage_record: UsageRecord):
        """Record resource usage for an organization."""

        # Get current period bounds
        now = datetime.utcnow()
        period_start, period_end = self._get_period_bounds(now, "monthly")  # Default to monthly

        # Check if usage record already exists for this period
        existing_usage = self.db.query(ResourceUsage).filter(
            ResourceUsage.organization_id == organization_id,
            ResourceUsage.resource_type == usage_record.resource_type,
            ResourceUsage.period_start == period_start,
            ResourceUsage.period_end == period_end
        ).first()

        if existing_usage:
            # Update existing record
            existing_usage.usage_amount += usage_record.usage_amount
            existing_usage.metadata.update(usage_record.metadata)
        else:
            # Create new record
            new_usage = ResourceUsage(
                organization_id=organization_id,
                resource_type=usage_record.resource_type,
                usage_amount=usage_record.usage_amount,
                period_start=period_start,
                period_end=period_end,
                metadata=usage_record.metadata
            )
            self.db.add(new_usage)

        self.db.commit()

        # Check quota after recording usage
        await self._check_quota_violations(organization_id, usage_record.resource_type)

        # Update cache
        cache_key = f"usage:{organization_id}:{usage_record.resource_type}"
        await self.redis.setex(
            cache_key,
            300,  # 5 minutes TTL
            json.dumps({
                "usage_amount": usage_record.usage_amount,
                "recorded_at": now.isoformat()
            })
        )

    async def _check_quota_violations(self, organization_id: str, resource_type: str):
        """Check for quota violations and take appropriate action."""

        # Get quota for this resource type
        quota = self.db.query(ResourceQuota).filter(
            ResourceQuota.organization_id == organization_id,
            ResourceQuota.resource_type == resource_type,
            ResourceQuota.is_active == True
        ).first()

        if not quota:
            return

        # Get current usage
        current_usage = await self.get_current_usage(organization_id, resource_type)
        usage_percentage = current_usage / quota.quota_limit if quota.quota_limit > 0 else 0

        # Check for violations
        if usage_percentage >= 1.0:  # Exceeded quota
            await self._handle_quota_violation(
                organization_id, resource_type, quota.quota_limit,
                current_usage, "exceeded"
            )
        elif usage_percentage >= quota.warning_threshold:  # Warning threshold
            await self._handle_quota_violation(
                organization_id, resource_type, quota.quota_limit,
                current_usage, "warning"
            )

    async def _handle_quota_violation(self, organization_id: str, resource_type: str,
                                      quota_limit: float, actual_usage: float,
                                      violation_type: str):
        """Handle quota violations with appropriate actions."""

        # Check if violation already recorded recently
        recent_violation = self.db.query(QuotaViolation).filter(
            QuotaViolation.organization_id == organization_id,
            QuotaViolation.resource_type == resource_type,
            QuotaViolation.violation_type == violation_type,
            QuotaViolation.created_at >= datetime.utcnow() - timedelta(hours=1),
            QuotaViolation.resolved == False
        ).first()

        if recent_violation:
            return  # Don't create duplicate violations

        action_taken = None

        if violation_type == "exceeded":
            # Take enforcement action based on resource type
            if resource_type == ResourceType.CONCURRENT_PIPELINES:
                action_taken = "suspend_new_pipelines"
                await self._suspend_organization_pipelines(organization_id)
            elif resource_type == ResourceType.API_CALLS:
                action_taken = "rate_limit_api"
                await self._apply_rate_limiting(organization_id)
            else:
                action_taken = "alert_only"
        else:
            action_taken = "warning_sent"

        # Record violation
        violation = QuotaViolation(
            organization_id=organization_id,
            resource_type=resource_type,
            quota_limit=quota_limit,
            actual_usage=actual_usage,
            violation_type=violation_type,
            action_taken=action_taken
        )

        self.db.add(violation)
        self.db.commit()

        # Send alerts
        await self._send_quota_alert(organization_id, violation)

        logger.warning("Quota violation detected",
                       organization_id=str(organization_id),
                       resource_type=resource_type,
                       violation_type=violation_type,
                       actual_usage=actual_usage,
                       quota_limit=quota_limit,
                       action_taken=action_taken)

    async def _suspend_organization_pipelines(self, organization_id: str):
        """Suspend new pipeline executions for an organization."""

        # Set suspension flag in Redis
        await self.redis.setex(
            f"pipeline_suspension:{organization_id}",
            86400,  # 24 hours
            "suspended_quota_exceeded"
        )

        logger.info("Pipeline execution suspended due to quota violation",
                    organization_id=str(organization_id))

    async def _apply_rate_limiting(self, organization_id: str):
        """Apply API rate limiting for an organization."""

        # Set rate limiting in Redis
        await self.redis.setex(
            f"rate_limit:{organization_id}",
            86400,  # 24 hours
            json.dumps({
                "requests_per_minute": 10,  # Reduced from normal quota
                "reason": "quota_exceeded"
            })
        )

        logger.info("API rate limiting applied due to quota violation",
                    organization_id=str(organization_id))

    async def _send_quota_alert(self, organization_id: str, violation: QuotaViolation):
        """Send quota violation alerts."""

        # This would integrate with the alerting system
        alert_data = {
            "type": "quota_violation",
            "organization_id": str(organization_id),
            "resource_type": violation.resource_type,
            "violation_type": violation.violation_type,
            "quota_limit": violation.quota_limit,
            "actual_usage": violation.actual_usage,
            "action_taken": violation.action_taken
        }

        # Send to monitoring service
        # await self.send_alert(alert_data)

        logger.info("Quota violation alert sent", **alert_data)

    async def get_current_usage(self, organization_id: str, resource_type: str) -> float:
        """Get current usage for a resource type."""

        # Try cache first
        cache_key = f"usage_current:{organization_id}:{resource_type}"
        cached_usage = await self.redis.get(cache_key)

        if cached_usage:
            return float(cached_usage)

        # Calculate from database
        now = datetime.utcnow()
        period_start, period_end = self._get_period_bounds(now, "monthly")

        usage_record = self.db.query(ResourceUsage).filter(
            ResourceUsage.organization_id == organization_id,
            ResourceUsage.resource_type == resource_type,
            ResourceUsage.period_start == period_start,
            ResourceUsage.period_end == period_end
        ).first()

        current_usage = usage_record.usage_amount if usage_record else 0.0

        # Cache result
        await self.redis.setex(cache_key, 300, str(current_usage))

        return current_usage

    async def get_quota_status(self, organization_id: str) -> List[ResourceQuotaStatus]:
        """Get quota status for all resources of an organization."""

        quotas = self.db.query(ResourceQuota).filter(
            ResourceQuota.organization_id == organization_id,
            ResourceQuota.is_active == True
        ).all()

        quota_statuses = []

        for quota in quotas:
            current_usage = await self.get_current_usage(organization_id, quota.resource_type)
            usage_percentage = current_usage / quota.quota_limit if quota.quota_limit > 0 else 0

            # Determine status
            if usage_percentage >= 1.0:
                status = QuotaStatus.EXCEEDED
            elif usage_percentage >= quota.warning_threshold:
                status = QuotaStatus.WARNING
            else:
                status = QuotaStatus.ACTIVE

            # Calculate next reset time
            now = datetime.utcnow()
            _, period_end = self._get_period_bounds(now, quota.reset_period)

            quota_status = ResourceQuotaStatus(
                resource_type=quota.resource_type,
                quota_limit=quota.quota_limit,
                current_usage=current_usage,
                usage_percentage=usage_percentage,
                status=status,
                warning_threshold=quota.warning_threshold,
                reset_period=quota.reset_period,
                next_reset=period_end
            )

            quota_statuses.append(quota_status)

        return quota_statuses

    def _get_period_bounds(self, timestamp: datetime, reset_period: str) -> tuple[datetime, datetime]:
        """Get period start and end times based on reset period."""

        if reset_period == "hourly":
            period_start = timestamp.replace(minute=0, second=0, microsecond=0)
            period_end = period_start + timedelta(hours=1)
        elif reset_period == "daily":
            period_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end = period_start + timedelta(days=1)
        elif reset_period == "weekly":
            days_since_monday = timestamp.weekday()
            period_start = (timestamp - timedelta(days=days_since_monday)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            period_end = period_start + timedelta(weeks=1)
        else:  # monthly
            period_start = timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if period_start.month == 12:
                period_end = period_start.replace(year=period_start.year + 1, month=1)
            else:
                period_end = period_start.replace(month=period_start.month + 1)

        return period_start, period_end

    async def delete_organization(self, organization_id: str) -> bool:
        """Delete an organization and all its resources."""

        org = self.db.query(Organization).filter(
            Organization.id == organization_id
        ).first()

        if not org:
            raise ValueError("Organization not found")

        try:
            # Create deprovisioning task
            task = ProvisioningTask(
                organization_id=organization_id,
                task_type="delete",
                resource_type="organization",
                configuration={"organization_name": org.name}
            )

            self.db.add(task)
            self.db.commit()

            # Start deprovisioning
            await self._deprovision_organization_resources(organization_id)

            # Mark organization as inactive
            org.is_active = False
            self.db.commit()

            logger.info("Organization deletion initiated",
                        organization_id=str(organization_id),
                        name=org.name)

            return True

        except Exception as e:
            logger.error("Failed to delete organization",
                         organization_id=str(organization_id),
                         error=str(e))
            raise

    async def _deprovision_organization_resources(self, organization_id: str):
        """Deprovision all resources for an organization."""

        try:
            # Get resource configuration from secrets
            org = self.db.query(Organization).filter(
                Organization.id == organization_id
            ).first()

            secret_name = f"multi-tenant-ingestion/{org.name}/resources"

            try:
                response = self.secrets_manager.get_secret_value(SecretId=secret_name)
                resources = json.loads(response['SecretString'])
            except ClientError:
                logger.warning("Could not retrieve resource configuration",
                               organization_id=str(organization_id))
                return

            # Delete S3 bucket (after emptying it)
            bucket_name = resources.get('s3_bucket')
            if bucket_name:
                try:
                    # Empty bucket first
                    objects = self.s3.list_objects_v2(Bucket=bucket_name)
                    if 'Contents' in objects:
                        delete_keys = [{'Key': obj['Key']} for obj in objects['Contents']]
                        self.s3.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': delete_keys}
                        )

                    # Delete bucket
                    self.s3.delete_bucket(Bucket=bucket_name)
                    logger.info("S3 bucket deleted", bucket=bucket_name)
                except ClientError as e:
                    logger.error("Failed to delete S3 bucket", bucket=bucket_name, error=str(e))

            # Delete IAM role and policy
            iam_role_arn = resources.get('iam_role_arn')
            if iam_role_arn:
                role_name = iam_role_arn.split('/')[-1]
                try:
                    # Detach policies
                    attached_policies = self.iam.list_attached_role_policies(RoleName=role_name)
                    for policy in attached_policies['AttachedPolicies']:
                        self.iam.detach_role_policy(
                            RoleName=role_name,
                            PolicyArn=policy['PolicyArn']
                        )
                        # Delete custom policies
                        if 'MultiTenantIngestion' in policy['PolicyArn']:
                            self.iam.delete_policy(PolicyArn=policy['PolicyArn'])

                    # Delete role
                    self.iam.delete_role(RoleName=role_name)
                    logger.info("IAM role deleted", role=role_name)
                except ClientError as e:
                    logger.error("Failed to delete IAM role", role=role_name, error=str(e))

            # Delete KMS key alias
            try:
                self.kms.delete_alias(AliasName=f"alias/multi-tenant-{org.name}")
                logger.info("KMS alias deleted", organization=org.name)
            except ClientError as e:
                logger.error("Failed to delete KMS alias", error=str(e))

            # Delete secrets
            try:
                self.secrets_manager.delete_secret(
                    SecretId=secret_name,
                    ForceDeleteWithoutRecovery=True
                )
                logger.info("Secrets deleted", secret_name=secret_name)
            except ClientError as e:
                logger.error("Failed to delete secrets", error=str(e))

            # Delete tenant isolation records
            self.db.query(TenantIsolation).filter(
                TenantIsolation.organization_id == organization_id
            ).delete()

            self.db.commit()

            logger.info("Organization resources deprovisioned",
                        organization_id=str(organization_id))

        except Exception as e:
            logger.error("Failed to deprovision organization resources",
                         organization_id=str(organization_id),
                         error=str(e))
            raise


# Usage Tracker for real-time monitoring
class UsageTracker:
    """Track resource usage in real-time with efficient caching."""

    def __init__(self, resource_manager: ResourceManager, redis_client: redis.Redis):
        self.resource_manager = resource_manager
        self.redis = redis_client
        self.batch_size = 100
        self.flush_interval = 60  # seconds

        # Start background task for batch processing
        asyncio.create_task(self._batch_processor())

    async def track_usage(self, organization_id: str, resource_type: ResourceType,
                          amount: float, metadata: Dict[str, Any] = None):
        """Track resource usage with batching for performance."""

        usage_data = {
            "organization_id": organization_id,
            "resource_type": resource_type,
            "amount": amount,
            "metadata": metadata or {},
            "timestamp": datetime.utcnow().isoformat()
        }

        # Add to batch queue
        await self.redis.lpush(
            "usage_batch_queue",
            json.dumps(usage_data, default=str)
        )

        # Update real-time counters
        counter_key = f"usage_counter:{organization_id}:{resource_type}"
        await self.redis.incrbyfloat(counter_key, amount)
        await self.redis.expire(counter_key, 3600)  # 1 hour TTL

    async def _batch_processor(self):
        """Process usage records in batches."""

        while True:
            try:
                batch = []

                # Collect batch
                for _ in range(self.batch_size):
                    item = await self.redis.rpop("usage_batch_queue")
                    if not item:
                        break
                    batch.append(json.loads(item))

                if batch:
                    # Group by organization and resource type
                    grouped_usage = {}
                    for item in batch:
                        key = (item["organization_id"], item["resource_type"])
                        if key not in grouped_usage:
                            grouped_usage[key] = {
                                "total_amount": 0,
                                "metadata": {},
                                "count": 0
                            }

                        grouped_usage[key]["total_amount"] += item["amount"]
                        grouped_usage[key]["metadata"].update(item["metadata"])
                        grouped_usage[key]["count"] += 1

                    # Record aggregated usage
                    for (org_id, resource_type), data in grouped_usage.items():
                        usage_record = UsageRecord(
                            resource_type=resource_type,
                            usage_amount=data["total_amount"],
                            metadata={
                                **data["metadata"],
                                "batch_count": data["count"],
                                "batch_processed_at": datetime.utcnow().isoformat()
                            }
                        )

                        await self.resource_manager.record_usage(org_id, usage_record)

                await asyncio.sleep(self.flush_interval)

            except Exception as e:
                logger.error("Batch processing failed", error=str(e))
                await asyncio.sleep(10)


# FastAPI Application
app = FastAPI(title="Multi-Tenant Resource Manager", version="1.0.0")
security = HTTPBearer()


# Dependency injection
def get_db():
    engine = create_engine("postgresql://user:pass@localhost/db")
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_redis():
    return redis.Redis(host='redis', port=6379, decode_responses=True)


def get_resource_manager(db: Session = Depends(get_db), redis_client: redis.Redis = Depends(get_redis)):
    return ResourceManager(db, redis_client)


def get_usage_tracker(resource_manager: ResourceManager = Depends(get_resource_manager),
                      redis_client: redis.Redis = Depends(get_redis)):
    return UsageTracker(resource_manager, redis_client)


# API Endpoints
@app.post("/organizations", response_model=Organization)
async def create_organization(
        org_data: OrganizationCreate,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Create a new organization with resource provisioning."""
    return await resource_manager.create_organization(org_data)


@app.get("/organizations/{organization_id}/quotas")
async def get_organization_quotas(
        organization_id: str,
        resource_manager: ResourceManager = Depends(get_resource_manager)
) -> List[ResourceQuotaStatus]:
    """Get quota status for an organization."""
    return await resource_manager.get_quota_status(organization_id)


@app.post("/organizations/{organization_id}/quotas")
async def create_quota(
        organization_id: str,
        quota_data: QuotaCreate,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Create a new resource quota."""
    quota = ResourceQuota(
        organization_id=organization_id,
        resource_type=quota_data.resource_type,
        quota_limit=quota_data.quota_limit,
        warning_threshold=quota_data.warning_threshold,
        reset_period=quota_data.reset_period
    )

    resource_manager.db.add(quota)
    resource_manager.db.commit()

    return quota


@app.put("/organizations/{organization_id}/quotas/{resource_type}")
async def update_quota(
        organization_id: str,
        resource_type: ResourceType,
        quota_update: QuotaUpdate,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Update a resource quota."""
    quota = resource_manager.db.query(ResourceQuota).filter(
        ResourceQuota.organization_id == organization_id,
        ResourceQuota.resource_type == resource_type,
        ResourceQuota.is_active == True
    ).first()

    if not quota:
        raise HTTPException(status_code=404, detail="Quota not found")

    update_data = quota_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(quota, field, value)

    quota.updated_at = datetime.utcnow()
    resource_manager.db.commit()

    return quota


@app.post("/organizations/{organization_id}/usage")
async def record_usage(
        organization_id: str,
        usage_record: UsageRecord,
        usage_tracker: UsageTracker = Depends(get_usage_tracker)
):
    """Record resource usage."""
    await usage_tracker.track_usage(
        organization_id,
        usage_record.resource_type,
        usage_record.usage_amount,
        usage_record.metadata
    )

    return {"status": "recorded", "organization_id": organization_id}


@app.get("/organizations/{organization_id}/usage/{resource_type}")
async def get_current_usage(
        organization_id: str,
        resource_type: ResourceType,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Get current usage for a resource type."""
    current_usage = await resource_manager.get_current_usage(organization_id, resource_type)

    return {
        "organization_id": organization_id,
        "resource_type": resource_type,
        "current_usage": current_usage
    }


@app.get("/organizations/{organization_id}/violations")
async def get_quota_violations(
        organization_id: str,
        resolved: Optional[bool] = None,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Get quota violations for an organization."""
    query = resource_manager.db.query(QuotaViolation).filter(
        QuotaViolation.organization_id == organization_id
    )

    if resolved is not None:
        query = query.filter(QuotaViolation.resolved == resolved)

    violations = query.order_by(QuotaViolation.created_at.desc()).limit(100).all()

    return violations


@app.post("/organizations/{organization_id}/violations/{violation_id}/resolve")
async def resolve_quota_violation(
        organization_id: str,
        violation_id: str,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Resolve a quota violation."""
    violation = resource_manager.db.query(QuotaViolation).filter(
        QuotaViolation.id == violation_id,
        QuotaViolation.organization_id == organization_id
    ).first()

    if not violation:
        raise HTTPException(status_code=404, detail="Violation not found")

    violation.resolved = True
    violation.resolved_at = datetime.utcnow()
    resource_manager.db.commit()

    return violation


@app.delete("/organizations/{organization_id}")
async def delete_organization(
        organization_id: str,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Delete an organization and all its resources."""
    success = await resource_manager.delete_organization(organization_id)

    return {"status": "deleted" if success else "failed", "organization_id": organization_id}


@app.get("/organizations/{organization_id}/provisioning")
async def get_provisioning_status(
        organization_id: str,
        resource_manager: ResourceManager = Depends(get_resource_manager)
):
    """Get provisioning status for an organization."""
    tasks = resource_manager.db.query(ProvisioningTask).filter(
        ProvisioningTask.organization_id == organization_id
    ).order_by(ProvisioningTask.created_at.desc()).limit(10).all()

    return tasks


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "resource-manager"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8005)  # services/resource-manager/app/main.py
