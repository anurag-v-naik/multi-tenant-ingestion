import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from fastapi import HTTPException
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class ResourceUsage:
    """Current resource usage for an organization"""
    dbu_usage: float
    storage_gb: float
    compute_nodes: int
    cost_this_month: float
    timestamp: datetime


@dataclass
class ResourceQuota:
    """Resource quota limits for an organization"""
    max_dbu_per_hour: int
    max_storage_gb: int
    max_compute_nodes: int
    monthly_budget_usd: float


class ResourceQuotaEnforcer:
    """Enforces resource quotas and manages cost tracking for organizations"""

    def __init__(self, aws_region: str = "us-west-2"):
        self.aws_region = aws_region
        self.cloudwatch = boto3.client('cloudwatch', region_name=aws_region)
        self.ce = boto3.client('ce', region_name=aws_region)  # Cost Explorer
        self.pricing = boto3.client('pricing', region_name='us-east-1')  # Pricing API only in us-east-1

    async def check_quota_compliance(self, org_id: str) -> bool:
        """Check if organization is within resource quotas"""
        try:
            current_usage = await self.get_current_usage(org_id)
            quota_limits = await self.get_organization_quota(org_id)

            # Check DBU usage
            if current_usage.dbu_usage >= quota_limits.max_dbu_per_hour:
                await self._send_quota_alert(org_id, "DBU", current_usage.dbu_usage, quota_limits.max_dbu_per_hour)
                return False

            # Check storage usage
            if current_usage.storage_gb >= quota_limits.max_storage_gb:
                await self._send_quota_alert(org_id, "Storage", current_usage.storage_gb, quota_limits.max_storage_gb)
                return False

            # Check compute nodes
            if current_usage.compute_nodes >= quota_limits.max_compute_nodes:
                await self._send_quota_alert(org_id, "Compute Nodes", current_usage.compute_nodes,
                                             quota_limits.max_compute_nodes)
                return False

            # Check monthly budget
            if current_usage.cost_this_month >= quota_limits.monthly_budget_usd:
                await self._send_quota_alert(org_id, "Monthly Budget", current_usage.cost_this_month,
                                             quota_limits.monthly_budget_usd)
                return False

            logger.info(f"Organization {org_id} is within all resource quotas")
            return True

        except Exception as e:
            logger.error(f"Error checking quota compliance for {org_id}: {str(e)}")
            # Fail open for availability, but log the error
            return True

    async def get_current_usage(self, org_id: str) -> ResourceUsage:
        """Get current resource usage for organization"""
        try:
            # Get DBU usage from CloudWatch
            dbu_usage = await self._get_dbu_usage(org_id)

            # Get storage usage from CloudWatch
            storage_gb = await self._get_storage_usage(org_id)

            # Get compute node count
            compute_nodes = await self._get_compute_node_count(org_id)

            # Get current month cost
            cost_this_month = await self._get_monthly_cost(org_id)

            return ResourceUsage(
                dbu_usage=dbu_usage,
                storage_gb=storage_gb,
                compute_nodes=compute_nodes,
                cost_this_month=cost_this_month,
                timestamp=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"Error getting usage for {org_id}: {str(e)}")
            # Return zero usage on error to prevent false quota violations
            return ResourceUsage(0, 0, 0, 0, datetime.utcnow())

    async def _get_dbu_usage(self, org_id: str) -> float:
        """Get DBU usage from CloudWatch metrics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            response = self.cloudwatch.get_metric_statistics(
                Namespace='Databricks/Usage',
                MetricName='DBUConsumption',
                Dimensions=[
                    {
                        'Name': 'OrganizationId',
                        'Value': org_id
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,  # 1 hour
                Statistics=['Sum']
            )

            if response['Datapoints']:
                return response['Datapoints'][-1]['Sum']
            return 0.0

        except ClientError as e:
            logger.error(f"CloudWatch error getting DBU usage for {org_id}: {str(e)}")
            return 0.0

    async def _get_storage_usage(self, org_id: str) -> float:
        """Get storage usage from CloudWatch metrics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            response = self.cloudwatch.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName='BucketSizeBytes',
                Dimensions=[
                    {
                        'Name': 'BucketName',
                        'Value': f'multi-tenant-{org_id}-data'
                    },
                    {
                        'Name': 'StorageType',
                        'Value': 'StandardStorage'
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average']
            )

            if response['Datapoints']:
                bytes_used = response['Datapoints'][-1]['Average']
                return bytes_used / (1024 ** 3)  # Convert to GB
            return 0.0

        except ClientError as e:
            logger.error(f"CloudWatch error getting storage usage for {org_id}: {str(e)}")
            return 0.0

    async def _get_compute_node_count(self, org_id: str) -> int:
        """Get current compute node count for organization"""
        try:
            # This would integrate with Databricks API to get cluster info
            # Placeholder implementation
            response = self.cloudwatch.get_metric_statistics(
                Namespace='Databricks/Clusters',
                MetricName='ActiveNodes',
                Dimensions=[
                    {
                        'Name': 'OrganizationId',
                        'Value': org_id
                    }
                ],
                StartTime=datetime.utcnow() - timedelta(minutes=5),
                EndTime=datetime.utcnow(),
                Period=300,
                Statistics=['Maximum']
            )

            if response['Datapoints']:
                return int(response['Datapoints'][-1]['Maximum'])
            return 0

        except ClientError as e:
            logger.error(f"Error getting compute node count for {org_id}: {str(e)}")
            return 0

    async def _get_monthly_cost(self, org_id: str) -> float:
        """Get current month's cost for organization"""
        try:
            # Get first day of current month
            now = datetime.utcnow()
            start_of_month = datetime(now.year, now.month, 1)

            response = self.ce.get_cost_and_usage(
                TimePeriod={
                    'Start': start_of_month.strftime('%Y-%m-%d'),
                    'End': now.strftime('%Y-%m-%d')
                },
                Granularity='MONTHLY',
                Metrics=['BlendedCost'],
                GroupBy=[
                    {
                        'Type': 'TAG',
                        'Key': 'Organization'
                    }
                ],
                Filter={
                    'Tags': {
                        'Key': 'Organization',
                        'Values': [org_id]
                    }
                }
            )

            if response['ResultsByTime']:
                for group in response['ResultsByTime'][0]['Groups']:
                    if group['Keys'][0] == org_id:
                        return float(group['Metrics']['BlendedCost']['Amount'])

            return 0.0

        except ClientError as e:
            logger.error(f"Cost Explorer error for {org_id}: {str(e)}")
            return 0.0

    async def get_organization_quota(self, org_id: str) -> ResourceQuota:
        """Get quota limits for organization from configuration"""
        # This would typically come from a database or configuration service
        # Using hardcoded values for demonstration
        quota_configs = {
            "finance": ResourceQuota(1000, 50000, 50, 50000.0),
            "retail": ResourceQuota(500, 25000, 25, 25000.0),
            "marketing": ResourceQuota(300, 15000, 15, 15000.0)
        }

        return quota_configs.get(org_id, ResourceQuota(100, 5000, 5, 5000.0))

    async def _send_quota_alert(self, org_id: str, resource_type: str, current: float, limit: float):
        """Send quota violation alert"""
        logger.warning(f"Quota violation for {org_id}: {resource_type} usage {current} exceeds limit {limit}")

        # This would integrate with your alerting system (SNS, email, Slack, etc.)
        try:
            # Example: Send SNS notification
            sns = boto3.client('sns', region_name=self.aws_region)
            message = {
                "organization_id": org_id,
                "resource_type": resource_type,
                "current_usage": current,
                "quota_limit": limit,
                "timestamp": datetime.utcnow().isoformat(),
                "severity": "WARNING"
            }

            # This would use actual SNS topic ARN from configuration
            topic_arn = f"arn:aws:sns:{self.aws_region}:123456789012:multi-tenant-quota-alerts"

            sns.publish(
                TopicArn=topic_arn,
                Message=str(message),
                Subject=f"Quota Alert: {resource_type} limit exceeded for {org_id}"
            )

        except Exception as e:
            logger.error(f"Failed to send quota alert for {org_id}: {str(e)}")


# Usage example
async def enforce_quota_for_operation(org_id: str, enforcer: ResourceQuotaEnforcer):
    """Helper function to check quotas before executing operations"""
    if not await enforcer.check_quota_compliance(org_id):
        raise HTTPException(
            status_code=429,
            detail=f"Resource quota exceeded for organization {org_id}. Please contact your administrator."
        )
