import aiohttp
import json
from typing import Dict, Any, Optional
from .config import get_settings

settings = get_settings()


class DatabricksClient:
    """Client for interacting with Databricks API"""

    def __init__(self):
        self.base_url = None  # Will be set per tenant
        self.token = None  # Will be set per tenant

    async def _get_tenant_config(self, tenant_id: str) -> Dict[str, str]:
        """Get Databricks configuration for tenant"""
        # In production, this would fetch from tenant configuration
        # For now, return default values
        return {
            "workspace_url": settings.DATABRICKS_WORKSPACE_URL,
            "token": settings.DATABRICKS_TOKEN
        }

    async def submit_notebook(
            self,
            tenant_id: str,
            pipeline_id: str,
            execution_id: str,
            notebook_content: str,
            cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Submit notebook for execution"""

        tenant_config = await self._get_tenant_config(tenant_id)

        # Create job configuration
        job_config = {
            "run_name": f"pipeline-{pipeline_id}-{execution_id}",
            "existing_cluster_id": cluster_config.get("cluster_id"),
            "notebook_task": {
                "notebook_path": f"/tmp/pipeline-{pipeline_id}-{execution_id}",
                "base_parameters": {
                    "execution_id": execution_id,
                    "pipeline_id": pipeline_id,
                    "tenant_id": tenant_id
                }
            },
            "timeout_seconds": cluster_config.get("timeout_seconds", 3600)
        }

        # If no existing cluster, create new one
        if not cluster_config.get("cluster_id"):
            job_config["new_cluster"] = {
                "spark_version": cluster_config.get("spark_version", "11.3.x-scala2.12"),
                "node_type_id": cluster_config.get("node_type_id", "i3.xlarge"),
                "num_workers": cluster_config.get("num_workers", 2),
                "spark_conf": cluster_config.get("spark_conf", {}),
                "aws_attributes": {
                    "zone_id": "us-west-2a",
                    "instance_profile_arn": cluster_config.get("instance_profile_arn")
                }
            }
            del job_config["existing_cluster_id"]

        # Upload notebook first
        await self._upload_notebook(
            tenant_config["workspace_url"],
            tenant_config["token"],
            f"/tmp/pipeline-{pipeline_id}-{execution_id}",
            notebook_content
        )

        # Submit job
        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {tenant_config['token']}",
                "Content-Type": "application/json"
            }

            async with session.post(
                    f"{tenant_config['workspace_url']}/api/2.1/jobs/runs/submit",
                    headers=headers,
                    json=job_config
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    return {
                        "job_id": result.get("job_id"),
                        "run_id": result.get("run_id")
                    }
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to submit job: {response.status} - {error_text}")

    async def _upload_notebook(
            self,
            workspace_url: str,
            token: str,
            path: str,
            content: str
    ):
        """Upload notebook to Databricks workspace"""

        import base64

        # Encode notebook content
        encoded_content = base64.b64encode(content.encode()).decode()

        upload_config = {
            "path": path,
            "content": encoded_content,
            "language": "PYTHON",
            "format": "SOURCE",
            "overwrite": True
        }

        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            async with session.post(
                    f"{workspace_url}/api/2.0/workspace/import",
                    headers=headers,
                    json=upload_config
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"Failed to upload notebook: {response.status} - {error_text}")

    async def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get job execution status"""

        # This would use tenant-specific config in production
        tenant_config = await self._get_tenant_config("default")

        async with aiohttp.ClientSession() as session:
            headers = {
                "Authorization": f"Bearer {tenant_config['token']}",
                "Content-Type": "application/json"
            }

            async with session.get(
                    f"{tenant_config['workspace_url']}/api/2.1/jobs/runs/get?run_id={run_id}",
                    headers=headers
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to get job status: {response.status} - {error_text}")

    async def get_job_metrics(self, run_id: str) -> Dict[str, Any]:
        """Get job execution metrics"""

        # This would extract metrics from job logs/output
        # For now, return sample metrics
        return {
            "records_processed": 1000,
            "records_failed": 0,
            "bytes_processed": 1024000,
            "execution_time_seconds": 120
        }