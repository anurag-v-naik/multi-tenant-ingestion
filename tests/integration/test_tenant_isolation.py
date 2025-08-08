import pytest
import httpx
import jwt
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any

# Test configuration
TEST_BASE_URL = "http://localhost:8000"
JWT_SECRET = "your-secret-key"
JWT_ALGORITHM = "HS256"


class TestTenantIsolation:
    """Comprehensive tenant isolation testing"""

    @pytest.fixture
    def finance_token(self) -> str:
        """Generate JWT token for Finance organization"""
        payload = {
            "org_id": "finance",
            "user_id": "finance_user_001",
            "roles": ["admin"],
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    @pytest.fixture
    def retail_token(self) -> str:
        """Generate JWT token for Retail organization"""
        payload = {
            "org_id": "retail",
            "user_id": "retail_user_001",
            "roles": ["user"],
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    @pytest.fixture
    def expired_token(self) -> str:
        """Generate expired JWT token"""
        payload = {
            "org_id": "finance",
            "user_id": "finance_user_001",
            "roles": ["admin"],
            "exp": datetime.utcnow() - timedelta(hours=1)
        }
        return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

    @pytest.mark.asyncio
    async def test_cross_tenant_data_access_prevention(self, finance_token: str, retail_token: str):
        """Test that retail organization cannot access finance data"""
        async with httpx.AsyncClient() as client:
            # Attempt to access finance pipelines with retail token
            response = await client.get(
                f"{TEST_BASE_URL}/api/v1/pipelines",
                headers={
                    "Authorization": f"Bearer {retail_token}",
                    "X-Organization-ID": "finance"
                }
            )

            assert response.status_code == 403
            assert "Organization mismatch" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_valid_tenant_access(self, finance_token: str):
        """Test that organization can access their own data"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{TEST_BASE_URL}/api/v1/pipelines",
                headers={
                    "Authorization": f"Bearer {finance_token}",
                    "X-Organization-ID": "finance"
                }
            )

            assert response.status_code == 200
            data = response.json()
            assert data["organization_id"] == "finance"

    @pytest.mark.asyncio
    async def test_invalid_token_rejection(self, expired_token: str):
        """Test that expired tokens are rejected"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{TEST_BASE_URL}/api/v1/pipelines",
                headers={
                    "Authorization": f"Bearer {expired_token}",
                    "X-Organization-ID": "finance"
                }
            )

            assert response.status_code == 401
            assert "Invalid authentication token" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_missing_organization_header(self, finance_token: str):
        """Test behavior when organization header is missing"""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{TEST_BASE_URL}/api/v1/pipelines",
                headers={
                    "Authorization": f"Bearer {finance_token}"
                }
            )

            # Should still work as org_id is extracted from token
            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_resource_quota_enforcement(self, finance_token: str):
        """Test that resource quotas are properly enforced"""
        async with httpx.AsyncClient() as client:
            # Create multiple pipelines to test quota limits
            pipeline_configs = [
                {"name": f"test_pipeline_{i}", "type": "batch"}
                for i in range(10)
            ]

            created_pipelines = []
            for config in pipeline_configs:
                response = await client.post(
                    f"{TEST_BASE_URL}/api/v1/pipelines",
                    json=config,
                    headers={
                        "Authorization": f"Bearer {finance_token}",
                        "X-Organization-ID": "finance"
                    }
                )

                if response.status_code == 201:
                    created_pipelines.append(response.json()["pipeline_id"])
                elif response.status_code == 429:
                    # Quota exceeded - expected behavior
                    assert "quota exceeded" in response.json()["detail"].lower()
                    break

            # Cleanup created pipelines
            for pipeline_id in created_pipelines:
                await client.delete(
                    f"{TEST_BASE_URL}/api/v1/pipelines/{pipeline_id}",
                    headers={
                        "Authorization": f"Bearer {finance_token}",
                        "X-Organization-ID": "finance"
                    }
                )

    @pytest.mark.asyncio
    async def test_concurrent_tenant_operations(self, finance_token: str, retail_token: str):
        """Test concurrent operations from different tenants"""

        async def finance_operation():
            async with httpx.AsyncClient() as client:
                return await client.get(
                    f"{TEST_BASE_URL}/api/v1/pipelines",
                    headers={
                        "Authorization": f"Bearer {finance_token}",
                        "X-Organization-ID": "finance"
                    }
                )

        async def retail_operation():
            async with httpx.AsyncClient() as client:
                return await client.get(
                    f"{TEST_BASE_URL}/api/v1/pipelines",
                    headers={
                        "Authorization": f"Bearer {retail_token}",
                        "X-Organization-ID": "retail"
                    }
                )

        # Execute operations concurrently
        finance_response, retail_response = await asyncio.gather(
            finance_operation(),
            retail_operation()
        )

        # Both should succeed with their respective data
        assert finance_response.status_code == 200
        assert retail_response.status_code == 200
        assert finance_response.json()["organization_id"] == "finance"
        assert retail_response.json()["organization_id"] == "retail"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
