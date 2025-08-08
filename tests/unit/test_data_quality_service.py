# tests/unit/test_data_quality_service.py
import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
import pandas as pd
from datetime import datetime
import json

from services.data_quality_service.app.rules.quality_engine import QualityRuleEngine
from services.data_quality_service.app.models.models import QualityRule


class TestQualityRuleEngine:
    """Unit tests for QualityRuleEngine"""

    @pytest.fixture
    def performance_rules(self):
        """Create multiple rules for performance testing"""
        return [
            QualityRule(
                id=i,
                organization_id="test-org",
                name=f"Rule {i}",
                rule_type="completeness",
                table_name="large_table",
                column_name="name",
                rule_definition='{"threshold": 0.95}',
                severity="medium",
                is_active=True
            ) for i in range(10)
        ]

    @pytest.mark.asyncio
    async def test_large_dataset_processing_performance(self, quality_engine, large_dataframe, performance_rules):
        """Test performance with large dataset"""

        with patch.object(quality_engine, '_get_organization_connection') as mock_conn:
            with patch.object(quality_engine, '_load_dataset') as mock_load:
                mock_conn.return_value = {"type": "test"}
                mock_load.return_value = large_dataframe

                start_time = time.time()

                result = await quality_engine.execute_quality_checks(
                    "test-org", "large_dataset", "large_table", performance_rules
                )

                execution_time = time.time() - start_time

                # Should complete within reasonable time (adjust threshold as needed)
                assert execution_time < 30.0  # 30 seconds max
                assert result["success"] is True
                assert result["total_records"] == 100000
                assert len(result["results"]) == 10

    @pytest.mark.asyncio
    async def test_concurrent_quality_checks(self, quality_engine):
        """Test concurrent execution of quality checks"""

        async def run_quality_check(org_id, dataset_name):
            with patch.object(quality_engine, '_get_organization_connection') as mock_conn:
                with patch.object(quality_engine, '_load_dataset') as mock_load:
                    mock_conn.return_value = {"type": "test"}
                    mock_load.return_value = pd.DataFrame({
                        'id': range(1000),
                        'value': range(1000)
                    })

                    rule = QualityRule(
                        id=1,
                        organization_id=org_id,
                        name="Concurrent Test Rule",
                        rule_type="completeness",
                        table_name="test_table",
                        column_name="value",
                        rule_definition='{"threshold": 0.95}',
                        severity="medium",
                        is_active=True
                    )

                    return await quality_engine.execute_quality_checks(
                        org_id, dataset_name, "test_table", [rule]
                    )

        # Run multiple quality checks concurrently
        start_time = time.time()

        tasks = [
            run_quality_check(f"org-{i}", f"dataset-{i}")
            for i in range(5)
        ]

        results = await asyncio.gather(*tasks)

        execution_time = time.time() - start_time

        # All checks should succeed
        for result in results:
            assert result["success"] is True

        # Concurrent execution should be faster than sequential
        assert execution_time < 10.0  # Reasonable time for concurrent execution

    def test_memory_usage_large_dataset(self, quality_engine, large_dataframe):
        """Test memory usage with large datasets"""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Process large dataframe multiple times
        for _ in range(5):
            # Simulate data processing
            df_copy = large_dataframe.copy()
            completeness_ratio = (len(df_copy) - df_copy['name'].isnull().sum()) / len(df_copy)
            assert completeness_ratio > 0

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 500MB for this test)
        assert memory_increase < 500


# tests/conftest.py - Shared test configuration
import pytest
import asyncio
from unittest.mock import Mock, patch
import os
import tempfile
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from services.data_quality_service.app.core.database import Base


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def test_database():
    """Create test database for the session"""
    # Create temporary database file
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    test_db_url = f"sqlite:///{db_path}"

    # Create engine and tables
    engine = create_engine(test_db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)

    yield engine

    # Cleanup
    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def mock_auth():
    """Mock authentication for tests"""
    with patch('services.data_quality_service.app.core.auth.get_current_user') as mock_user:
        with patch('services.data_quality_service.app.core.auth.verify_organization_access') as mock_verify:
            mock_user.return_value = {
                "user_id": "test-user-123",
                "username": "testuser",
                "email": "test@example.com"
            }
            mock_verify.return_value = None
            yield mock_user, mock_verify


@pytest.fixture
def mock_databricks():
    """Mock Databricks connection for tests"""
    with patch('services.data_quality_service.app.rules.quality_engine.databricks_sql') as mock_sql:
        mock_connection = Mock()
        mock_cursor = Mock()

        mock_sql.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Default mock data
        mock_cursor.description = [("id",), ("name",), ("value",)]
        mock_cursor.fetchall.return_value = [
            (1, "test1", 100),
            (2, "test2", 200),
            (3, "test3", 300)
        ]

        yield mock_sql


# tests/fixtures/sample_data.py - Test data fixtures
import pandas as pd
import json
from datetime import datetime, timedelta


def create_sample_quality_rules():
    """Create sample quality rules for testing"""
    return [
        {
            "name": "Customer Email Completeness",
            "description": "Ensure customer emails are not null",
            "rule_type": "completeness",
            "table_name": "customers",
            "column_name": "email",
            "rule_definition": json.dumps({"threshold": 0.95}),
            "severity": "high"
        },
        {
            "name": "Product ID Uniqueness",
            "description": "Ensure product IDs are unique",
            "rule_type": "uniqueness",
            "table_name": "products",
            "column_name": "product_id",
            "rule_definition": json.dumps({}),
            "severity": "critical"
        },
        {
            "name": "Order Amount Range",
            "description": "Validate order amounts are within expected range",
            "rule_type": "range_check",
            "table_name": "orders",
            "column_name": "amount",
            "rule_definition": json.dumps({"min_value": 0, "max_value": 10000, "threshold": 0.99}),
            "severity": "medium"
        }
    ]


def create_sample_datasets():
    """Create sample datasets for testing"""

    # Customer data with quality issues
    customers_df = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown', None, 'Eve Davis',
                 'Frank Miller', 'Grace Lee', 'Henry Wilson', 'Ivy Chen', 'Jack Taylor'],
        'email': ['alice@email.com', 'bob@email.com', None, 'david@email.com', 'eve@email.com',
                  'frank@email.com', None, 'henry@email.com', 'ivy@email.com', 'jack@email.com'],
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        'created_at': [datetime.now() - timedelta(days=i) for i in range(10)]
    })

    # Product data
    products_df = pd.DataFrame({
        'product_id': [101, 102, 103, 104, 105],
        'name': ['Widget A', 'Widget B', 'Widget C', 'Widget D', 'Widget E'],
        'price': [10.99, 25.50, 45.00, 12.75, 33.25],
        'category': ['Electronics', 'Home', 'Electronics', 'Books', 'Home']
    })

    # Order data with some outliers
    orders_df = pd.DataFrame({
        'order_id': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008],
        'customer_id': [1, 2, 3, 4, 5, 1, 2, 3],
        'product_id': [101, 102, 103, 104, 105, 101, 102, 103],
        'amount': [10.99, 25.50, 45.00, 12.75, 15000, 10.99, 25.50, 45.00],  # One outlier
        'order_date': [datetime.now() - timedelta(days=i) for i in range(8)]
    })

    return {
        'customers': customers_df,
        'products': products_df,
        'orders': orders_df
    }


# tests/utils/test_helpers.py - Test utility functions
import json
import tempfile
import os
from typing import Dict, Any


def create_temp_config_file(config_data: Dict[str, Any]) -> str:
    """Create a temporary configuration file for testing"""
    fd, path = tempfile.mkstemp(suffix='.json')

    with os.fdopen(fd, 'w') as f:
        json.dump(config_data, f, indent=2)

    return path


def cleanup_temp_file(file_path: str):
    """Clean up temporary file"""
    if os.path.exists(file_path):
        os.unlink(file_path)


def assert_quality_check_result(result: Dict[str, Any], expected_passed: bool, expected_rule_count: int):
    """Assert quality check result structure and values"""
    assert "success" in result
    assert "passed_count" in result
    assert "failed_count" in result
    assert "results" in result

    assert result["success"] is True
    assert len(result["results"]) == expected_rule_count

    if expected_passed:
        assert result["passed_count"] > 0
    else:
        assert result["failed_count"] > 0


def create_mock_databricks_response(columns: list, data: list):
    """Create mock Databricks query response"""
    return {
        "description": [(col,) for col in columns],
        "data": data
    }


# tests/integration/test_end_to_end.py - End-to-end integration tests
import pytest
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
import json

from services.data_quality_service.app.main import app
from tests.fixtures.sample_data import create_sample_quality_rules, create_sample_datasets


class TestEndToEndIntegration:
    """End-to-end integration tests for the data quality service"""

    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def auth_headers(self):
        return {
            "X-Organization-ID": "test-org",
            "Authorization": "Bearer test-token"
        }

    @pytest.mark.asyncio
    async def test_complete_quality_workflow(self, client, auth_headers, mock_auth, mock_databricks):
        """Test complete workflow from rule creation to quality check execution"""

        # Step 1: Create quality rules
        sample_rules = create_sample_quality_rules()
        created_rule_ids = []

        for rule_data in sample_rules:
            response = client.post("/api/v1/quality-rules", json=rule_data, headers=auth_headers)
            assert response.status_code == 200
            created_rule_ids.append(response.json()["id"])

        # Step 2: List quality rules
        response = client.get("/api/v1/quality-rules", headers=auth_headers)
        assert response.status_code == 200
        rules = response.json()
        assert len(rules) >= len(sample_rules)

        # Step 3: Run quality check
        check_request = {
            "check_name": "Integration Test Check",
            "dataset_name": "test_dataset",
            "table_name": "customers",
            "rule_ids": created_rule_ids[:1]  # Use first rule only
        }

        with patch('services.data_quality_service.app.main.run_quality_check_async') as mock_async_check:
            response = client.post("/api/v1/quality-checks", json=check_request, headers=auth_headers)
            assert response.status_code == 200
            check_id = response.json()["id"]
            assert response.json()["status"] == "RUNNING"

        # Step 4: Get quality check status
        response = client.get(f"/api/v1/quality-checks/{check_id}", headers=auth_headers)
        assert response.status_code == 200
        check_details = response.json()
        assert check_details["id"] == check_id
        assert check_details["organization_id"] == "test-org"

    @pytest.mark.asyncio
    async def test_multi_organization_isolation(self, client, mock_auth):
        """Test that organizations are properly isolated"""

        org1_headers = {"X-Organization-ID": "org1", "Authorization": "Bearer token1"}
        org2_headers = {"X-Organization-ID": "org2", "Authorization": "Bearer token2"}

        # Create rule for org1
        rule_data = {
            "name": "Org1 Rule",
            "description": "Test rule for org1",
            "rule_type": "completeness",
            "table_name": "data",
            "column_name": "field1",
            "rule_definition": '{"threshold": 0.95}',
            "severity": "high",
            "is_active": True
        }

        response = client.post("/api/v1/quality-rules", json=rule_data, headers=org1_headers)
        assert response.status_code == 200
        org1_rule_id = response.json()["id"]

        # Try to access org1 rule from org2
        response = client.get("/api/v1/quality-rules", headers=org2_headers)
        assert response.status_code == 200
        org2_rules = response.json()

        # Org2 should not see org1's rules
        org2_rule_ids = [rule["id"] for rule in org2_rules]
        assert org1_rule_id not in org2_rule_ids

    def test_api_error_handling(self, client, auth_headers, mock_auth):
        """Test API error handling"""

        # Test invalid rule creation
        invalid_rule = {
            "name": "",  # Empty name should fail validation
            "rule_type": "invalid_type",
            "table_name": "test_table"
        }

        response = client.post("/api/v1/quality-rules", json=invalid_rule, headers=auth_headers)
        assert response.status_code in [400, 422]  # Validation error

        # Test accessing non-existent quality check
        response = client.get("/api/v1/quality-checks/99999", headers=auth_headers)
        assert response.status_code == 404

        # Test missing organization header
        headers_without_org = {"Authorization": "Bearer test-token"}
        response = client.get("/api/v1/quality-rules", headers=headers_without_org)
        assert response.status_code == 422  # Missing required header quality_engine(self):
        return QualityRuleEngine()

    @pytest.fixture
    def sample_dataframe(self):
        return pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'email': ['alice@test.com', 'bob@test.com', 'invalid-email', 'david@test.com', 'eve@test.com'],
            'salary': [50000, 60000, 70000, 80000, 90000],
            'created_at': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
        })

    @pytest.fixture
    def completeness_rule(self):
        return QualityRule(
            id=1,
            organization_id="test-org",
            name="Name Completeness",
            rule_type="completeness",
            table_name="users",
            column_name="name",
            rule_definition='{"threshold": 0.8}',
            severity="high",
            is_active=True
        )

    @pytest.fixture
    def uniqueness_rule(self):
        return QualityRule(
            id=2,
            organization_id="test-org",
            name="ID Uniqueness",
            rule_type="uniqueness",
            table_name="users",
            column_name="id",
            rule_definition='{}',
            severity="critical",
            is_active=True
        )

    @pytest.fixture
    def validity_rule(self):
        return QualityRule(
            id=3,
            organization_id="test-org",
            name="Email Validity",
            rule_type="validity",
            table_name="users",
            column_name="email",
            rule_definition='{"expected_type": "email", "threshold": 0.8}',
            severity="medium",
            is_active=True
        )

    @pytest.mark.asyncio
    async def test_completeness_rule_pass(self, quality_engine, sample_dataframe, completeness_rule):
        """Test completeness rule that should pass"""
        connection_info = {"type": "test"}

        result = await quality_engine._process_completeness_rule(
            sample_dataframe, completeness_rule, connection_info
        )

        assert result["passed"] is True
        assert result["completeness_ratio"] == 0.8  # 4 out of 5 non-null
        assert result["details"]["total_records"] == 5
        assert result["details"]["null_count"] == 1
        assert result["details"]["non_null_count"] == 4

    @pytest.mark.asyncio
    async def test_completeness_rule_fail(self, quality_engine, sample_dataframe, completeness_rule):
        """Test completeness rule that should fail"""
        # Modify rule to require 90% completeness
        completeness_rule.rule_definition = '{"threshold": 0.9}'
        connection_info = {"type": "test"}

        result = await quality_engine._process_completeness_rule(
            sample_dataframe, completeness_rule, connection_info
        )

        assert result["passed"] is False
        assert result["completeness_ratio"] == 0.8
        assert result["threshold"] == 0.9

    @pytest.mark.asyncio
    async def test_uniqueness_rule_pass(self, quality_engine, sample_dataframe, uniqueness_rule):
        """Test uniqueness rule that should pass"""
        connection_info = {"type": "test"}

        result = await quality_engine._process_uniqueness_rule(
            sample_dataframe, uniqueness_rule, connection_info
        )

        assert result["passed"] is True
        assert result["details"]["unique_count"] == 5
        assert result["details"]["duplicate_count"] == 0

    @pytest.mark.asyncio
    async def test_uniqueness_rule_fail(self, quality_engine, uniqueness_rule):
        """Test uniqueness rule that should fail"""
        # Create dataframe with duplicates
        df_with_duplicates = pd.DataFrame({
            'id': [1, 2, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Bob2', 'Charlie', 'David']
        })
        connection_info = {"type": "test"}

        result = await quality_engine._process_uniqueness_rule(
            df_with_duplicates, uniqueness_rule, connection_info
        )

        assert result["passed"] is False
        assert result["details"]["unique_count"] == 4  # Only 4 unique IDs
        assert result["details"]["duplicate_count"] == 1

    @pytest.mark.asyncio
    async def test_validity_rule_email(self, quality_engine, sample_dataframe, validity_rule):
        """Test email validity rule"""
        connection_info = {"type": "test"}

        result = await quality_engine._process_validity_rule(
            sample_dataframe, validity_rule, connection_info
        )

        assert result["passed"] is True  # 4 out of 5 emails are valid (80%)
        assert result["details"]["valid_count"] == 4
        assert result["details"]["invalid_count"] == 1

    @pytest.mark.asyncio
    async def test_column_not_found_error(self, quality_engine, sample_dataframe, completeness_rule):
        """Test error handling when column doesn't exist"""
        completeness_rule.column_name = "non_existent_column"
        connection_info = {"type": "test"}

        result = await quality_engine._process_completeness_rule(
            sample_dataframe, completeness_rule, connection_info
        )

        assert result["passed"] is False
        assert "not found in dataset" in result["error"]


# tests/integration/test_multi_tenant_isolation.py
import pytest
import asyncio
from fastapi.testclient import TestClient
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from services.data_quality_service.app.main import app
from services.data_quality_service.app.core.database import get_db, Base


class TestMultiTenantIsolation:
    """Integration tests for multi-tenant isolation"""

    @pytest.fixture(scope="class")
    def test_db_engine(self):
        """Create test database engine"""
        test_db_url = "sqlite:///./test_multi_tenant.db"
        engine = create_engine(test_db_url, connect_args={"check_same_thread": False})
        Base.metadata.create_all(bind=engine)
        return engine

    @pytest.fixture(scope="class")
    def test_db_session(self, test_db_engine):
        """Create test database session"""
        TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_db_engine)
        return TestingSessionLocal()

    @pytest.fixture
    def client(self, test_db_session):
        """Create test client with dependency override"""

        def override_get_db():
            try:
                yield test_db_session
            finally:
                pass

        app.dependency_overrides[get_db] = override_get_db
        return TestClient(app)

    @pytest.fixture
    def org1_headers(self):
        return {
            "X-Organization-ID": "finance",
            "Authorization": "Bearer test-token-finance"
        }

    @pytest.fixture
    def org2_headers(self):
        return {
            "X-Organization-ID": "retail",
            "Authorization": "Bearer test-token-retail"
        }

    def test_organization_isolation_quality_rules(self, client, org1_headers, org2_headers):
        """Test that organizations cannot access each other's quality rules"""

        # Create quality rule for org1
        rule_data = {
            "name": "Finance Data Quality Rule",
            "description": "Test rule for finance org",
            "rule_type": "completeness",
            "table_name": "transactions",
            "column_name": "amount",
            "rule_definition": '{"threshold": 0.95}',
            "severity": "high",
            "is_active": True
        }

        with patch('services.data_quality_service.app.core.auth.get_current_user') as mock_auth:
            with patch('services.data_quality_service.app.core.auth.verify_organization_access') as mock_verify:
                mock_auth.return_value = {"user_id": "test-user"}
                mock_verify.return_value = None

                # Create rule for finance org
                response = client.post("/api/v1/quality-rules", json=rule_data, headers=org1_headers)
                assert response.status_code == 200
                finance_rule_id = response.json()["id"]

                # Try to access finance rule from retail org
                response = client.get("/api/v1/quality-rules", headers=org2_headers)
                assert response.status_code == 200
                retail_rules = response.json()

                # Retail org should not see finance rules
                rule_ids = [rule["id"] for rule in retail_rules]
                assert finance_rule_id not in rule_ids

    def test_cross_organization_data_access_prevention(self, client, org1_headers, org2_headers):
        """Test that one organization cannot access another's data"""

        check_request = {
            "check_name": "Cross Org Test",
            "dataset_name": "sensitive_data",
            "table_name": "financial_records",
            "rule_ids": []
        }

        with patch('services.data_quality_service.app.core.auth.get_current_user') as mock_auth:
            with patch('services.data_quality_service.app.core.auth.verify_organization_access') as mock_verify:
                mock_auth.return_value = {"user_id": "test-user"}
                mock_verify.return_value = None

                # Finance org creates a quality check
                response = client.post("/api/v1/quality-checks", json=check_request, headers=org1_headers)
                assert response.status_code == 200
                finance_check_id = response.json()["id"]

                # Retail org tries to access finance check
                response = client.get(f"/api/v1/quality-checks/{finance_check_id}", headers=org2_headers)
                assert response.status_code == 404  # Should not be found


# tests/integration/test_databricks_integration.py
import pytest
from unittest.mock import patch, Mock
import pandas as pd

from services.data_quality_service.app.rules.quality_engine import QualityRuleEngine


class TestDatabricksIntegration:
    """Integration tests for Databricks connectivity"""

    @pytest.fixture
    def quality_engine(self):
        return QualityRuleEngine()

    @pytest.fixture
    def databricks_connection_info(self):
        return {
            "type": "databricks",
            "server_hostname": "test-org.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/test-warehouse",
            "access_token": "test-token",
            "catalog": "test_catalog",
            "schema": "default"
        }

    @pytest.mark.asyncio
    @patch('services.data_quality_service.app.rules.quality_engine.databricks_sql')
    async def test_databricks_connection(self, mock_databricks_sql, quality_engine, databricks_connection_info):
        """Test Databricks connection and data loading"""

        # Mock Databricks connection
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_databricks_sql.connect.return_value.__enter__.return_value = mock_connection
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock query result
        mock_cursor.description = [("id",), ("name",), ("value",)]
        mock_cursor.fetchall.return_value = [
            (1, "test1", 100),
            (2, "test2", 200),
            (3, "test3", 300)
        ]

        # Load data from Databricks
        result_df = await quality_engine._load_from_databricks(
            databricks_connection_info, "test_dataset", "test_table"
        )

        # Verify connection was established correctly
        mock_databricks_sql.connect.assert_called_once_with(
            server_hostname="test-org.cloud.databricks.com",
            http_path="/sql/1.0/warehouses/test-warehouse",
            access_token="test-token"
        )

        # Verify data was loaded correctly
        assert len(result_df) == 3
        assert list(result_df.columns) == ["id", "name", "value"]
        assert result_df.iloc[0]["id"] == 1
        assert result_df.iloc[0]["name"] == "test1"

    @pytest.mark.asyncio
    @patch('services.data_quality_service.app.rules.quality_engine.databricks_sql')
    async def test_databricks_connection_failure(self, mock_databricks_sql, quality_engine, databricks_connection_info):
        """Test handling of Databricks connection failures"""

        # Mock connection failure
        mock_databricks_sql.connect.side_effect = Exception("Connection failed")

        with pytest.raises(Exception) as exc_info:
            await quality_engine._load_from_databricks(
                databricks_connection_info, "test_dataset", "test_table"
            )

        assert "Connection failed" in str(exc_info.value)


# tests/load/test_performance.py
import pytest
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from unittest.mock import patch, Mock

from services.data_quality_service.app.rules.quality_engine import QualityRuleEngine
from services.data_quality_service.app.models.models import QualityRule


class TestPerformance:
    """Load and performance tests"""

    @pytest.fixture
    def quality_engine(self):
        return QualityRuleEngine()

    @pytest.fixture
    def large_dataframe(self):
        """Create a large dataframe for performance testing"""
        import numpy as np

        size = 100000  # 100K records
        return pd.DataFrame({
            'id': range(size),
            'name': [f'user_{i}' for i in range(size)],
            'age': np.random.randint(18, 80, size),
            'email': [f'user_{i}@test.com' for i in range(size)],
            'salary': np.random.randint(30000, 150000, size),
            'created_at': pd.date_range('2020-01-01', periods=size, freq='H')
        })

    @pytest.fixture
    def