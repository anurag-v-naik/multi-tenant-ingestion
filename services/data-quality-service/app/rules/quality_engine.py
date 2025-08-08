# services/data-quality-service/app/rules/quality_engine.py
import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from databricks import sql as databricks_sql
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext

from ..core.config import settings
from ..models.models import QualityRule

logger = logging.getLogger(__name__)


class QualityRuleEngine:
    """Data Quality Rules Engine for multi-tenant validation"""

    def __init__(self):
        self.rule_processors = {
            "completeness": self._process_completeness_rule,
            "uniqueness": self._process_uniqueness_rule,
            "validity": self._process_validity_rule,
            "consistency": self._process_consistency_rule,
            "accuracy": self._process_accuracy_rule,
            "timeliness": self._process_timeliness_rule,
            "custom_sql": self._process_custom_sql_rule,
            "regex_pattern": self._process_regex_rule,
            "range_check": self._process_range_rule,
            "reference_integrity": self._process_reference_integrity_rule
        }

    async def execute_quality_checks(
            self,
            organization_id: str,
            dataset_name: str,
            table_name: str,
            rules: List[QualityRule]
    ) -> Dict[str, Any]:
        """Execute quality checks for a dataset"""
        try:
            # Get data connection for organization
            connection_info = await self._get_organization_connection(organization_id)

            # Load dataset
            df = await self._load_dataset(connection_info, dataset_name, table_name)

            if df is None or df.empty:
                return {
                    "success": False,
                    "error": "Dataset is empty or could not be loaded",
                    "passed_count": 0,
                    "failed_count": 0,
                    "results": []
                }

            results = []
            passed_count = 0
            failed_count = 0

            # Execute each rule
            for rule in rules:
                try:
                    result = await self._execute_single_rule(df, rule, connection_info)
                    results.append(result)

                    if result.get("passed", False):
                        passed_count += 1
                    else:
                        failed_count += 1

                except Exception as e:
                    logger.error(f"Failed to execute rule {rule.id}: {e}")
                    failed_count += 1
                    results.append({
                        "rule_id": rule.id,
                        "rule_name": rule.name,
                        "passed": False,
                        "error": str(e),
                        "executed_at": datetime.utcnow().isoformat()
                    })

            return {
                "success": True,
                "passed_count": passed_count,
                "failed_count": failed_count,
                "total_records": len(df),
                "results": results,
                "execution_summary": {
                    "dataset_name": dataset_name,
                    "table_name": table_name,
                    "organization_id": organization_id,
                    "executed_at": datetime.utcnow().isoformat()
                }
            }

        except Exception as e:
            logger.error(f"Quality check execution failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "passed_count": 0,
                "failed_count": len(rules),
                "results": []
            }

    async def _execute_single_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a single quality rule"""
        rule_processor = self.rule_processors.get(rule.rule_type)

        if not rule_processor:
            raise ValueError(f"Unknown rule type: {rule.rule_type}")

        try:
            result = await rule_processor(df, rule, connection_info)
            result.update({
                "rule_id": rule.id,
                "rule_name": rule.name,
                "rule_type": rule.rule_type,
                "severity": rule.severity,
                "executed_at": datetime.utcnow().isoformat()
            })
            return result

        except Exception as e:
            logger.error(f"Rule execution failed for rule {rule.id}: {e}")
            raise

    async def _process_completeness_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process completeness (null check) rule"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name
        threshold = rule_def.get("threshold", 0.95)  # 95% completeness by default

        if column_name not in df.columns:
            return {
                "passed": False,
                "error": f"Column {column_name} not found in dataset",
                "details": {}
            }

        try:
            # Convert column to datetime
            datetime_col = pd.to_datetime(df[column_name], errors='coerce')
            current_time = datetime.utcnow()

            # Calculate age in hours
            age_hours = (current_time - datetime_col).dt.total_seconds() / 3600

            # Check timeliness
            timely_mask = age_hours <= max_age_hours
            timely_count = timely_mask.sum()
            total_records = len(df)
            timeliness_ratio = timely_count / total_records if total_records > 0 else 0

            passed = timeliness_ratio >= rule_def.get("threshold", 0.95)

            return {
                "passed": passed,
                "timeliness_ratio": timeliness_ratio,
                "details": {
                    "total_records": total_records,
                    "timely_count": int(timely_count),
                    "outdated_count": int(total_records - timely_count),
                    "max_age_hours": max_age_hours,
                    "timeliness_percentage": round(timeliness_ratio * 100, 2)
                }
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Timeliness check failed: {str(e)}",
                "details": {}
            }

    async def _process_reference_integrity_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process referential integrity rule"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name
        reference_table = rule_def.get("reference_table")
        reference_column = rule_def.get("reference_column")

        if not reference_table or not reference_column:
            return {
                "passed": False,
                "error": "Reference table or column not specified",
                "details": {}
            }

        try:
            # Load reference data
            engine = await self._get_database_engine(connection_info)
            reference_query = f"SELECT DISTINCT {reference_column} FROM {reference_table}"

            with engine.connect() as conn:
                reference_df = pd.read_sql(reference_query, conn)

            reference_values = set(reference_df[reference_column].dropna())

            # Check referential integrity
            valid_mask = df[column_name].isin(reference_values) | df[column_name].isna()
            valid_count = valid_mask.sum()
            total_records = len(df)
            integrity_ratio = valid_count / total_records if total_records > 0 else 0

            passed = integrity_ratio >= rule_def.get("threshold", 0.95)

            return {
                "passed": passed,
                "integrity_ratio": integrity_ratio,
                "details": {
                    "total_records": total_records,
                    "valid_references": int(valid_count),
                    "invalid_references": int(total_records - valid_count),
                    "reference_table": reference_table,
                    "reference_column": reference_column,
                    "integrity_percentage": round(integrity_ratio * 100, 2)
                }
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Reference integrity check failed: {str(e)}",
                "details": {}
            }

    # Helper methods
    async def _get_organization_connection(self, organization_id: str) -> Dict[str, Any]:
        """Get database connection info for organization"""
        # This would typically fetch from a configuration service or database
        # For now, returning Databricks connection template
        return {
            "type": "databricks",
            "server_hostname": f"{organization_id}.cloud.databricks.com",
            "http_path": f"/sql/1.0/warehouses/{organization_id}-warehouse",
            "access_token": f"databricks_token_for_{organization_id}",
            "catalog": f"{organization_id}_catalog",
            "schema": "default"
        }

    async def _load_dataset(
            self,
            connection_info: Dict[str, Any],
            dataset_name: str,
            table_name: str
    ) -> pd.DataFrame:
        """Load dataset from organization's data source"""
        try:
            if connection_info["type"] == "databricks":
                return await self._load_from_databricks(connection_info, dataset_name, table_name)
            else:
                engine = await self._get_database_engine(connection_info)
                query = f"SELECT * FROM {dataset_name}.{table_name} LIMIT 10000"  # Limit for performance
                return pd.read_sql(query, engine)

        except Exception as e:
            logger.error(f"Failed to load dataset {dataset_name}.{table_name}: {e}")
            return None

    async def _load_from_databricks(
            self,
            connection_info: Dict[str, Any],
            dataset_name: str,
            table_name: str
    ) -> pd.DataFrame:
        """Load data from Databricks"""
        try:
            with databricks_sql.connect(
                    server_hostname=connection_info["server_hostname"],
                    http_path=connection_info["http_path"],
                    access_token=connection_info["access_token"]
            ) as connection:
                with connection.cursor() as cursor:
                    query = f"""
                    SELECT * FROM {connection_info['catalog']}.{dataset_name}.{table_name} 
                    LIMIT 10000
                    """
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    data = cursor.fetchall()
                    return pd.DataFrame(data, columns=columns)

        except Exception as e:
            logger.error(f"Failed to load from Databricks: {e}")
            raise

    async def _get_database_engine(self, connection_info: Dict[str, Any]):
        """Get SQLAlchemy engine for database connection"""
        if connection_info["type"] == "postgresql":
            connection_string = f"postgresql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
        elif connection_info["type"] == "mysql":
            connection_string = f"mysql://{connection_info['user']}:{connection_info['password']}@{connection_info['host']}:{connection_info['port']}/{connection_info['database']}"
        else:
            raise ValueError(f"Unsupported database type: {connection_info['type']}")

        return create_engine(connection_string)

    def _evaluate_sql_result(self, query_result, expected_result):
        """Evaluate SQL query result against expected outcome"""
        if expected_result == "pass":
            return len(query_result) == 0  # No rows means pass
        elif expected_result == "fail":
            return len(query_result) > 0  # Any rows means fail
        elif isinstance(expected_result, (int, float)):
            return len(query_result) == expected_result
        else:
            return str(query_result) == str(expected_result)

    async def _check_cross_column_consistency(self, df: pd.DataFrame, rule_def: Dict, threshold: float):
        """Check consistency between columns"""
        column1 = rule_def.get("column1")
        column2 = rule_def.get("column2")
        consistency_rule = rule_def.get("rule")  # e.g., "column1 > column2"

        if not all([column1, column2, consistency_rule]):
            return {
                "passed": False,
                "error": "Missing column names or consistency rule",
                "details": {}
            }

        try:
            # Evaluate consistency rule
            consistent_mask = eval(
                consistency_rule.replace("column1", f"df['{column1}']").replace("column2", f"df['{column2}']"))
            consistent_count = consistent_mask.sum()
            total_records = len(df)
            consistency_ratio = consistent_count / total_records if total_records > 0 else 0

            passed = consistency_ratio >= threshold

            return {
                "passed": passed,
                "consistency_ratio": consistency_ratio,
                "details": {
                    "total_records": total_records,
                    "consistent_count": int(consistent_count),
                    "inconsistent_count": int(total_records - consistent_count),
                    "consistency_rule": consistency_rule,
                    "consistency_percentage": round(consistency_ratio * 100, 2)
                }
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Cross-column consistency check failed: {str(e)}",
                "details": {}
            }

        total_records = len(df)
        null_count = df[column_name].isnull().sum()
        non_null_count = total_records - null_count
        completeness_ratio = non_null_count / total_records if total_records > 0 else 0

        passed = completeness_ratio >= threshold

        return {
            "passed": passed,
            "completeness_ratio": completeness_ratio,
            "threshold": threshold,
            "details": {
                "total_records": total_records,
                "null_count": int(null_count),
                "non_null_count": int(non_null_count),
                "completeness_percentage": round(completeness_ratio * 100, 2)
            }
        }

    async def _process_uniqueness_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process uniqueness rule"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name

        if column_name not in df.columns:
            return {
                "passed": False,
                "error": f"Column {column_name} not found in dataset",
                "details": {}
            }

        total_records = len(df)
        unique_count = df[column_name].nunique()
        duplicate_count = total_records - unique_count

        # For uniqueness, we expect 100% unique values
        passed = duplicate_count == 0

        return {
            "passed": passed,
            "uniqueness_ratio": unique_count / total_records if total_records > 0 else 0,
            "details": {
                "total_records": total_records,
                "unique_count": int(unique_count),
                "duplicate_count": int(duplicate_count),
                "uniqueness_percentage": round((unique_count / total_records * 100), 2) if total_records > 0 else 0
            }
        }

    async def _process_validity_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process validity rule (data type validation)"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name
        expected_type = rule_def.get("expected_type", "string")
        threshold = rule_def.get("threshold", 0.95)

        if column_name not in df.columns:
            return {
                "passed": False,
                "error": f"Column {column_name} not found in dataset",
                "details": {}
            }

        total_records = len(df)
        valid_count = 0

        # Type validation logic
        if expected_type == "integer":
            valid_count = df[column_name].apply(
                lambda x: isinstance(x, (int, np.integer)) or (pd.isna(x) and rule_def.get("allow_null", False))).sum()
        elif expected_type == "float":
            valid_count = df[column_name].apply(lambda x: isinstance(x, (float, int, np.number)) or (
                        pd.isna(x) and rule_def.get("allow_null", False))).sum()
        elif expected_type == "string":
            valid_count = df[column_name].apply(
                lambda x: isinstance(x, str) or (pd.isna(x) and rule_def.get("allow_null", False))).sum()
        elif expected_type == "datetime":
            valid_count = pd.to_datetime(df[column_name], errors='coerce').notna().sum()
        elif expected_type == "email":
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            valid_count = df[column_name].str.match(email_pattern, na=False).sum()

        validity_ratio = valid_count / total_records if total_records > 0 else 0
        passed = validity_ratio >= threshold

        return {
            "passed": passed,
            "validity_ratio": validity_ratio,
            "threshold": threshold,
            "details": {
                "total_records": total_records,
                "valid_count": int(valid_count),
                "invalid_count": int(total_records - valid_count),
                "validity_percentage": round(validity_ratio * 100, 2),
                "expected_type": expected_type
            }
        }

    async def _process_range_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process range validation rule"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name
        min_value = rule_def.get("min_value")
        max_value = rule_def.get("max_value")
        threshold = rule_def.get("threshold", 0.95)

        if column_name not in df.columns:
            return {
                "passed": False,
                "error": f"Column {column_name} not found in dataset",
                "details": {}
            }

        total_records = len(df)
        valid_mask = pd.Series([True] * total_records)

        if min_value is not None:
            valid_mask &= (df[column_name] >= min_value) | df[column_name].isna()

        if max_value is not None:
            valid_mask &= (df[column_name] <= max_value) | df[column_name].isna()

        valid_count = valid_mask.sum()
        validity_ratio = valid_count / total_records if total_records > 0 else 0
        passed = validity_ratio >= threshold

        return {
            "passed": passed,
            "validity_ratio": validity_ratio,
            "threshold": threshold,
            "details": {
                "total_records": total_records,
                "valid_count": int(valid_count),
                "invalid_count": int(total_records - valid_count),
                "validity_percentage": round(validity_ratio * 100, 2),
                "min_value": min_value,
                "max_value": max_value
            }
        }

    async def _process_regex_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process regex pattern validation rule"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name
        pattern = rule_def.get("pattern")
        threshold = rule_def.get("threshold", 0.95)

        if not pattern:
            return {
                "passed": False,
                "error": "Regex pattern not specified in rule definition",
                "details": {}
            }

        if column_name not in df.columns:
            return {
                "passed": False,
                "error": f"Column {column_name} not found in dataset",
                "details": {}
            }

        total_records = len(df)
        valid_count = df[column_name].str.match(pattern, na=False).sum()
        validity_ratio = valid_count / total_records if total_records > 0 else 0
        passed = validity_ratio >= threshold

        return {
            "passed": passed,
            "validity_ratio": validity_ratio,
            "threshold": threshold,
            "details": {
                "total_records": total_records,
                "valid_count": int(valid_count),
                "invalid_count": int(total_records - valid_count),
                "validity_percentage": round(validity_ratio * 100, 2),
                "pattern": pattern
            }
        }

    async def _process_custom_sql_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process custom SQL rule"""
        rule_def = json.loads(rule.rule_definition)
        sql_query = rule_def.get("sql_query")
        expected_result = rule_def.get("expected_result", "pass")

        if not sql_query:
            return {
                "passed": False,
                "error": "SQL query not specified in rule definition",
                "details": {}
            }

        try:
            # Execute custom SQL query using organization's connection
            engine = await self._get_database_engine(connection_info)

            with engine.connect() as conn:
                result = conn.execute(text(sql_query))
                query_result = result.fetchall()

            # Evaluate result based on expected outcome
            passed = self._evaluate_sql_result(query_result, expected_result)

            return {
                "passed": passed,
                "details": {
                    "sql_query": sql_query,
                    "query_result": str(query_result),
                    "expected_result": expected_result
                }
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"SQL execution failed: {str(e)}",
                "details": {
                    "sql_query": sql_query
                }
            }

    async def _process_consistency_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process consistency rule (cross-column validation)"""
        rule_def = json.loads(rule.rule_definition)
        consistency_type = rule_def.get("consistency_type", "cross_column")
        threshold = rule_def.get("threshold", 0.95)

        if consistency_type == "cross_column":
            return await self._check_cross_column_consistency(df, rule_def, threshold)
        elif consistency_type == "referential":
            return await self._check_referential_consistency(df, rule_def, connection_info, threshold)
        else:
            return {
                "passed": False,
                "error": f"Unknown consistency type: {consistency_type}",
                "details": {}
            }

    async def _process_accuracy_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process accuracy rule (business logic validation)"""
        rule_def = json.loads(rule.rule_definition)
        accuracy_type = rule_def.get("accuracy_type", "business_rule")

        if accuracy_type == "business_rule":
            return await self._check_business_rule_accuracy(df, rule_def)
        elif accuracy_type == "statistical":
            return await self._check_statistical_accuracy(df, rule_def)
        else:
            return {
                "passed": False,
                "error": f"Unknown accuracy type: {accuracy_type}",
                "details": {}
            }

    async def _process_timeliness_rule(
            self,
            df: pd.DataFrame,
            rule: QualityRule,
            connection_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process timeliness rule"""
        rule_def = json.loads(rule.rule_definition)
        column_name = rule.column_name
        max_age_hours = rule_def.get("max_age_hours", 24)

        if column_name not in df.columns:
            return {
                "passed": False,
                "error": f"Column {column_name} not found in dataset",