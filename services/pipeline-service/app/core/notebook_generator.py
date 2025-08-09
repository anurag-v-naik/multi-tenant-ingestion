from typing import Dict, Any
from ..models.pipeline import Pipeline, PipelineVersion
from ..models.connection import DataConnection


class NotebookGenerator:
    """Generates Databricks notebooks for pipeline execution"""

    def __init__(self):
        self.template = self._get_base_template()

    def _get_base_template(self) -> str:
        """Get base notebook template"""
        return '''
# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Tenant Pipeline Execution
# MAGIC 
# MAGIC This notebook was auto-generated for pipeline execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import sys

# Get parameters
execution_id = dbutils.widgets.get("execution_id")
pipeline_id = dbutils.widgets.get("pipeline_id")
tenant_id = dbutils.widgets.get("tenant_id")

print(f"Execution ID: {execution_id}")
print(f"Pipeline ID: {pipeline_id}")
print(f"Tenant ID: {tenant_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source Connection Configuration

# COMMAND ----------

{source_config}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Connection Configuration

# COMMAND ----------

{target_config}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Extraction

# COMMAND ----------

{extract_code}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------

{transform_code}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------

{load_code}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metrics and Logging

# COMMAND ----------

{metrics_code}
'''

    async def generate_notebook(
            self,
            pipeline: Pipeline,
            version: PipelineVersion,
            source_connection: DataConnection,
            target_connection: DataConnection,
            parameters: Dict[str, Any]
    ) -> str:
        """Generate complete notebook for pipeline execution"""

        # Generate source configuration
        source_config = self._generate_source_config(source_connection)

        # Generate target configuration
        target_config = self._generate_target_config(target_connection)

        # Generate extraction code
        extract_code = self._generate_extract_code(source_connection, parameters)

        # Generate transformation code
        transform_code = self._generate_transform_code(
            pipeline.transformation_config,
            parameters
        )

        # Generate loading code
        load_code = self._generate_load_code(target_connection, parameters)

        # Generate metrics code
        metrics_code = self._generate_metrics_code()

        # Fill template
        notebook_content = self.template.format(
            source_config=source_config,
            target_config=target_config,
            extract_code=extract_code,
            transform_code=transform_code,
            load_code=load_code,
            metrics_code=metrics_code
        )

        return notebook_content

    def _generate_source_config(self, connection: DataConnection) -> str:
        """Generate source connection configuration code"""

        if connection.connection_type == "postgresql":
            return f'''
# PostgreSQL Source Configuration
source_config = {{
    "url": "jdbc:postgresql://{connection.config['host']}:{connection.config['port']}/{connection.config['database']}",
    "user": "{connection.credentials['username']}",
    "password": "{connection.credentials['password']}",
    "driver": "org.postgresql.Driver"
}}

print("Source: PostgreSQL configured")
'''

        elif connection.connection_type == "s3":
            return f'''
# S3 Source Configuration
source_config = {{
    "bucket": "{connection.config['bucket_name']}",
    "prefix": "{connection.config.get('prefix', '')}",
    "format": "parquet"  # or csv, json based on file
}}

# Configure S3 credentials
spark.conf.set("fs.s3a.access.key", "{connection.credentials['access_key_id']}")
spark.conf.set("fs.s3a.secret.key", "{connection.credentials['secret_access_key']}")

print("Source: S3 configured")
'''

        else:
            return f"# Source configuration for {connection.connection_type} not implemented"

    def _generate_target_config(self, connection: DataConnection) -> str:
        """Generate target connection configuration code"""

        if connection.connection_type == "postgresql":
            return f'''
# PostgreSQL Target Configuration
target_config = {{
    "url": "jdbc:postgresql://{connection.config['host']}:{connection.config['port']}/{connection.config['database']}",
    "user": "{connection.credentials['username']}",
    "password": "{connection.credentials['password']}",
    "driver": "org.postgresql.Driver"
}}

print("Target: PostgreSQL configured")
'''

        elif connection.connection_type == "s3":
            return f'''
# S3 Target Configuration
target_config = {{
    "bucket": "{connection.config['bucket_name']}",
    "prefix": "{connection.config.get('prefix', '')}",
    "format": "parquet"
}}

print("Target: S3 configured")
'''

        else:
            return f"# Target configuration for {connection.connection_type} not implemented"

    def _generate_extract_code(self, connection: DataConnection, parameters: Dict[str, Any]) -> str:
        """Generate data extraction code"""

        if connection.connection_type == "postgresql":
            table_name = parameters.get("source_table", "default_table")
            return f'''
# Extract data from PostgreSQL
try:
    source_df = spark.read \\
        .format("jdbc") \\
        .option("url", source_config["url"]) \\
        .option("dbtable", "{table_name}") \\
        .option("user", source_config["user"]) \\
        .option("password", source_config["password"]) \\
        .option("driver", source_config["driver"]) \\
        .load()

    print(f"Extracted {{source_df.count()}} records from {table_name}")
    source_df.printSchema()

except Exception as e:
    print(f"Error extracting data: {{str(e)}}")
    raise e
'''

        elif connection.connection_type == "s3":
            return f'''
# Extract data from S3
try:
    s3_path = f"s3a://{{source_config['bucket']}}/{{source_config['prefix']}}/*"

    source_df = spark.read \\
        .option("header", "true") \\
        .option("inferSchema", "true") \\
        .format(source_config["format"]) \\
        .load(s3_path)

    print(f"Extracted {{source_df.count()}} records from S3")
    source_df.printSchema()

except Exception as e:
    print(f"Error extracting data: {{str(e)}}")
    raise e
'''

        else:
            return f"# Extract code for {connection.connection_type} not implemented"

    def _generate_transform_code(self, transform_config: Dict[str, Any], parameters: Dict[str, Any]) -> str:
        """Generate data transformation code"""

        transforms = transform_config.get("transforms", [])

        if not transforms:
            return '''
# No transformations specified - pass through
transformed_df = source_df
print("No transformations applied")
'''

        code = "# Apply transformations\ntransformed_df = source_df\n\n"

        for transform in transforms:
            transform_type = transform.get("type")

            if transform_type == "filter":
                condition = transform.get("condition")
                code += f'''
# Apply filter: {condition}
transformed_df = transformed_df.filter("{condition}")
print(f"After filter: {{transformed_df.count()}} records")

'''

            elif transform_type == "select":
                columns = transform.get("columns", [])
                columns_str = ", ".join([f'"{col}"' for col in columns])
                code += f'''
# Select columns: {columns}
transformed_df = transformed_df.select({columns_str})
print(f"Selected columns: {columns}")

'''

            elif transform_type == "rename":
                mappings = transform.get("mappings", {})
                for old_name, new_name in mappings.items():
                    code += f'''
# Rename column: {old_name} -> {new_name}
transformed_df = transformed_df.withColumnRenamed("{old_name}", "{new_name}")

'''

            elif transform_type == "add_column":
                column_name = transform.get("column_name")
                expression = transform.get("expression")
                code += f'''
# Add column: {column_name}
transformed_df = transformed_df.withColumn("{column_name}", expr("{expression}"))

'''

        code += '''
print(f"Transformation complete: {transformed_df.count()} records")
transformed_df.printSchema()
'''

        return code

    def _generate_load_code(self, connection: DataConnection, parameters: Dict[str, Any]) -> str:
        """Generate data loading code"""

        if connection.connection_type == "postgresql":
            table_name = parameters.get("target_table", "default_table")
            return f'''
# Load data to PostgreSQL
try:
    transformed_df.write \\
        .format("jdbc") \\
        .option("url", target_config["url"]) \\
        .option("dbtable", "{table_name}") \\
        .option("user", target_config["user"]) \\
        .option("password", target_config["password"]) \\
        .option("driver", target_config["driver"]) \\
        .mode("overwrite") \\
        .save()

    print(f"Data loaded to {table_name}")

except Exception as e:
    print(f"Error loading data: {{str(e)}}")
    raise e
'''

        elif connection.connection_type == "s3":
            return f'''
# Load data to S3
try:
    output_path = f"s3a://{{target_config['bucket']}}/{{target_config['prefix']}}/{{execution_id}}"

    transformed_df.write \\
        .format(target_config["format"]) \\
        .mode("overwrite") \\
        .save(output_path)

    print(f"Data loaded to {{output_path}}")

except Exception as e:
    print(f"Error loading data: {{str(e)}}")
    raise e
'''

        else:
            return f"# Load code for {connection.connection_type} not implemented"

    def _generate_metrics_code(self) -> str:
        """Generate metrics collection code"""

        return '''
# Collect execution metrics
try:
    metrics = {
        "execution_id": execution_id,
        "pipeline_id": pipeline_id,
        "tenant_id": tenant_id,
        "records_processed": transformed_df.count(),
        "records_failed": 0,  # Would be calculated based on data quality checks
        "execution_time": datetime.now().isoformat(),
        "status": "success"
    }

    print("=== EXECUTION METRICS ===")
    for key, value in metrics.items():
        print(f"{key}: {value}")

    # In production, these metrics would be sent back to the pipeline service
    # dbutils.notebook.exit(json.dumps(metrics))

except Exception as e:
    print(f"Error collecting metrics: {str(e)}")
    metrics = {
        "execution_id": execution_id,
        "status": "failed",
        "error": str(e)
    }

print("Pipeline execution completed")
'''
