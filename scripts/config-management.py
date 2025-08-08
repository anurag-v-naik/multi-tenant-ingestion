# scripts/config-management.py
"""
Configuration management system for multi-tenant data ingestion framework.
Provides centralized configuration handling for all services and deployments.
"""

import os
import sys
import json
import yaml
import argparse
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum
import jinja2
from cryptography.fernet import Fernet
import base64

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Environment(Enum):
    """Supported environments"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"


class ComplianceLevel(Enum):
    """Compliance levels for organizations"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


@dataclass
class DatabaseConfig:
    """Database configuration"""
    host: str = "localhost"
    port: int = 5432
    username: str = "postgres"
    password: str = "postgres"
    database: str = "multi_tenant_ingestion"
    ssl_mode: str = "prefer"
    pool_size: int = 10
    max_overflow: int = 20


@dataclass
class RedisConfig:
    """Redis configuration"""
    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    ssl: bool = False


@dataclass
class DatabricksConfig:
    """Databricks configuration"""
    host: str = ""
    token: str = ""
    cluster_id: str = ""
    workspace_id: str = ""
    unity_catalog_enabled: bool = True


@dataclass
class AWSConfig:
    """AWS configuration"""
    region: str = "us-east-1"
    access_key_id: str = ""
    secret_access_key: str = ""
    s3_bucket_prefix: str = "multi-tenant-data"
    kms_key_id: Optional[str] = None


@dataclass
class KubernetesConfig:
    """Kubernetes configuration"""
    namespace_prefix: str = "multi-tenant"
    storage_class: str = "standard"
    ingress_class: str = "nginx"
    resource_quotas: Dict[str, str] = None

    def __post_init__(self):
        if self.resource_quotas is None:
            self.resource_quotas = {
                "requests.cpu": "2",
                "requests.memory": "4Gi",
                "limits.cpu": "4",
                "limits.memory": "8Gi",
                "persistentvolumeclaims": "10"
            }


@dataclass
class SecurityConfig:
    """Security configuration"""
    jwt_secret_key: str = ""
    encryption_key: str = ""
    password_salt: str = ""
    session_timeout: int = 3600
    max_login_attempts: int = 5
    enable_2fa: bool = False


@dataclass
class MonitoringConfig:
    """Monitoring configuration"""
    prometheus_enabled: bool = True
    grafana_enabled: bool = True
    alertmanager_enabled: bool = True
    log_level: str = "INFO"
    metrics_retention: str = "30d"


@dataclass
class OrganizationConfig:
    """Organization-specific configuration"""
    name: str
    display_name: str
    compliance_level: ComplianceLevel = ComplianceLevel.MEDIUM
    cost_center: str = ""
    contact_email: str = ""
    features: Dict[str, bool] = None
    resource_limits: Dict[str, Any] = None
    custom_settings: Dict[str, Any] = None

    def __post_init__(self):
        if self.features is None:
            self.features = {
                "unity_catalog": True,
                "auto_scaling": True,
                "monitoring": True,
                "data_quality": True,
                "cost_tracking": True
            }

        if self.resource_limits is None:
            self.resource_limits = {
                "max_dbu_per_hour": 100,
                "max_storage_gb": 10000,
                "max_api_calls_per_minute": 1000,
                "max_concurrent_jobs": 10
            }

        if self.custom_settings is None:
            self.custom_settings = {}


@dataclass
class GlobalConfig:
    """Global application configuration"""
    environment: Environment
    project_name: str = "multi-tenant-ingestion"
    version: str = "1.0.0"
    debug: bool = False

    # Service configurations
    database: DatabaseConfig = None
    redis: RedisConfig = None
    databricks: DatabricksConfig = None
    aws: AWSConfig = None
    kubernetes: KubernetesConfig = None
    security: SecurityConfig = None
    monitoring: MonitoringConfig = None

    # Organizations
    organizations: Dict[str, OrganizationConfig] = None

    def __post_init__(self):
        if self.database is None:
            self.database = DatabaseConfig()
        if self.redis is None:
            self.redis = RedisConfig()
        if self.databricks is None:
            self.databricks = DatabricksConfig()
        if self.aws is None:
            self.aws = AWSConfig()
        if self.kubernetes is None:
            self.kubernetes = KubernetesConfig()
        if self.security is None:
            self.security = SecurityConfig()
        if self.monitoring is None:
            self.monitoring = MonitoringConfig()
        if self.organizations is None:
            self.organizations = {}


class ConfigManager:
    """Configuration management system"""

    def __init__(self, config_dir: str = "deployment/configs"):
        self.config_dir = Path(config_dir)
        self.template_dir = Path("templates/configs")
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher = Fernet(self.encryption_key)

        # Create directories if they don't exist
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.template_dir.mkdir(parents=True, exist_ok=True)

    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for sensitive data"""
        key_file = self.config_dir / ".encryption_key"

        if key_file.exists():
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(key_file, 'wb') as f:
                f.write(key)
            return key

    def encrypt_value(self, value: str) -> str:
        """Encrypt sensitive configuration value"""
        encrypted = self.cipher.encrypt(value.encode())
        return base64.b64encode(encrypted).decode()

    def decrypt_value(self, encrypted_value: str) -> str:
        """Decrypt sensitive configuration value"""
        encrypted = base64.b64decode(encrypted_value.encode())
        return self.cipher.decrypt(encrypted).decode()

    def load_config(self, environment: Environment) -> GlobalConfig:
        """Load configuration for specified environment"""
        config_file = self.config_dir / f"{environment.value}.yaml"

        if not config_file.exists():
            logger.warning(f"Configuration file {config_file} not found, creating default")
            return self.create_default_config(environment)

        try:
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)

            # Decrypt sensitive values
            config_data = self._decrypt_config_data(config_data)

            return self._dict_to_config(config_data, environment)

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise

    def save_config(self, config: GlobalConfig) -> None:
        """Save configuration to file"""
        config_file = self.config_dir / f"{config.environment.value}.yaml"

        try:
            # Convert to dict and encrypt sensitive values
            config_data = asdict(config)
            config_data = self._encrypt_config_data(config_data)

            with open(config_file, 'w') as f:
                yaml.dump(config_data, f, default_flow_style=False, indent=2)

            logger.info(f"Configuration saved to {config_file}")

        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            raise

    def create_default_config(self, environment: Environment) -> GlobalConfig:
        """Create default configuration for environment"""
        config = GlobalConfig(environment=environment)

        # Environment-specific defaults
        if environment == Environment.DEVELOPMENT:
            config.debug = True
            config.database.host = "localhost"
            config.redis.host = "localhost"
            config.monitoring.log_level = "DEBUG"

        elif environment == Environment.STAGING:
            config.debug = False
            config.database.host = "staging-db.internal"
            config.redis.host = "staging-redis.internal"
            config.monitoring.log_level = "INFO"

        elif environment == Environment.PRODUCTION:
            config.debug = False
            config.database.host = "prod-db.internal"
            config.redis.host = "prod-redis.internal"
            config.monitoring.log_level = "WARNING"
            config.security.enable_2fa = True

        return config

    def add_organization(self, config: GlobalConfig, org_config: OrganizationConfig) -> GlobalConfig:
        """Add organization to configuration"""
        config.organizations[org_config.name] = org_config
        return config

    def remove_organization(self, config: GlobalConfig, org_name: str) -> GlobalConfig:
        """Remove organization from configuration"""
        if org_name in config.organizations:
            del config.organizations[org_name]
        return config

    def generate_env_file(self, config: GlobalConfig, output_path: str) -> None:
        """Generate .env file from configuration"""
        env_vars = self._config_to_env_vars(config)

        with open(output_path, 'w') as f:
            f.write("# Multi-Tenant Data Ingestion Framework Environment Configuration\n")
            f.write(f"# Generated for environment: {config.environment.value}\n")
            f.write(f"# Generated at: {logger.name}\n\n")

            for key, value in env_vars.items():
                f.write(f"{key}={value}\n")

        logger.info(f"Environment file generated: {output_path}")

    def generate_k8s_manifests(self, config: GlobalConfig, org_name: str, output_dir: str) -> None:
        """Generate Kubernetes manifests for organization"""
        if org_name not in config.organizations:
            raise ValueError(f"Organization {org_name} not found in configuration")

        org_config = config.organizations[org_name]
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Load template
        template_file = self.template_dir / "kubernetes" / "namespace-template.yaml"
        if not template_file.exists():
            logger.warning(f"Template file {template_file} not found")
            return

        with open(template_file, 'r') as f:
            template_content = f.read()

        # Render template
        template = jinja2.Template(template_content)
        rendered = template.render(
            organization=org_config,
            global_config=config,
            environment=config.environment.value
        )

        # Save rendered manifest
        manifest_file = output_path / f"{org_name}-namespace.yaml"
        with open(manifest_file, 'w') as f:
            f.write(rendered)

        logger.info(f"Kubernetes manifest generated: {manifest_file}")

    def generate_docker_compose(self, config: GlobalConfig, output_path: str) -> None:
        """Generate docker-compose.yml from configuration"""
        template_file = self.template_dir / "docker-compose-template.yaml"

        if not template_file.exists():
            logger.warning(f"Template file {template_file} not found")
            return

        with open(template_file, 'r') as f:
            template_content = f.read()

        template = jinja2.Template(template_content)
        rendered = template.render(config=config)

        with open(output_path, 'w') as f:
            f.write(rendered)

        logger.info(f"Docker Compose file generated: {output_path}")

    def validate_config(self, config: GlobalConfig) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []

        # Check required fields
        if not config.security.jwt_secret_key:
            issues.append("JWT secret key is not set")

        if not config.security.encryption_key:
            issues.append("Encryption key is not set")

        if config.environment == Environment.PRODUCTION:
            if config.debug:
                issues.append("Debug mode should be disabled in production")

            if not config.security.enable_2fa:
                issues.append("2FA should be enabled in production")

        # Validate organization configurations
        for org_name, org_config in config.organizations.items():
            if not org_config.contact_email:
                issues.append(f"Contact email not set for organization {org_name}")

            if not org_config.cost_center:
                issues.append(f"Cost center not set for organization {org_name}")

        return issues

    def _dict_to_config(self, config_data: Dict[str, Any], environment: Environment) -> GlobalConfig:
        """Convert dictionary to GlobalConfig object"""
        # Create global config
        global_config = GlobalConfig(environment=environment)

        # Update with loaded data
        for key, value in config_data.items():
            if hasattr(global_config, key):
                setattr(global_config, key, value)

        return global_config

    def _encrypt_config_data(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive values in configuration data"""
        sensitive_fields = [
            'password', 'token', 'secret', 'key', 'access_key', 'secret_key'
        ]

        def encrypt_recursive(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if any(field in key.lower() for field in sensitive_fields):
                        if isinstance(value, str) and value:
                            obj[key] = f"encrypted:{self.encrypt_value(value)}"
                    else:
                        encrypt_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    encrypt_recursive(item)

        encrypt_recursive(config_data)
        return config_data

    def _decrypt_config_data(self, config_data: Dict[str, Any]) -> Dict[str, Any]: