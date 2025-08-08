#!/bin/bash

# deployment/scripts/setup-organization.sh
# Automated organization provisioning script for multi-tenant data ingestion framework

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_DIR="${ROOT_DIR}/deployment/configs"
K8S_DIR="${ROOT_DIR}/infrastructure/kubernetes"

# Default values
DEFAULT_CPU_REQUEST="100m"
DEFAULT_MEMORY_REQUEST="128Mi"
DEFAULT_CPU_LIMIT="500m"
DEFAULT_MEMORY_LIMIT="512Mi"
MAX_CPU_LIMIT="2000m"
MAX_MEMORY_LIMIT="4Gi"
MIN_CPU_REQUEST="50m"
MIN_MEMORY_REQUEST="64Mi"
POD_MAX_CPU="1000m"
POD_MAX_MEMORY="2Gi"
MAX_PVC_SIZE="100Gi"
MIN_PVC_SIZE="1Gi"
PRIORITY_VALUE="100"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
Organization Setup Script for Multi-Tenant Data Ingestion Framework

Usage: $0 [OPTIONS] ORGANIZATION_NAME

OPTIONS:
    -h, --help              Show this help message
    -c, --config-file       Path to organization configuration file
    -e, --environment       Target environment (development, staging, production)
    -d, --dry-run          Show what would be done without executing
    -f, --force            Force setup even if organization exists
    -s, --skip-k8s         Skip Kubernetes namespace setup
    -b, --skip-databricks  Skip Databricks workspace setup
    -v, --verbose          Enable verbose output
    --cpu-request          Default CPU request (default: ${DEFAULT_CPU_REQUEST})
    --memory-request       Default memory request (default: ${DEFAULT_MEMORY_REQUEST})
    --cpu-limit            Default CPU limit (default: ${DEFAULT_CPU_LIMIT})
    --memory-limit         Default memory limit (default: ${DEFAULT_MEMORY_LIMIT})
    --compliance-level     Compliance level (low, medium, high)
    --cost-center          Cost center code
    --priority             Priority value for workloads (default: ${PRIORITY_VALUE})

EXAMPLES:
    # Basic organization setup
    $0 finance

    # Setup with custom configuration
    $0 --config-file configs/finance.yaml finance

    # Setup with custom resource limits
    $0 --cpu-limit 1000m --memory-limit 2Gi retail

    # Dry run to see what would be created
    $0 --dry-run --verbose healthcare

CONFIGURATION FILE FORMAT:
    Create a YAML file with organization-specific settings:

    organization:
      name: finance
      display_name: "Finance Department"
      compliance_level: high
      cost_center: "FIN-001"

    resources:
      cpu_request: "200m"
      memory_request: "256Mi"
      cpu_limit: "1000m"
      memory_limit: "2Gi"

    databricks:
      workspace_name: "finance-workspace"
      tier: "premium"

    features:
      unity_catalog: true
      auto_scaling: true
      monitoring: true

EOF
}

# Parse command line arguments
parse_arguments() {
    local config_file=""
    local environment="development"
    local dry_run=false
    local force=false
    local skip_k8s=false
    local skip_databricks=false
    local verbose=false
    local compliance_level="medium"
    local cost_center=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -c|--config-file)
                config_file="$2"
                shift 2
                ;;
            -e|--environment)
                environment="$2"
                shift 2
                ;;
            -d|--dry-run)
                dry_run=true
                shift
                ;;
            -f|--force)
                force=true
                shift
                ;;
            -s|--skip-k8s)
                skip_k8s=true
                shift
                ;;
            -b|--skip-databricks)
                skip_databricks=true
                shift
                ;;
            -v|--verbose)
                verbose=true
                shift
                ;;
            --cpu-request)
                DEFAULT_CPU_REQUEST="$2"
                shift 2
                ;;
            --memory-request)
                DEFAULT_MEMORY_REQUEST="$2"
                shift 2
                ;;
            --cpu-limit)
                DEFAULT_CPU_LIMIT="$2"
                shift 2
                ;;
            --memory-limit)
                DEFAULT_MEMORY_LIMIT="$2"
                shift 2
                ;;
            --compliance-level)
                compliance_level="$2"
                shift 2
                ;;
            --cost-center)
                cost_center="$2"
                shift 2
                ;;
            --priority)
                PRIORITY_VALUE="$2"
                shift 2
                ;;
            -*)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
            *)
                if [[ -z "${ORGANIZATION_NAME:-}" ]]; then
                    ORGANIZATION_NAME="$1"
                else
                    log_error "Multiple organization names provided"
                    exit 1
                fi
                shift
                ;;
        esac
    done

    # Validate required arguments
    if [[ -z "${ORGANIZATION_NAME:-}" ]]; then
        log_error "Organization name is required"
        show_help
        exit 1
    fi

    # Validate organization name
    if [[ ! "$ORGANIZATION_NAME" =~ ^[a-z0-9-]+$ ]]; then
        log_error "Organization name must contain only lowercase letters, numbers, and hyphens"
        exit 1
    fi

    # Set global variables
    CONFIG_FILE="$config_file"
    ENVIRONMENT="$environment"
    DRY_RUN="$dry_run"
    FORCE="$force"
    SKIP_K8S="$skip_k8s"
    SKIP_DATABRICKS="$skip_databricks"
    VERBOSE="$verbose"
    COMPLIANCE_LEVEL="$compliance_level"
    COST_CENTER="$cost_center"
}

# Load configuration from file
load_configuration() {
    if [[ -n "$CONFIG_FILE" && -f "$CONFIG_FILE" ]]; then
        log_info "Loading configuration from $CONFIG_FILE"

        # Use yq to parse YAML if available, otherwise use basic parsing
        if command -v yq &> /dev/null; then
            ORGANIZATION_DISPLAY_NAME=$(yq eval '.organization.display_name // ""' "$CONFIG_FILE")
            COMPLIANCE_LEVEL=$(yq eval '.organization.compliance_level // "medium"' "$CONFIG_FILE")
            COST_CENTER=$(yq eval '.organization.cost_center // ""' "$CONFIG_FILE")
            DEFAULT_CPU_REQUEST=$(yq eval '.resources.cpu_request // "100m"' "$CONFIG_FILE")
            DEFAULT_MEMORY_REQUEST=$(yq eval '.resources.memory_request // "128Mi"' "$CONFIG_FILE")
            DEFAULT_CPU_LIMIT=$(yq eval '.resources.cpu_limit // "500m"' "$CONFIG_FILE")
            DEFAULT_MEMORY_LIMIT=$(yq eval '.resources.memory_limit // "512Mi"' "$CONFIG_FILE")
        else
            log_warning "yq not found, using basic configuration parsing"
            # Basic YAML parsing (simplified)
            if grep -q "display_name:" "$CONFIG_FILE"; then
                ORGANIZATION_DISPLAY_NAME=$(grep "display_name:" "$CONFIG_FILE" | cut -d':' -f2 | tr -d ' "')
            fi
        fi
    fi

    # Set defaults if not provided
    ORGANIZATION_DISPLAY_NAME="${ORGANIZATION_DISPLAY_NAME:-${ORGANIZATION_NAME^} Organization}"
    COST_CENTER="${COST_CENTER:-${ORGANIZATION_NAME^^}-001}"
    CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_tools=()

    # Check required tools
    if ! command -v kubectl &> /dev/null; then
        missing_tools+=("kubectl")
    fi

    if ! command -v aws &> /dev/null; then
        missing_tools+=("aws")
    fi

    if ! command -v jq &> /dev/null; then
        missing_tools+=("jq")
    fi

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_error "Please install the missing tools and try again"
        exit 1
    fi

    # Check Kubernetes connectivity
    if [[ "$SKIP_K8S" != "true" ]]; then
        if ! kubectl cluster-info &> /dev/null; then
            log_error "Cannot connect to Kubernetes cluster"
            log_error "Please ensure kubectl is configured and try again"
            exit 1
        fi
    fi

    # Check AWS connectivity
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "Cannot connect to AWS"
        log_error "Please ensure AWS credentials are configured and try again"
        exit 1
    fi

    log_success "Prerequisites check passed"
}

# Check if organization already exists
check_organization_exists() {
    log_info "Checking if organization '$ORGANIZATION_NAME' already exists..."

    local exists=false

    # Check Kubernetes namespace
    if [[ "$SKIP_K8S" != "true" ]] && kubectl get namespace "$ORGANIZATION_NAME" &> /dev/null; then
        exists=true
        log_warning "Kubernetes namespace '$ORGANIZATION_NAME' already exists"
    fi

    # Check AWS secrets
    if aws secretsmanager describe-secret --secret-id "${ORGANIZATION_NAME}/config" &> /dev/null; then
        exists=true
        log_warning "AWS secret for organization '$ORGANIZATION_NAME' already exists"
    fi

    if [[ "$exists" == "true" && "$FORCE" != "true" ]]; then
        log_error "Organization '$ORGANIZATION_NAME' already exists. Use --force to override."
        exit 1
    fi

    if [[ "$exists" == "true" && "$FORCE" == "true" ]]; then
        log_warning "Organization exists but --force specified, continuing with setup"
    fi
}

# Create Kubernetes namespace and RBAC
setup_kubernetes_namespace() {
    if [[ "$SKIP_K8S" == "true" ]]; then
        log_info "Skipping Kubernetes namespace setup"
        return
    fi

    log_info "Setting up Kubernetes namespace and RBAC for '$ORGANIZATION_NAME'..."

    # Create temporary file for processed manifest
    local temp_manifest
    temp_manifest=$(mktemp)

    # Process template variables
    envsubst < "${K8S_DIR}/namespaces/namespace-template.yaml" > "$temp_manifest" << EOF
export ORGANIZATION_NAME="$ORGANIZATION_NAME"
export COMPLIANCE_LEVEL="$COMPLIANCE_LEVEL"
export COST_CENTER="$COST_CENTER"
export CREATED_AT="$CREATED_AT"
export RESOURCE_QUOTAS="{}"
export CPU_REQUESTS_LIMIT="5"
export MEMORY_REQUESTS_LIMIT="10Gi"
export CPU_LIMITS_LIMIT="10"
export MEMORY_LIMITS_LIMIT="20Gi"
export PVC_LIMIT="10"
export SERVICES_LIMIT="20"
export SECRETS_LIMIT="50"
export CONFIGMAPS_LIMIT="50"
export PODS_LIMIT="50"
export DEFAULT_CPU_LIMIT="$DEFAULT_CPU_LIMIT"
export DEFAULT_MEMORY_LIMIT="$DEFAULT_MEMORY_LIMIT"
export DEFAULT_CPU_REQUEST="$DEFAULT_CPU_REQUEST"
export DEFAULT_MEMORY_REQUEST="$DEFAULT_MEMORY_REQUEST"
export MAX_CPU_LIMIT="$MAX_CPU_LIMIT"
export MAX_MEMORY_LIMIT="$MAX_MEMORY_LIMIT"
export MIN_CPU_REQUEST="$MIN_CPU_REQUEST"
export MIN_MEMORY_REQUEST="$MIN_MEMORY_REQUEST"
export POD_MAX_CPU="$POD_MAX_CPU"
export POD_MAX_MEMORY="$POD_MAX_MEMORY"
export MAX_PVC_SIZE="$MAX_PVC_SIZE"
export MIN_PVC_SIZE="$MIN_PVC_SIZE"
export PRIORITY_VALUE="$PRIORITY_VALUE"
EOF

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would apply the following Kubernetes manifest:"
        if [[ "$VERBOSE" == "true" ]]; then
            cat "$temp_manifest"
        else
            echo "Kubernetes namespace and RBAC configuration for $ORGANIZATION_NAME"
        fi
    else
        kubectl apply -f "$temp_manifest"
        log_success "Kubernetes namespace and RBAC created for '$ORGANIZATION_NAME'"
    fi

    # Clean up temporary file
    rm -f "$temp_manifest"
}

# Create AWS resources
setup_aws_resources() {
    log_info "Setting up AWS resources for '$ORGANIZATION_NAME'..."

    # Create organization configuration secret
    local config_secret_name="${ORGANIZATION_NAME}/config"
    local config_data
    config_data=$(cat << EOF
{
    "organization_name": "$ORGANIZATION_NAME",
    "display_name": "$ORGANIZATION_DISPLAY_NAME",
    "compliance_level": "$COMPLIANCE_LEVEL",
    "cost_center": "$COST_CENTER",
    "created_at": "$CREATED_AT",
    "environment": "$ENVIRONMENT",
    "features": {
        "unity_catalog": true,
        "auto_scaling": true,
        "monitoring": true,
        "data_quality": true
    },
    "resource_limits": {
        "cpu_request": "$DEFAULT_CPU_REQUEST",
        "memory_request": "$DEFAULT_MEMORY_REQUEST",
        "cpu_limit": "$DEFAULT_CPU_LIMIT",
        "memory_limit": "$DEFAULT_MEMORY_LIMIT"
    }
}
EOF
)

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create AWS secret '$config_secret_name'"
        if [[ "$VERBOSE" == "true" ]]; then
            echo "Secret data: $config_data"
        fi
    else
        aws secretsmanager create-secret \
            --name "$config_secret_name" \
            --description "Configuration for organization $ORGANIZATION_NAME" \
            --secret-string "$config_data" \
            --tags "Key=Organization,Value=$ORGANIZATION_NAME" "Key=Environment,Value=$ENVIRONMENT" \
            --region "${AWS_REGION:-us-east-1}" || \
        aws secretsmanager update-secret \
            --secret-id "$config_secret_name" \
            --secret-string "$config_data" \
            --region "${AWS_REGION:-us-east-1}"

        log_success "AWS configuration secret created for '$ORGANIZATION_NAME'"
    fi

    # Create S3 bucket for organization data
    local bucket_name="multi-tenant-data-${ORGANIZATION_NAME}-${ENVIRONMENT}"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create S3 bucket '$bucket_name'"
    else
        if ! aws s3api head-bucket --bucket "$bucket_name" 2>/dev/null; then
            aws s3 mb "s3://$bucket_name" --region "${AWS_REGION:-us-east-1}"

            # Apply bucket policy for organization isolation
            local bucket_policy
            bucket_policy=$(cat << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "OrganizationAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:root"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::$bucket_name",
                "arn:aws:s3:::$bucket_name/*"
            ],
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/Organization": "$ORGANIZATION_NAME"
                }
            }
        }
    ]
}
EOF
)

            aws s3api put-bucket-policy \
                --bucket "$bucket_name" \
                --policy "$bucket_policy"

            # Enable versioning and encryption
            aws s3api put-bucket-versioning \
                --bucket "$bucket_name" \
                --versioning-configuration Status=Enabled

            aws s3api put-bucket-encryption \
                --bucket "$bucket_name" \
                --server-side-encryption-configuration '{
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                            }
                        }
                    ]
                }'

            log_success "S3 bucket '$bucket_name' created and configured"
        else
            log_warning "S3 bucket '$bucket_name' already exists"
        fi
    fi
}

# Setup Databricks workspace
setup_databricks_workspace() {
    if [[ "$SKIP_DATABRICKS" == "true" ]]; then
        log_info "Skipping Databricks workspace setup"
        return
    fi

    log_info "Setting up Databricks workspace for '$ORGANIZATION_NAME'..."

    # Check if Databricks CLI is configured
    if ! command -v databricks &> /dev/null; then
        log_warning "Databricks CLI not found, skipping workspace setup"
        log_warning "Please install Databricks CLI and run the setup script manually"
        return
    fi

    local workspace_name="${ORGANIZATION_NAME}-workspace"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create Databricks workspace '$workspace_name'"
        return
    fi

    # Create workspace configuration script
    local databricks_setup_script="${ROOT_DIR}/scripts/setup-databricks-workspace.py"

    if [[ -f "$databricks_setup_script" ]]; then
        python3 "$databricks_setup_script" \
            --organization "$ORGANIZATION_NAME" \
            --workspace-name "$workspace_name" \
            --environment "$ENVIRONMENT"

        log_success "Databricks workspace setup initiated for '$ORGANIZATION_NAME'"
    else
        log_warning "Databricks setup script not found at $databricks_setup_script"
    fi
}

# Create organization database
setup_organization_database() {
    log_info "Setting up organization database records for '$ORGANIZATION_NAME'..."

    # Create organization record in the main database
    local db_config_script="${ROOT_DIR}/scripts/create-organization-db.py"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create organization database records"
        return
    fi

    if [[ -f "$db_config_script" ]]; then
        python3 "$db_config_script" \
            --organization "$ORGANIZATION_NAME" \
            --display-name "$ORGANIZATION_DISPLAY_NAME" \
            --compliance-level "$COMPLIANCE_LEVEL" \
            --cost-center "$COST_CENTER" \
            --environment "$ENVIRONMENT"

        log_success "Organization database records created for '$ORGANIZATION_NAME'"
    else
        log_warning "Database setup script not found at $db_config_script"
    fi
}

# Setup monitoring and alerting
setup_monitoring() {
    log_info "Setting up monitoring and alerting for '$ORGANIZATION_NAME'..."

    if [[ "$SKIP_K8S" == "true" ]]; then
        log_info "Skipping monitoring setup (Kubernetes setup skipped)"
        return
    fi

    # Create ServiceMonitor for Prometheus
    local monitoring_manifest
    monitoring_manifest=$(cat << EOF
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: ${ORGANIZATION_NAME}-service-monitor
  namespace: ${ORGANIZATION_NAME}
  labels:
    organization: ${ORGANIZATION_NAME}
    app: multi-tenant-ingestion
spec:
  selector:
    matchLabels:
      organization: ${ORGANIZATION_NAME}
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
---
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: ${ORGANIZATION_NAME}-alerts
  namespace: ${ORGANIZATION_NAME}
  labels:
    organization: ${ORGANIZATION_NAME}
    app: multi-tenant-ingestion
spec:
  groups:
  - name: ${ORGANIZATION_NAME}.rules
    rules:
    - alert: HighErrorRate
      expr: rate(http_requests_total{organization="${ORGANIZATION_NAME}",status=~"5.."}[5m]) > 0.1
      for: 5m
      labels:
        severity: warning
        organization: ${ORGANIZATION_NAME}
      annotations:
        summary: "High error rate for organization ${ORGANIZATION_NAME}"
        description: "Error rate is {{ \$value }} for organization ${ORGANIZATION_NAME}"

    - alert: HighMemoryUsage
      expr: container_memory_usage_bytes{namespace="${ORGANIZATION_NAME}"} / container_spec_memory_limit_bytes > 0.8
      for: 5m
      labels:
        severity: warning
        organization: ${ORGANIZATION_NAME}
      annotations:
        summary: "High memory usage for organization ${ORGANIZATION_NAME}"
        description: "Memory usage is {{ \$value }}% for organization ${ORGANIZATION_NAME}"
EOF
)

    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would create monitoring configuration"
        if [[ "$VERBOSE" == "true" ]]; then
            echo "$monitoring_manifest"
        fi
    else
        echo "$monitoring_manifest" | kubectl apply -f -
        log_success "Monitoring and alerting configured for '$ORGANIZATION_NAME'"
    fi
}

# Verify organization setup
verify_setup() {
    log_info "Verifying organization setup for '$ORGANIZATION_NAME'..."

    local verification_failed=false

    # Check Kubernetes namespace
    if [[ "$SKIP_K8S" != "true" ]]; then
        if kubectl get namespace "$ORGANIZATION_NAME" &> /dev/null; then
            log_success "✓ Kubernetes namespace exists"
        else
            log_error "✗ Kubernetes namespace not found"
            verification_failed=true
        fi

        # Check service account
        if kubectl get serviceaccount "${ORGANIZATION_NAME}-service-account" -n "$ORGANIZATION_NAME" &> /dev/null; then
            log_success "✓ Service account exists"
        else
            log_error "✗ Service account not found"
            verification_failed=true
        fi
    fi

    # Check AWS resources
    if aws secretsmanager describe-secret --secret-id "${ORGANIZATION_NAME}/config" &> /dev/null; then
        log_success "✓ AWS configuration secret exists"
    else
        log_error "✗ AWS configuration secret not found"
        verification_failed=true
    fi

    # Check S3 bucket
    local bucket_name="multi-tenant-data-${ORGANIZATION_NAME}-${ENVIRONMENT}"
    if aws s3api head-bucket --bucket "$bucket_name" &> /dev/null; then
        log_success "✓ S3 bucket exists"
    else
        log_error "✗ S3 bucket not found"
        verification_failed=true
    fi

    if [[ "$verification_failed" == "true" ]]; then
        log_error "Organization setup verification failed"
        exit 1
    else
        log_success "Organization setup verification passed"
    fi
}

# Generate setup summary
generate_summary() {
    log_info "Organization Setup Summary"
    echo "=================================="
    echo "Organization Name: $ORGANIZATION_NAME"
    echo "Display Name: $ORGANIZATION_DISPLAY_NAME"
    echo "Environment: $ENVIRONMENT"
    echo "Compliance Level: $COMPLIANCE_LEVEL"
    echo "Cost Center: $COST_CENTER"
    echo "Created At: $CREATED_AT"
    echo ""
    echo "Resources Created:"
    if [[ "$SKIP_K8S" != "true" ]]; then
        echo "  - Kubernetes namespace: $ORGANIZATION_NAME"
        echo "  - Service account: ${ORGANIZATION_NAME}-service-account"
        echo "  - RBAC roles and bindings"
        echo "  - Resource quotas and limits"
        echo "  - Network policies"
    fi
    echo "  - AWS configuration secret: ${ORGANIZATION_NAME}/config"
    echo "  - S3 bucket: multi-tenant-data-${ORGANIZATION_NAME}-${ENVIRONMENT}"
    if [[ "$SKIP_DATABRICKS" != "true" ]]; then
        echo "  - Databricks workspace: ${ORGANIZATION_NAME}-workspace"
    fi
    echo ""
    echo "Next Steps:"
    echo "  1. Configure data connectors for the organization"
    echo "  2. Set up data quality rules"
    echo "  3. Create initial data pipelines"
    echo "  4. Configure monitoring dashboards"
    echo "  5. Test data ingestion workflows"
    echo ""
    echo "Access Information:"
    echo "  - API Base URL: https://api.yourdomain.com/api/v1"
    echo "  - Organization Header: X-Organization-ID: $ORGANIZATION_NAME"
    echo "  - Kubernetes Namespace: $ORGANIZATION_NAME"
    echo ""
}

# Cleanup function for error handling
cleanup() {
    local exit_code=$?
    if [[ $exit_code -ne 0 ]]; then
        log_error "Setup failed with exit code $exit_code"
        log_info "Cleaning up partial resources..."

        # Cleanup logic here if needed
        if [[ "$FORCE" == "true" && "$DRY_RUN" != "true" ]]; then
            log_warning "Use the cleanup script to remove partial resources"
        fi
    fi
}

# Main execution function
main() {
    # Set up error handling
    trap cleanup EXIT

    # Parse arguments
    parse_arguments "$@"

    # Load configuration
    load_configuration

    # Print header
    echo "======================================"
    echo "Multi-Tenant Data Ingestion Framework"
    echo "Organization Setup Script"
    echo "======================================"
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        log_warning "DRY RUN MODE - No changes will be made"
        echo ""
    fi

    # Execute setup steps
    check_prerequisites
    check_organization_exists
    setup_kubernetes_namespace
    setup_aws_resources
    setup_databricks_workspace
    setup_organization_database
    setup_monitoring

    if [[ "$DRY_RUN" != "true" ]]; then
        verify_setup
    fi

    generate_summary

    log_success "Organization setup completed successfully!"
}

# Run main function with all arguments
main "$@"