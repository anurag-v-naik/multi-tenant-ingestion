#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if organization name is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: Organization name is required${NC}"
    echo "Usage: $0 <organization_name> [environment]"
    echo "Example: $0 finance staging"
    exit 1
fi

ORGANIZATION_NAME="$1"
ENVIRONMENT="${2:-staging}"

echo -e "${BLUE}🏢 Setting up organization: ${ORGANIZATION_NAME} (${ENVIRONMENT})${NC}"

# Validate organization name
if [[ ! "$ORGANIZATION_NAME" =~ ^[a-z0-9-]+$ ]]; then
    echo -e "${RED}Error: Organization name must contain only lowercase letters, numbers, and hyphens${NC}"
    exit 1
fi

# Check prerequisites
echo -e "${BLUE}🔍 Checking prerequisites...${NC}"

if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI not found${NC}"
    exit 1
fi

if ! command -v databricks &> /dev/null; then
    echo -e "${RED}Error: Databricks CLI not found${NC}"
    exit 1
fi

if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites check passed${NC}"

# Get AWS account details
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=${AWS_REGION:-us-east-1}

# 1. Create organization-specific S3 bucket
echo -e "${BLUE}🪣 Creating S3 bucket for ${ORGANIZATION_NAME}...${NC}"

BUCKET_NAME="multi-tenant-ingestion-${ORGANIZATION_NAME}-data-$(openssl rand -hex 4)"

aws s3 mb s3://$BUCKET_NAME --region $AWS_REGION

# Configure bucket settings
aws s3api put-bucket-versioning \
    --bucket $BUCKET_NAME \
    --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
    --bucket $BUCKET_NAME \
    --server-side-encryption-configuration '{
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "aws:kms"
                }
            }
        ]
    }'

# Set bucket policy for organization access
aws s3api put-bucket-policy \
    --bucket $BUCKET_NAME \
    --policy "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [
            {
                \"Sid\": \"OrganizationAccess\",
                \"Effect\": \"Allow\",
                \"Principal\": {
                    \"AWS\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:root\"
                },
                \"Action\": \"s3:*\",
                \"Resource\": [
                    \"arn:aws:s3:::${BUCKET_NAME}\",
                    \"arn:aws:s3:::${BUCKET_NAME}/*\"
                ],
                \"Condition\": {
                    \"StringEquals\": {
                        \"s3:ExistingObjectTag/Organization\": \"${ORGANIZATION_NAME}\"
                    }
                }
            }
        ]
    }"

echo -e "${GREEN}✅ S3 bucket created: ${BUCKET_NAME}${NC}"

# 2. Create organization-specific KMS key
echo -e "${BLUE}🔐 Creating KMS key for ${ORGANIZATION_NAME}...${NC}"

KMS_KEY_POLICY="{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
        {
            \"Sid\": \"Enable IAM User Permissions\",
            \"Effect\": \"Allow\",
            \"Principal\": {
                \"AWS\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:root\"
            },
            \"Action\": \"kms:*\",
            \"Resource\": \"*\"
        },
        {
            \"Sid\": \"OrganizationAccess\",
            \"Effect\": \"Allow\",
            \"Principal\": {
                \"AWS\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/multi-tenant-ingestion-ecs-task\"
            },
            \"Action\": [
                \"kms:Decrypt\",
                \"kms:Encrypt\",
                \"kms:GenerateDataKey\"
            ],
            \"Resource\": \"*\"
        }
    ]
}"

KMS_KEY_ID=$(aws kms create-key \
    --description "KMS key for ${ORGANIZATION_NAME} organization" \
    --policy "$KMS_KEY_POLICY" \
    --query KeyMetadata.KeyId \
    --output text)

# Create alias for the key
aws kms create-alias \
    --alias-name "alias/multi-tenant-ingestion-${ORGANIZATION_NAME}" \
    --target-key-id $KMS_KEY_ID

echo -e "${GREEN}✅ KMS key created: ${KMS_KEY_ID}${NC}"

# 3. Create Databricks workspace (if not exists)
echo -e "${BLUE}🎯 Setting up Databricks workspace for ${ORGANIZATION_NAME}...${NC}"

# Check if workspace already exists
WORKSPACE_NAME="${ORGANIZATION_NAME}-workspace"

if databricks workspaces list --output json | jq -e ".workspaces[] | select(.workspace_name == \"$WORKSPACE_NAME\")" > /dev/null; then
    echo -e "${YELLOW}⚠️  Databricks workspace ${WORKSPACE_NAME} already exists${NC}"
    WORKSPACE_URL=$(databricks workspaces list --output json | jq -r ".workspaces[] | select(.workspace_name == \"$WORKSPACE_NAME\") | .workspace_url")
else
    # Create new workspace
    WORKSPACE_RESULT=$(databricks workspaces create \
        --workspace-name $WORKSPACE_NAME \
        --aws-region $AWS_REGION \
        --output json)
    
    WORKSPACE_URL=$(echo $WORKSPACE_RESULT | jq -r '.workspace_url')
    
    echo -e "${GREEN}✅ Databricks workspace created: ${WORKSPACE_URL}${NC}"
fi

# 4. Set up Unity Catalog for the organization
echo -e "${BLUE}📊 Setting up Unity Catalog for ${ORGANIZATION_NAME}...${NC}"

# Create catalog
CATALOG_NAME="${ORGANIZATION_NAME}_catalog"

databricks catalogs create \
    --name $CATALOG_NAME \
    --comment "Data catalog for ${ORGANIZATION_NAME} organization" \
    --storage-root "s3://${BUCKET_NAME}/unity-catalog/"

# Create schemas
SCHEMAS=("bronze" "silver" "gold" "analytics" "sandbox")

for schema in "${SCHEMAS[@]}"; do
    databricks schemas create \
        --catalog-name $CATALOG_NAME \
        --name $schema \
        --comment "${schema} schema for ${ORGANIZATION_NAME}"
    
    echo -e "${GREEN}✅ Schema created: ${CATALOG_NAME}.${schema}${NC}"
done

# 5. Create organization secrets in AWS Secrets Manager
echo -e "${BLUE}🔒 Creating organization secrets...${NC}"

# Generate organization configuration secret
ORGANIZATION_CONFIG="{
    \"organization_id\": \"${ORGANIZATION_NAME}\",
    \"display_name\": \"$(echo ${ORGANIZATION_NAME} | sed 's/./\U&/' | sed 's/-/ /g') Organization\",
    \"s3_bucket\": \"${BUCKET_NAME}\",
    \"kms_key_id\": \"${KMS_KEY_ID}\",
    \"databricks_workspace_url\": \"${WORKSPACE_URL}\",
    \"unity_catalog_name\": \"${CATALOG_NAME}\",
    \"resource_quotas\": {
        \"max_dbu_per_hour\": 100,
        \"max_storage_gb\": 10000,
        \"max_api_calls_per_minute\": 1000
    },
    \"compliance_level\": \"medium\",
    \"cost_center\": \"$(echo ${ORGANIZATION_NAME} | tr '[:lower:]' '[:upper:]')-001\"
}"

# Create the secret
SECRET_NAME="multi-tenant-ingestion/${ORGANIZATION_NAME}/config"

aws secretsmanager create-secret \
    --name $SECRET_NAME \
    --description "Configuration for ${ORGANIZATION_NAME} organization" \
    --secret-string "$ORGANIZATION_CONFIG" \
    --tags '[{"Key":"Organization","Value":"'${ORGANIZATION_NAME}'"},{"Key":"Environment","Value":"'${ENVIRONMENT}'"}]'

echo -e "${GREEN}✅ Organization secrets created${NC}"

# 6. Create sample data and test pipeline
echo -e "${BLUE}📝 Creating sample pipeline for ${ORGANIZATION_NAME}...${NC}"

# Create sample pipeline configuration
SAMPLE_PIPELINE="{
    \"name\": \"${ORGANIZATION_NAME}-sample-pipeline\",
    \"description\": \"Sample data pipeline for ${ORGANIZATION_NAME}\",
    \"pipeline_type\": \"batch\",
    \"source_config\": {
        \"type\": \"sample_data\",
        \"parameters\": {
            \"record_count\": 1000,
            \"data_type\": \"transactions\"
        }
    },
    \"target_config\": {
        \"type\": \"iceberg\",
        \"catalog\": \"${CATALOG_NAME}\",
        \"schema\": \"bronze\",
        \"table\": \"sample_transactions\",
        \"mode\": \"overwrite\"
    },
    \"schedule_config\": {
        \"enabled\": false,
        \"cron_expression\": \"0 2 * * *\",
        \"timezone\": \"UTC\"
    }
}"

# Save pipeline configuration
mkdir -p "organizations/${ORGANIZATION_NAME}"
echo "$SAMPLE_PIPELINE" > "organizations/${ORGANIZATION_NAME}/sample-pipeline.json"

echo -e "${GREEN}✅ Sample pipeline configuration created${NC}"

# 7. Set up monitoring and alerting
echo -e "${BLUE}📊 Setting up monitoring for ${ORGANIZATION_NAME}...${NC}"

# Create CloudWatch dashboard for the organization
DASHBOARD_BODY="{
    \"widgets\": [
        {
            \"type\": \"metric\",
            \"x\": 0,
            \"y\": 0,
            \"width\": 12,
            \"height\": 6,
            \"properties\": {
                \"metrics\": [
                    [\"AWS/ECS\", \"CPUUtilization\", \"ServiceName\", \"pipeline-service\"],
                    [\".\", \"MemoryUtilization\", \".\", \".\"]
                ],
                \"period\": 300,
                \"stat\": \"Average\",
                \"region\": \"${AWS_REGION}\",
                \"title\": \"${ORGANIZATION_NAME} Service Metrics\"
            }
        }
    ]
}"

aws cloudwatch put-dashboard \
    --dashboard-name "MultiTenant-${ORGANIZATION_NAME}" \
    --dashboard-body "$DASHBOARD_BODY"

# Create budget for cost tracking
aws budgets create-budget \
    --account-id $AWS_ACCOUNT_ID \
    --budget "{
        \"BudgetName\": \"${ORGANIZATION_NAME}-monthly-budget\",
        \"BudgetLimit\": {
            \"Amount\": \"1000\",
            \"Unit\": \"USD\"
        },
        \"TimeUnit\": \"MONTHLY\",
        \"BudgetType\": \"COST\",
        \"CostFilters\": {
            \"TagKey\": [\"Organization\"],
            \"TagValue\": [\"${ORGANIZATION_NAME}\"]
        }
    }" \
    --notifications-with-subscribers "[{
        \"Notification\": {
            \"NotificationType\": \"ACTUAL\",
            \"ComparisonOperator\": \"GREATER_THAN\",
            \"Threshold\": 80
        },
        \"Subscribers\": [{
            \"SubscriptionType\": \"EMAIL\",
            \"Address\": \"admin@company.com\"
        }]
    }]"

echo -e "${GREEN}✅ Monitoring and budgets configured${NC}"

# 8. Validate setup
echo -e "${BLUE}✅ Validating organization setup...${NC}"

# Test S3 access
echo "test" | aws s3 cp - s3://$BUCKET_NAME/test.txt
aws s3 rm s3://$BUCKET_NAME/test.txt

# Test KMS key
TEST_PLAINTEXT="Hello, ${ORGANIZATION_NAME}!"
ENCRYPTED=$(aws kms encrypt --key-id $KMS_KEY_ID --plaintext "$TEST_PLAINTEXT" --query CiphertextBlob --output text)
DECRYPTED=$(aws kms decrypt --ciphertext-blob "$ENCRYPTED" --query Plaintext --output text | base64 --decode)

if [ "$TEST_PLAINTEXT" = "$DECRYPTED" ]; then
    echo -e "${GREEN}✅ KMS key validation passed${NC}"
else
    echo -e "${RED}❌ KMS key validation failed${NC}"
    exit 1
fi

# Test Databricks connectivity
if databricks catalogs get --name $CATALOG_NAME > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Unity Catalog validation passed${NC}"
else
    echo -e "${RED}❌ Unity Catalog validation failed${NC}"
    exit 1
fi

# 9. Generate organization summary
echo -e "${BLUE}📋 Organization Setup Summary${NC}"

SETUP_SUMMARY="{
    \"organization_name\": \"${ORGANIZATION_NAME}\",
    \"environment\": \"${ENVIRONMENT}\",
    \"setup_date\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
    \"resources\": {
        \"s3_bucket\": \"${BUCKET_NAME}\",
        \"kms_key_id\": \"${KMS_KEY_ID}\",
        \"databricks_workspace_url\": \"${WORKSPACE_URL}\",
        \"unity_catalog_name\": \"${CATALOG_NAME}\",
        \"secret_name\": \"${SECRET_NAME}\"
    },
    \"schemas_created\": $(printf '%s\n' "${SCHEMAS[@]}" | jq -R . | jq -s .),
    \"monitoring\": {
        \"cloudwatch_dashboard\": \"MultiTenant-${ORGANIZATION_NAME}\",
        \"budget_name\": \"${ORGANIZATION_NAME}-monthly-budget\"
    }
}"

echo "$SETUP_SUMMARY" > "organizations/${ORGANIZATION_NAME}/setup-summary.json"

# Display summary
echo -e "${GREEN}🎉 Organization setup completed successfully!${NC}"
echo -e ""
echo -e "${BLUE}📊 Resources Created:${NC}"
echo -e "  • S3 Bucket: ${BUCKET_NAME}"
echo -e "  • KMS Key: ${KMS_KEY_ID}"
echo -e "  • Databricks Workspace: ${WORKSPACE_URL}"
echo -e "  • Unity Catalog: ${CATALOG_NAME}"
echo -e "  • Secrets: ${SECRET_NAME}"
echo -e ""
echo -e "${BLUE}📈 Next Steps:${NC}"
echo -e "  1. Configure Databricks workspace settings"
echo -e "  2. Set up data connectors for ${ORGANIZATION_NAME}"
echo -e "  3. Create and test data pipelines"
echo -e "  4. Configure user access and permissions"
echo -e "  5. Set up monitoring alerts"
echo -e ""
echo -e "Setup summary saved to: organizations/${ORGANIZATION_NAME}/setup-summary.json"
