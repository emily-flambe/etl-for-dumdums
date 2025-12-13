#!/bin/bash

# Setup secrets in Google Secret Manager for Cloud Run deployment
# Run this once before first deployment

set -e

# Load .env file if it exists
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
fi

PROJECT_ID=${GCP_PROJECT_ID:-"your-project-id"}
SA_KEY_FILE=${GCP_SA_KEY_FILE:-.secrets/gcp-service-account.json}

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Setting up secrets for Cloud Run deployment...${NC}"

# Check prerequisites
if [ "$PROJECT_ID" = "your-project-id" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID not set${NC}"
    exit 1
fi

if [ ! -f "$SA_KEY_FILE" ]; then
    echo -e "${RED}Error: Service account key file not found: ${SA_KEY_FILE}${NC}"
    echo "Expected location: .secrets/gcp-service-account.json"
    exit 1
fi

# Set project
gcloud config set project ${PROJECT_ID}

# Enable Secret Manager API
echo -e "${GREEN}Enabling Secret Manager API...${NC}"
gcloud services enable secretmanager.googleapis.com

# Create or update the GCP service account secret
SECRET_NAME="etl-dashboard-gcp-sa"
echo -e "${GREEN}Creating/updating secret: ${SECRET_NAME}${NC}"

if gcloud secrets describe ${SECRET_NAME} --project=${PROJECT_ID} >/dev/null 2>&1; then
    # Secret exists, add new version
    gcloud secrets versions add ${SECRET_NAME} \
        --data-file="${SA_KEY_FILE}" \
        --project=${PROJECT_ID}
    echo -e "${GREEN}Updated existing secret with new version${NC}"
else
    # Create new secret
    gcloud secrets create ${SECRET_NAME} \
        --data-file="${SA_KEY_FILE}" \
        --replication-policy="automatic" \
        --project=${PROJECT_ID}
    echo -e "${GREEN}Created new secret${NC}"
fi

# Grant Cloud Run service account access to the secret
echo -e "${GREEN}Granting Cloud Run access to secret...${NC}"
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')
CLOUD_RUN_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud secrets add-iam-policy-binding ${SECRET_NAME} \
    --member="serviceAccount:${CLOUD_RUN_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project=${PROJECT_ID}

echo ""
echo -e "${GREEN}Secrets setup complete!${NC}"
echo -e "${YELLOW}You can now run: ./deploy.sh${NC}"
