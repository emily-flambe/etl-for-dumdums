#!/bin/bash

# Deployment script for Google Cloud Run
# Usage: ./deploy.sh

set -e

# Load .env file if it exists (for local deployment)
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)
fi

# Configuration
PROJECT_ID=${GCP_PROJECT_ID:-"your-project-id"}
REGION=${GCP_REGION:-"us-central1"}
SERVICE_NAME="etl-dashboard"
ARTIFACT_REGISTRY_LOCATION="us-central1"
ARTIFACT_REGISTRY_REPO="cloud-run-apps"
IMAGE_NAME="${ARTIFACT_REGISTRY_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${ARTIFACT_REGISTRY_REPO}/${SERVICE_NAME}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Starting deployment to Google Cloud Run...${NC}"

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}Error: gcloud CLI is not installed${NC}"
    echo "Please install: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if PROJECT_ID is set
if [ "$PROJECT_ID" = "your-project-id" ]; then
    echo -e "${RED}Error: GCP_PROJECT_ID not set${NC}"
    echo "Set it in .env or run: GCP_PROJECT_ID=your-project ./deploy.sh"
    exit 1
fi

echo -e "${GREEN}Project: ${PROJECT_ID}${NC}"
echo -e "${GREEN}Service: ${SERVICE_NAME}${NC}"
echo -e "${GREEN}Region: ${REGION}${NC}"

# Set the project
gcloud config set project ${PROJECT_ID}

# Enable required APIs
echo -e "${GREEN}Enabling required APIs...${NC}"
gcloud services enable cloudbuild.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable secretmanager.googleapis.com

# Create Artifact Registry repository if it doesn't exist
echo -e "${GREEN}Ensuring Artifact Registry repository exists...${NC}"
gcloud artifacts repositories describe ${ARTIFACT_REGISTRY_REPO} \
    --location=${ARTIFACT_REGISTRY_LOCATION} \
    --project=${PROJECT_ID} >/dev/null 2>&1 || \
gcloud artifacts repositories create ${ARTIFACT_REGISTRY_REPO} \
    --repository-format=docker \
    --location=${ARTIFACT_REGISTRY_LOCATION} \
    --description="Docker repository for Cloud Run apps" \
    --project=${PROJECT_ID}

# Build and push using Cloud Build (no local Docker required)
echo -e "${GREEN}Building and pushing image with Cloud Build...${NC}"
gcloud builds submit \
    --tag ${IMAGE_NAME} \
    --project ${PROJECT_ID}

# Deploy to Cloud Run
# Note: Secrets must be created first via setup-secrets.sh
echo -e "${GREEN}Deploying to Cloud Run...${NC}"
gcloud run deploy ${SERVICE_NAME} \
    --image ${IMAGE_NAME} \
    --platform managed \
    --region ${REGION} \
    --allow-unauthenticated \
    --port 8080 \
    --memory 1Gi \
    --cpu 1 \
    --timeout 300 \
    --set-env-vars "DEPLOYMENT_MODE=public,GCP_PROJECT_ID=${PROJECT_ID},GCP_SA_KEY_FILE=/secrets/gcp-sa-key/credentials.json" \
    --set-secrets "/secrets/gcp-sa-key/credentials.json=etl-dashboard-gcp-sa:latest" \
    --project ${PROJECT_ID}

# Get the service URL
SERVICE_URL=$(gcloud run services describe ${SERVICE_NAME} \
    --platform managed \
    --region ${REGION} \
    --project ${PROJECT_ID} \
    --format 'value(status.url)')

echo ""
echo -e "${GREEN}Deployment complete!${NC}"
echo -e "${GREEN}Service URL: ${SERVICE_URL}${NC}"
echo ""
echo -e "${YELLOW}To map custom domain:${NC}"
echo "  gcloud run domain-mappings create --service ${SERVICE_NAME} --domain data.emilycogsdill.com --region ${REGION}"
