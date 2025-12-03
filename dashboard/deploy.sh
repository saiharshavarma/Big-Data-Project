#!/bin/bash
# Deploy FunnelPulse Dashboard to Google Cloud Run

set -e

# Configuration
PROJECT_ID="${GCP_PROJECT:-funnelpulse-479923}"
REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="funnelpulse-dashboard"
GCS_BUCKET="${GCS_BUCKET:-big-data-project-480103-funnelpulse-data}"

echo "Deploying FunnelPulse Dashboard to Cloud Run"
echo "============================================="
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Service: $SERVICE_NAME"
echo ""

# Build and deploy using Cloud Run source deploy
gcloud run deploy "$SERVICE_NAME" \
    --source=. \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --platform=managed \
    --allow-unauthenticated \
    --set-env-vars="ENVIRONMENT=gcp,GCS_BUCKET=$GCS_BUCKET" \
    --memory=1Gi \
    --cpu=1 \
    --timeout=300 \
    --max-instances=3

echo ""
echo "Deployment complete!"
echo "Run 'gcloud run services describe $SERVICE_NAME --region=$REGION' for service URL"

