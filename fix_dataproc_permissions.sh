#!/bin/bash
# Fix Dataproc service account permissions

set -e

PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")

# Dataproc service account
DATAPROC_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

echo "Fixing Dataproc Service Account Permissions"
echo "==========================================="
echo "Project: $PROJECT_ID"
echo "Service Account: $DATAPROC_SA"
echo ""

# Grant Storage Object Admin (read/write to GCS)
echo "Granting Storage Object Admin role..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${DATAPROC_SA}" \
    --role="roles/storage.objectAdmin" \
    --condition=None

# Grant Dataproc Worker role (for cluster operations)
echo "Granting Dataproc Worker role..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${DATAPROC_SA}" \
    --role="roles/dataproc.worker" \
    --condition=None

# Grant Service Account User (to use other service accounts)
echo "Granting Service Account User role..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${DATAPROC_SA}" \
    --role="roles/iam.serviceAccountUser" \
    --condition=None

echo ""
echo "✓ Permissions granted!"
echo ""
echo "Note: The firewall warning is about network security."
echo "For development/testing, this is usually fine."
echo "For production, consider restricting network access."

