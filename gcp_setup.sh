#!/bin/bash
# =============================================================================
# FunnelPulse GCP Setup Script
# =============================================================================
# This script sets up the entire GCP infrastructure for FunnelPulse:
# - Creates a Dataproc cluster
# - Uploads job scripts to GCS
# - Optionally runs all pipeline jobs
#
# Usage:
#   ./gcp_setup.sh           # Setup only (cluster + upload scripts)
#   ./gcp_setup.sh --run-all # Setup + run all jobs
#   ./gcp_setup.sh --delete  # Delete all resources
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION - Edit these values as needed
# =============================================================================
PROJECT_ID="funnelpulse-479512"
REGION="us-central1"
ZONE="us-central1-a"
CLUSTER_NAME="funnelpulse-cluster"
GCS_BUCKET="gs://funnelpulse-data-479512"

# Cluster configuration
MASTER_MACHINE_TYPE="n1-standard-2"
MASTER_BOOT_DISK_SIZE="50GB"
NUM_WORKERS=2
WORKER_MACHINE_TYPE="n1-standard-2"
WORKER_BOOT_DISK_SIZE="50GB"
IMAGE_VERSION="2.1-debian11"

# Script directory (relative to this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOBS_DIR="${SCRIPT_DIR}/gcp_jobs"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

print_header() {
    echo ""
    echo "============================================================"
    echo "$1"
    echo "============================================================"
}

print_step() {
    echo ""
    echo ">>> $1"
}

check_gcloud() {
    if ! command -v gcloud &> /dev/null; then
        echo "ERROR: gcloud CLI is not installed."
        echo "Install it from: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
}

check_authenticated() {
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 &> /dev/null; then
        echo "ERROR: Not authenticated with gcloud."
        echo "Run: gcloud auth login"
        exit 1
    fi
}

set_project() {
    print_step "Setting project to ${PROJECT_ID}"
    gcloud config set project ${PROJECT_ID}
}

# =============================================================================
# CLUSTER MANAGEMENT
# =============================================================================

create_cluster() {
    print_header "Creating Dataproc Cluster: ${CLUSTER_NAME}"

    # Check if cluster already exists
    if gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID} &> /dev/null; then
        echo "Cluster ${CLUSTER_NAME} already exists. Skipping creation."
        return 0
    fi

    echo "Creating cluster with:"
    echo "  - Master: ${MASTER_MACHINE_TYPE} (${MASTER_BOOT_DISK_SIZE})"
    echo "  - Workers: ${NUM_WORKERS} x ${WORKER_MACHINE_TYPE} (${WORKER_BOOT_DISK_SIZE})"
    echo "  - Image: ${IMAGE_VERSION}"
    echo "  - Components: Jupyter"
    echo ""
    echo "This may take 2-3 minutes..."

    gcloud dataproc clusters create ${CLUSTER_NAME} \
        --region=${REGION} \
        --zone=${ZONE} \
        --master-machine-type=${MASTER_MACHINE_TYPE} \
        --master-boot-disk-size=${MASTER_BOOT_DISK_SIZE} \
        --num-workers=${NUM_WORKERS} \
        --worker-machine-type=${WORKER_MACHINE_TYPE} \
        --worker-boot-disk-size=${WORKER_BOOT_DISK_SIZE} \
        --image-version=${IMAGE_VERSION} \
        --optional-components=JUPYTER \
        --enable-component-gateway \
        --project=${PROJECT_ID}

    echo ""
    echo "Cluster created successfully!"
}

delete_cluster() {
    print_header "Deleting Dataproc Cluster: ${CLUSTER_NAME}"

    if ! gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID} &> /dev/null; then
        echo "Cluster ${CLUSTER_NAME} does not exist. Nothing to delete."
        return 0
    fi

    echo "Deleting cluster..."
    gcloud dataproc clusters delete ${CLUSTER_NAME} \
        --region=${REGION} \
        --project=${PROJECT_ID} \
        --quiet

    echo "Cluster deleted successfully!"
}

# =============================================================================
# SCRIPT UPLOAD
# =============================================================================

upload_scripts() {
    print_header "Uploading Job Scripts to GCS"

    if [ ! -d "${JOBS_DIR}" ]; then
        echo "ERROR: Jobs directory not found: ${JOBS_DIR}"
        exit 1
    fi

    echo "Uploading scripts from: ${JOBS_DIR}"
    echo "Destination: ${GCS_BUCKET}/jobs/"

    gsutil -m cp "${JOBS_DIR}"/*.py "${GCS_BUCKET}/jobs/"

    echo ""
    echo "Scripts uploaded:"
    gsutil ls "${GCS_BUCKET}/jobs/"
}

# =============================================================================
# JOB EXECUTION
# =============================================================================

submit_job() {
    local job_name=$1
    local job_file=$2

    print_step "Running: ${job_name}"
    echo "Script: ${job_file}"

    gcloud dataproc jobs submit pyspark "${GCS_BUCKET}/jobs/${job_file}" \
        --cluster=${CLUSTER_NAME} \
        --region=${REGION} \
        --project=${PROJECT_ID}

    echo "${job_name} completed!"
}

run_all_jobs() {
    print_header "Running All Pipeline Jobs"

    echo "This will run the following jobs in sequence:"
    echo "  1. Batch Bronze-Silver-Gold (skip if tables exist)"
    echo "  2. Additional Gold Tables"
    echo "  3. Build Stream Input"
    echo "  4. Streaming Funnel"
    echo "  5. Anomaly Detection"
    echo "  6. Summary Report"
    echo ""
    read -p "Continue? (y/n) " -n 1 -r
    echo ""

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping job execution."
        return 0
    fi

    # Run jobs in order
    submit_job "Job 1: Batch Bronze-Silver-Gold" "01_batch_bronze_silver_gold.py"
    submit_job "Job 2: Additional Gold Tables" "02_additional_gold_tables.py"
    submit_job "Job 3: Build Stream Input" "03_build_stream_input.py"
    submit_job "Job 4: Streaming Funnel" "04_streaming_funnel.py"
    submit_job "Job 5: Anomaly Detection" "05_anomaly_detection.py"
    submit_job "Job 6: Summary Report" "06_summary_report.py"

    print_header "All Jobs Completed Successfully!"
}

# =============================================================================
# RESOURCE CLEANUP
# =============================================================================

delete_all_resources() {
    print_header "Deleting All GCP Resources"

    echo "This will delete:"
    echo "  - Dataproc cluster: ${CLUSTER_NAME}"
    echo "  - Note: GCS bucket and data will be preserved"
    echo ""
    read -p "Are you sure? (y/n) " -n 1 -r
    echo ""

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    delete_cluster

    print_header "Cleanup Complete!"
    echo ""
    echo "Resources deleted. Your data in GCS is preserved."
    echo "To delete GCS data, run: gsutil -m rm -r ${GCS_BUCKET}"
}

# =============================================================================
# STATUS CHECK
# =============================================================================

show_status() {
    print_header "FunnelPulse GCP Status"

    echo ""
    echo "Project: ${PROJECT_ID}"
    echo "Region:  ${REGION}"
    echo "Bucket:  ${GCS_BUCKET}"
    echo ""

    # Check cluster
    echo "--- Dataproc Cluster ---"
    if gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID} &> /dev/null; then
        echo "Status: RUNNING"
        gcloud dataproc clusters describe ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID} \
            --format="table(clusterName,status.state,config.masterConfig.machineTypeUri,config.workerConfig.numInstances)"
    else
        echo "Status: NOT RUNNING"
    fi

    echo ""
    echo "--- GCS Contents ---"
    echo "Jobs:"
    gsutil ls "${GCS_BUCKET}/jobs/" 2>/dev/null || echo "  (no jobs uploaded)"
    echo ""
    echo "Tables:"
    gsutil ls "${GCS_BUCKET}/tables/" 2>/dev/null || echo "  (no tables)"
}

# =============================================================================
# MAIN
# =============================================================================

main() {
    print_header "FunnelPulse GCP Setup"

    # Check prerequisites
    check_gcloud
    check_authenticated
    set_project

    case "${1:-}" in
        --delete)
            delete_all_resources
            ;;
        --status)
            show_status
            ;;
        --run-all)
            create_cluster
            upload_scripts
            run_all_jobs
            ;;
        --run-jobs)
            run_all_jobs
            ;;
        --upload)
            upload_scripts
            ;;
        *)
            # Default: setup only
            create_cluster
            upload_scripts

            print_header "Setup Complete!"
            echo ""
            echo "Your Dataproc cluster is ready!"
            echo ""
            echo "Next steps:"
            echo "  1. Run all jobs:    ./gcp_setup.sh --run-all"
            echo "  2. Check status:    ./gcp_setup.sh --status"
            echo "  3. Delete cluster:  ./gcp_setup.sh --delete"
            echo ""
            echo "Or run individual jobs:"
            echo "  gcloud dataproc jobs submit pyspark ${GCS_BUCKET}/jobs/01_batch_bronze_silver_gold.py \\"
            echo "      --cluster=${CLUSTER_NAME} --region=${REGION}"
            echo ""
            echo "Access Jupyter at:"
            echo "  https://console.cloud.google.com/dataproc/clusters/${CLUSTER_NAME}/web-interfaces?project=${PROJECT_ID}"
            ;;
    esac
}

# Run main
main "$@"
