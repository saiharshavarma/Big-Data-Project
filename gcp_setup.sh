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
#   ./gcp_setup.sh             # Setup only (cluster + upload scripts)
#   ./gcp_setup.sh --run-all   # Setup + run all batch/file-streaming jobs
#   ./gcp_setup.sh --run-kafka # Setup Kafka infra + run Kafka-based jobs
#   ./gcp_setup.sh --delete    # Delete all resources (cluster + Kafka VM)
#   ./gcp_setup.sh --status    # Show status of cluster, bucket, Kafka VM
# =============================================================================

set -e

# =============================================================================
# CONFIGURATION - Edit these values as needed
# =============================================================================
PROJECT_ID="big-data-project-480103"
REGION="us-central1"
ZONE="us-central1-a"
CLUSTER_NAME="funnelpulse-cluster"
GCS_BUCKET="gs://big-data-project-480103-funnelpulse-data"

# Cluster configuration
MASTER_MACHINE_TYPE="n1-standard-2"
MASTER_BOOT_DISK_SIZE="50GB"
NUM_WORKERS=2
WORKER_MACHINE_TYPE="n1-standard-2"
WORKER_BOOT_DISK_SIZE="50GB"
IMAGE_VERSION="2.1-debian11"

# Kafka VM configuration
KAFKA_VM_NAME="kafka-broker-1"
KAFKA_VM_MACHINE_TYPE="e2-standard-2"
KAFKA_VM_BOOT_DISK_SIZE="20GB"
KAFKA_VM_IMAGE_FAMILY="debian-11"
KAFKA_VM_IMAGE_PROJECT="debian-cloud"

# This is what Dataproc uses to reach Kafka (VM hostname inside the same VPC)
KAFKA_BOOTSTRAP_INTERNAL="${KAFKA_VM_NAME}:9092"

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
# KAFKA VM MANAGEMENT
# =============================================================================

create_kafka_vm() {
    print_header "Creating Kafka VM: ${KAFKA_VM_NAME}"

    if gcloud compute instances describe ${KAFKA_VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} &> /dev/null; then
        echo "Kafka VM ${KAFKA_VM_NAME} already exists. Skipping creation."
        return 0
    fi

    echo "Creating VM with:"
    echo "  - Name: ${KAFKA_VM_NAME}"
    echo "  - Machine type: ${KAFKA_VM_MACHINE_TYPE}"
    echo "  - Boot disk: ${KAFKA_VM_BOOT_DISK_SIZE}"
    echo "  - Image: ${KAFKA_VM_IMAGE_FAMILY} (${KAFKA_VM_IMAGE_PROJECT})"

    gcloud compute instances create "${KAFKA_VM_NAME}" \
        --zone="${ZONE}" \
        --machine-type="${KAFKA_VM_MACHINE_TYPE}" \
        --boot-disk-size="${KAFKA_VM_BOOT_DISK_SIZE}" \
        --image-family="${KAFKA_VM_IMAGE_FAMILY}" \
        --image-project="${KAFKA_VM_IMAGE_PROJECT}" \
        --project="${PROJECT_ID}"

    echo "Kafka VM created."
}

setup_kafka_on_vm() {
    print_header "Installing Docker and starting Kafka on VM: ${KAFKA_VM_NAME}"

    # This command is idempotent:
    # - Installs docker
    # - Starts docker
    # - Creates docker network kafka-net (if not exists)
    # - Ensures zookeeper container is running (recreate if exited)
    # - Recreates kafka container with a pinned, known-good image
    # - Ensures funnelpulse_events topic exists
    gcloud compute ssh "${KAFKA_VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --command "
          set -e
          echo 'Updating apt and installing docker...'
          sudo apt-get update -y
          sudo apt-get install -y docker.io
          sudo systemctl enable docker
          sudo systemctl start docker

          echo 'Creating docker network kafka-net (if needed)...'
          if ! sudo docker network ls | grep -q kafka-net; then
            sudo docker network create kafka-net
          fi

          echo 'Ensuring Zookeeper container is running...'
          if sudo docker ps -a --format '{{.Names}}' | grep -q '^zookeeper$'; then
            # Container exists: if not running, recreate it to avoid bad old configs
            if ! sudo docker ps --format '{{.Names}}' | grep -q '^zookeeper$'; then
              echo 'Zookeeper container exists but is not running. Recreating...'
              sudo docker rm -f zookeeper || true
              sudo docker run -d \
                --name zookeeper \
                --network kafka-net \
                -p 2181:2181 \
                -e ZOOKEEPER_CLIENT_PORT=2181 \
                confluentinc/cp-zookeeper:7.4.0
            fi
          else
            echo 'Zookeeper container not found. Creating...'
            sudo docker run -d \
              --name zookeeper \
              --network kafka-net \
              -p 2181:2181 \
              -e ZOOKEEPER_CLIENT_PORT=2181 \
              confluentinc/cp-zookeeper:7.4.0
          fi

          echo 'Recreating Kafka container with pinned image...'
          # Always recreate Kafka to avoid inheriting any broken config
          if sudo docker ps -a --format '{{.Names}}' | grep -q '^kafka$'; then
            sudo docker rm -f kafka || true
          fi

          sudo docker run -d \
            --name kafka \
            --network kafka-net \
            -p 9092:9092 \
            -e KAFKA_BROKER_ID=1 \
            -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
            -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
            -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${KAFKA_BOOTSTRAP_INTERNAL} \
            -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
            confluentinc/cp-kafka:7.4.0

          echo 'Waiting for Kafka to start...'
          sleep 20

          # Verify Kafka container is running
          if ! sudo docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
            echo 'ERROR: Kafka container is not running after startup. Logs:'
            sudo docker logs kafka || true
            exit 1
          fi

          echo 'Ensuring topic funnelpulse_events exists...'
          sudo docker exec kafka kafka-topics \
            --bootstrap-server localhost:9092 \
            --create \
            --topic funnelpulse_events \
            --partitions 1 \
            --replication-factor 1 \
            || echo 'Topic funnelpulse_events may already exist. Continuing.'

          echo 'Current containers:'
          sudo docker ps

          echo 'Current topics:'
          sudo docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list || true
        "

    echo ""
    echo "Kafka VM is ready. Internal bootstrap address that Dataproc should use:"
    echo "  ${KAFKA_BOOTSTRAP_INTERNAL}"
}

delete_kafka_vm() {
    print_header "Deleting Kafka VM: ${KAFKA_VM_NAME}"

    if ! gcloud compute instances describe ${KAFKA_VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} &> /dev/null; then
        echo "Kafka VM ${KAFKA_VM_NAME} does not exist. Nothing to delete."
        return 0
    fi

    echo "Deleting VM..."
    gcloud compute instances delete "${KAFKA_VM_NAME}" \
        --zone="${ZONE}" \
        --project="${PROJECT_ID}" \
        --quiet

    echo "Kafka VM deleted successfully!"
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
# JOB EXECUTION (BATCH + FILE STREAMING)
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
    print_header "Running All Pipeline Jobs (Batch + File Streaming)"

    echo "This will run the following jobs in sequence:"
    echo "  1. Batch Bronze-Silver-Gold (skip if tables exist)"
    echo "  2. Additional Gold Tables"
    echo "  3. Build Stream Input (file-based)"
    echo "  4. Streaming Funnel (file-based)"
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
    submit_job "Job 3: Build Stream Input (file-based)" "03_build_stream_input.py"
    submit_job "Job 4: Streaming Funnel (file-based)" "04_streaming_funnel.py"
    submit_job "Job 5: Anomaly Detection" "05_anomaly_detection.py"
    submit_job "Job 6: Summary Report" "06_summary_report.py"

    print_header "All Batch + File Streaming Jobs Completed Successfully!"
}

# =============================================================================
# JOB EXECUTION (KAFKA)
# =============================================================================

run_kafka_jobs() {
    print_header "Running Kafka-based Streaming Pipeline"

    echo "This will:"
    echo "  1. Ensure Dataproc cluster exists."
    echo "  2. Upload gcp_jobs scripts (if not already)."
    echo "  3. Ensure Kafka VM exists and Kafka is running in Docker."
    echo "  4. Replay bronze events to Kafka using Spark (Dataproc job)."
    echo "  5. Start the streaming funnel from Kafka on Dataproc (async)."
    echo "  6. Run an inspect job to show streaming gold row counts."
    echo ""
    read -p "Continue with Kafka jobs? (y/n) " -n 1 -r
    echo ""

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Skipping Kafka job execution."
        return 0
    fi

    # Ensure core infra
    create_cluster
    upload_scripts
    create_kafka_vm
    setup_kafka_on_vm

    # K1: Replay bronze events to Kafka
    print_step "Job K1: Replay bronze events to Kafka"
    echo "Using bootstrap servers: ${KAFKA_BOOTSTRAP_INTERNAL}"

    gcloud dataproc jobs submit pyspark \
        kafka/replay_from_bronze_spark.py \
        --cluster=${CLUSTER_NAME} \
        --region=${REGION} \
        --project=${PROJECT_ID} \
        --files=config.py \
        --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_INTERNAL},spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_INTERNAL}"

    echo "Job K1 completed."

    # K2: Streaming funnel from Kafka (long running) - submit async
    print_step "Job K2: Streaming funnel from Kafka (async)"

    gcloud dataproc jobs submit pyspark \
        kafka/funnel_funnel_hourly_brand_from_kafka.py \
        --cluster=${CLUSTER_NAME} \
        --region=${REGION} \
        --project=${PROJECT_ID} \
        --files=config.py \
        --properties="spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,spark.yarn.appMasterEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_INTERNAL},spark.executorEnv.KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_INTERNAL}" \
        --async

    echo ""
    echo "Streaming job submitted in the background."
    echo "You can monitor it in the Dataproc Jobs UI."

    # K3: Inspect streaming gold from Kafka
    print_step "Job K3: Inspect streaming gold from Kafka"

    gcloud dataproc jobs submit pyspark \
        gcp_jobs/inspect_kafka_stream_gold.py \
        --cluster=${CLUSTER_NAME} \
        --region=${REGION} \
        --project=${PROJECT_ID} \
        --files=config.py

    print_header "Kafka-based Pipeline Jobs Submitted/Completed!"
    echo ""
    echo "Notes:"
    echo "  - Job K2 (streaming from Kafka) keeps running until you stop it in the Dataproc UI."
    echo "  - Inspect job K3 shows the current row counts in the Kafka streaming gold table."
}

# =============================================================================
# RESOURCE CLEANUP
# =============================================================================

delete_all_resources() {
    print_header "Deleting All GCP Resources"

    echo "This will delete:"
    echo "  - Dataproc cluster: ${CLUSTER_NAME}"
    echo "  - Kafka VM: ${KAFKA_VM_NAME}"
    echo "  - Note: GCS bucket and data will be preserved"
    echo ""
    read -p "Are you sure? (y/n) " -n 1 -r
    echo ""

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    delete_cluster
    delete_kafka_vm

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
    echo "--- Kafka VM ---"
    if gcloud compute instances describe ${KAFKA_VM_NAME} --zone=${ZONE} --project=${PROJECT_ID} &> /dev/null; then
        echo "Status: PRESENT"
        gcloud compute instances list \
            --filter="name=${KAFKA_VM_NAME}" \
            --zones="${ZONE}" \
            --project="${PROJECT_ID}" \
            --format="table(name,zone,INTERNAL_IP,EXTERNAL_IP,status)"
        echo ""
        echo "Dataproc should use this bootstrap address:"
        echo "  ${KAFKA_BOOTSTRAP_INTERNAL}"
    else
        echo "Status: NOT PRESENT"
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
        --run-kafka)
            run_kafka_jobs
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
            echo "  1. Run all batch/file jobs:   ./gcp_setup.sh --run-all"
            echo "  2. Run Kafka pipeline:        ./gcp_setup.sh --run-kafka"
            echo "  3. Check status:              ./gcp_setup.sh --status"
            echo "  4. Delete all resources:      ./gcp_setup.sh --delete"
            echo ""
            echo "Or run individual jobs, for example:"
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