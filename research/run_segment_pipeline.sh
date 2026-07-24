#!/bin/bash
#
# run_segment_pipeline.sh
#
# Runs the `segment` and `segment_identity` steps sequentially on Dataflow,
# using a pipe-segment image built and pushed with a specific commit of the
# gpsdio-segment library. Intended for research testing.
#
# Flow:
#   1. Replace the gpsdio-segment ref in pyproject.toml with ${COMMIT_HASH}
#   2. Rebuild the dev image and regenerate requirements.txt (make reqs)
#   3. Build the prod image for linux/amd64 and push it to Artifact Registry
#   4. Run segment            (waits until it finishes)
#   5. Run segment_identity   (waits until it finishes)
#
# If any step fails, the script fails (set -euo pipefail) and the later steps
# are not executed.
#
# Prerequisites: see research/README.md
#
set -euo pipefail

# =============================================================================
# CONFIGURATION  -- edit before running
# =============================================================================

# --- gpsdio-segment / image --------------------------------------------------
# gpsdio-segment v.3.0.0 SHA: 16864b97a009544e9659f2b36349ae70b4400f7c
COMMIT_HASH="16864b97a009544e9659f2b36349ae70b4400f7c" # gpsdio-segment commit hash to use
IMAGE="us-central1-docker.pkg.dev/world-fishing-827/development/pipe-segment-vi-926"
IMAGE_TAG="${COMMIT_HASH}"                   # tag = hash, direct traceability
SDK_CONTAINER_IMAGE="${IMAGE}:${IMAGE_TAG}"

# --- date range (shared by both steps) ---------------------------------------
DATE_RANGE="2024-01-01,2024-06-30"

# --- Dataflow / GCP ----------------------------------------------------------
PROJECT="world-fishing-827"
REGION="us-central1"
SERVICE_ACCOUNT_EMAIL="research-and-development@world-fishing-827.iam.gserviceaccount.com"
JOB_NAME_PREFIX="core-ais-v3-vi-926-segment"       # a per-step suffix is appended

# --- tables: segment ---------------------------------------------------------
# subset from "gfw-int-ais-datalake.vessel_transmissions_normalized_v1.messages"
IN_NORMALIZED_MESSAGES_TABLE="world-fishing-827.vi_924_segment_quick_fix_1.normalized_messages_mmsi_test_set_v20260720" 
OUT_SEGMENTED_MESSAGES_TABLE="world-fishing-827.vi_924_segment_quick_fix_1.messages_segmented"
OUT_SEGMENTS_TABLE="world-fishing-827.vi_924_segment_quick_fix_1.segments"
FRAGMENTS_TABLE="world-fishing-827.vi_924_segment_quick_fix_1.fragments"
IN_NORMALIZED_SAT_OFFSET_MESSAGES_TABLE="world-fishing-827.vi_924_segment_quick_fix_1.normalized_messages_mmsi_test_set_v20260720"
IN_NORAD_TO_RECEIVER_TABLE="global-fishing-watch.pipe_static.norad_to_receiver_v20230510"
IN_SAT_POSITIONS_TABLE="gfw-int-pipe-v3.satellite_positions.satellite_positions_one_second_resolution_"
OUT_SAT_OFFSETS_TABLE="world-fishing-827.vi_924_segment_quick_fix_1.satellite_timing_offsets"

# --- tables: segment_identity ------------------------------------------------
SOURCE_SEGMENTS=$OUT_SEGMENTS_TABLE
SOURCE_FRAGMENTS=$FRAGMENTS_TABLE
DEST_SEGMENT_IDENTITY="world-fishing-827.vi_924_segment_quick_fix_1.segment_identity_daily"

# --- worker resources --------------------------------------------------------
MAX_NUM_WORKERS=50
DISK_SIZE_GB=50
WORKER_MACHINE_TYPE="e2-standard-4"
TEMP_LOCATION="gs://world-fishing-827-us-central1-dataflow/vi-926/dataflow_temp"
STAGING_LOCATION="gs://world-fishing-827-us-central1-dataflow/vi-926/dataflow_staging"
NETWORK="gfw-internal-network"
SUBNETWORK="regions/${REGION}/subnetworks/gfw-internal-${REGION}"

# --- labels (common to both steps) -------------------------------------------
COMMON_LABELS=(
  --labels=environment=development
  --labels=resource_creator=research
  --labels=project=core_pipeline
  --labels=step=segment
  --labels=stage=prototype
)

# =============================================================================
# DERIVED VALUES / LOGGING HELPERS
# =============================================================================

# Date suffix for job_name: first day of DATE_RANGE as YYYYMMDD.
DATE_SUFFIX="$(echo "${DATE_RANGE}" | cut -d, -f1 | tr -d '-')"
JOB_NAME_SEGMENT="${JOB_NAME_PREFIX}-segment--${DATE_SUFFIX}"
JOB_NAME_SEGMENT_IDENTITY="${JOB_NAME_PREFIX}-segment-identity-daily--${DATE_SUFFIX}"

# Registry host, derived from IMAGE (everything before the first '/').
REGISTRY_HOST="${IMAGE%%/*}"

# Repo root (this script lives in research/).
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

CURRENT_STEP="init"

log() {
  printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"
}

on_error() {
  local exit_code=$?
  log "ERROR: step '${CURRENT_STEP}' failed (line ${BASH_LINENO[0]}, exit ${exit_code})."
  log "Aborting. Later steps are NOT executed."
  exit "${exit_code}"
}
trap on_error ERR

# Run a step measuring its duration.
run_step() {
  CURRENT_STEP="$1"
  shift
  local start end
  start="$(date +%s)"
  log "==> START step: ${CURRENT_STEP}"
  "$@"
  end="$(date +%s)"
  log "<== END step: ${CURRENT_STEP} ($((end - start))s)"
}

# =============================================================================
# INITIAL VALIDATION + CONFIG SUMMARY
# =============================================================================

if [ "${COMMIT_HASH}" = "REPLACE_WITH_HASH" ]; then
  log "ERROR: set COMMIT_HASH in the script before running."
  exit 1
fi

cd "${REPO_ROOT}"

log "================ RESOLVED CONFIGURATION ================"
log "repo_root             : ${REPO_ROOT}"
log "commit_hash           : ${COMMIT_HASH}"
log "sdk_container_image   : ${SDK_CONTAINER_IMAGE}"
log "date_range            : ${DATE_RANGE}"
log "project               : ${PROJECT}"
log "region                : ${REGION}"
log "job (segment)         : ${JOB_NAME_SEGMENT}"
log "job (segment_identity): ${JOB_NAME_SEGMENT_IDENTITY}"
log "======================================================="

# =============================================================================
# 1. Pin gpsdio-segment hash in pyproject.toml
# =============================================================================
step_pin_hash() {
  log "Pinning gpsdio-segment to ${COMMIT_HASH} in pyproject.toml"
  sed -i.bak -E \
    "s|(gpsdio-segment\.git@)[A-Za-z0-9._-]+|\1${COMMIT_HASH}|" \
    pyproject.toml
  rm -f pyproject.toml.bak
  log "pyproject.toml: $(grep 'gpsdio-segment' pyproject.toml)"
}
run_step "pin_hash" step_pin_hash

# =============================================================================
# 2. Rebuild dev image + regenerate requirements.txt
# =============================================================================
step_reqs() {
  log "Rebuilding dev image (picks up the new dependency)"
  docker compose build dev
  log "Regenerating requirements.txt with make reqs"
  make reqs
  log "requirements.txt: $(grep 'gpsdio-segment' requirements.txt)"
}
run_step "build_dev_and_reqs" step_reqs

# =============================================================================
# 3. Build prod image (linux/amd64) + push to registry
# =============================================================================
step_build_push() {
  log "Configuring docker auth for ${REGISTRY_HOST} (idempotent)"
  gcloud auth configure-docker "${REGISTRY_HOST}" --quiet
  log "Build linux/amd64: ${SDK_CONTAINER_IMAGE}"
  docker build \
    --platform linux/amd64 \
    --target prod \
    --tag "${SDK_CONTAINER_IMAGE}" \
    --no-cache \
    .
  log "Push: ${SDK_CONTAINER_IMAGE}"
  docker push "${SDK_CONTAINER_IMAGE}"
}
run_step "build_and_push_image" step_build_push

# =============================================================================
# 4. segment
# =============================================================================
step_segment() {
  docker compose run --rm --entrypoint pipe-segment dev segment \
    --date_range="${DATE_RANGE}" \
    --in_normalized_messages_table="${IN_NORMALIZED_MESSAGES_TABLE}" \
    --out_segmented_messages_table="${OUT_SEGMENTED_MESSAGES_TABLE}" \
    --out_segments_table="${OUT_SEGMENTS_TABLE}" \
    --fragments_table="${FRAGMENTS_TABLE}" \
    --in_normalized_sat_offset_messages_table="${IN_NORMALIZED_SAT_OFFSET_MESSAGES_TABLE}" \
    --in_norad_to_receiver_table="${IN_NORAD_TO_RECEIVER_TABLE}" \
    --in_sat_positions_table="${IN_SAT_POSITIONS_TABLE}" \
    --out_sat_offsets_table="${OUT_SAT_OFFSETS_TABLE}" \
    "${COMMON_LABELS[@]}" \
    --project="${PROJECT}" \
    --sdk_container_image="${SDK_CONTAINER_IMAGE}" \
    --max_num_workers="${MAX_NUM_WORKERS}" \
    --disk_size_gb="${DISK_SIZE_GB}" \
    --worker_machine_type="${WORKER_MACHINE_TYPE}" \
    --service_account_email="${SERVICE_ACCOUNT_EMAIL}" \
    --temp_location="${TEMP_LOCATION}" \
    --staging_location="${STAGING_LOCATION}" \
    --region="${REGION}" \
    --network="${NETWORK}" \
    --no_use_public_ips \
    --wait_for_job \
    --job_name="${JOB_NAME_SEGMENT}" \
    --runner=dataflow \
    --subnetwork="${SUBNETWORK}"
}
run_step "segment" step_segment

# =============================================================================
# 5. segment_identity
# =============================================================================
step_segment_identity() {
  docker compose run --rm --entrypoint pipe-segment dev segment_identity \
    --date_range="${DATE_RANGE}" \
    --source_segments="${SOURCE_SEGMENTS}" \
    --source_fragments="${SOURCE_FRAGMENTS}" \
    --dest_segment_identity="${DEST_SEGMENT_IDENTITY}" \
    "${COMMON_LABELS[@]}" \
    --project="${PROJECT}" \
    --sdk_container_image="${SDK_CONTAINER_IMAGE}" \
    --max_num_workers="${MAX_NUM_WORKERS}" \
    --disk_size_gb="${DISK_SIZE_GB}" \
    --worker_machine_type="${WORKER_MACHINE_TYPE}" \
    --service_account_email="${SERVICE_ACCOUNT_EMAIL}" \
    --temp_location="${TEMP_LOCATION}" \
    --staging_location="${STAGING_LOCATION}" \
    --region="${REGION}" \
    --network="${NETWORK}" \
    --no_use_public_ips \
    --wait_for_job \
    --job_name="${JOB_NAME_SEGMENT_IDENTITY}" \
    --runner=dataflow \
    --subnetwork="${SUBNETWORK}"
}
run_step "segment_identity" step_segment_identity

log "OK: segment and segment_identity completed for ${DATE_RANGE}"
