# research/run_segment_pipeline.sh

Script to run the **`segment`** and **`segment_identity`** pipe-segment steps
sequentially on **Dataflow**, using a custom image built with a **specific commit of the
`gpsdio-segment` library**. Intended for research testing.

## What it does

Runs 5 steps in order. If any step fails, the script aborts (`set -euo pipefail`) and the
later steps are not executed.

1. **Pins the hash** of `gpsdio-segment` in `pyproject.toml` (repo root).
2. **Rebuilds the `dev` image** and **regenerates `requirements.txt`** with `make reqs`.
3. **Builds** the prod image for `linux/amd64` and **pushes** it to Artifact Registry
   (`docker buildx ... --push`).
4. Runs **`segment`** on Dataflow with `--wait_for_job` (blocks until it finishes).
5. Runs **`segment_identity`** on Dataflow with `--wait_for_job`, same `DATE_RANGE`.

Both steps use the **same image** (`sdk_container_image = IMAGE:COMMIT_HASH`) and the
**same date range** (`DATE_RANGE`).

## Prerequisites

| Requirement                                                       | How                     |
| ----------------------------------------------------------------- | ----------------------- |
| Docker with **buildx** (multi-platform builds)                    | `docker buildx version` |
| **GCP auth** for the `dev` service (volume `gcp`)                 | `make docker-gcp`       |
| **gcloud login** with **push** permission to the `IMAGE` registry | `gcloud auth login`     |
| Read/write access to the referenced **BigQuery tables**           | —                       |

> The script runs `gcloud auth configure-docker <registry-host>` automatically
> (idempotent). That sets up the credential helper, but it does **not** replace
> `gcloud auth login`: without a session or push permission, `docker push` fails with
> 401/403.

## Configuration

Edit the variables at the top of [`run_segment_pipeline.sh`](./run_segment_pipeline.sh):

| Variable                  | Required | Description                                                                                                    |
| ------------------------- | :------: | -------------------------------------------------------------------------------------------------------------- |
| `COMMIT_HASH`             | **yes**  | `gpsdio-segment` commit. The script aborts if it is still the placeholder. The image tag is derived from this. |
| `DATE_RANGE`              | **yes**  | Date range shared by both steps. Format `YYYY-MM-DD,YYYY-MM-DD`.                                               |
| `segment` tables          | **yes**  | `OUT_SEGMENTED_MESSAGES_TABLE`, `OUT_SEGMENTS_TABLE`, `OUT_SAT_OFFSETS_TABLE` and `FRAGMENTS_TABLE`.           |
| `segment_identity` tables | **yes**  | `SOURCE_SEGMENTS`, `SOURCE_FRAGMENTS`, `DEST_SEGMENT_IDENTITY`.                                                |

## Usage

From the repo root:

```bash
./research/run_segment_pipeline.sh
```

The script `cd`s to the repo root itself, so it does not matter where you invoke it from
as long as it is inside the repo.

## What to expect

- Logs with `[HH:MM:SS]` timestamp per step, with `START`/`END` and duration in seconds.
- On startup it prints a summary of the resolved configuration (hash, image, dates, job
  names).
- Two jobs in the Dataflow console, **sequential**:
  - `<JOB_NAME_PREFIX>-segment--<YYYYMMDD>`
  - `<JOB_NAME_PREFIX>-segment-identity-daily--<YYYYMMDD>`
- **Fail-fast**: if `segment` fails, `segment_identity` is not launched.

## Troubleshooting

| Symptom                                | Likely cause / fix                                                                                         |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `docker push` → **401 / 403**          | No session or no push permission. `gcloud auth login` + verify access to the Artifact Registry repo.       |
| Auth error when launching the job      | The `gcp` volume is not authenticated. Run `make docker-gcp`.                                              |
| **Duplicate job name**                 | A job with that name is already running. Wait for it to finish or change `DATE_RANGE` / `JOB_NAME_PREFIX`. |
| Image does not start on workers (arch) | The build must be `linux/amd64` (already forced with `--platform`). On arm64 Macs this is mandatory.       |
| `ERROR: set COMMIT_HASH ...`           | You did not edit `COMMIT_HASH` (still the placeholder).                                                    |

## Important note

The script **modifies the repo-root `pyproject.toml` and `requirements.txt`** (that is
where the package definition lives; the image is built from there). After testing, if you
do not want to keep the hash change:

```bash
git checkout -- pyproject.toml requirements.txt
```
