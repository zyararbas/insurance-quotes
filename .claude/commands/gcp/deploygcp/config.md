---
description: GCP deploy config — single source of truth for insurance-quotes' Cloud Run target
---

Canonical GCP deploy variables for **insurance-quotes**. Phase commands read from HERE.

## Stack tool
- STACK_GCP_DIR: `../cc-stack/gcp` — run image/release from here after
  `set -a; source ./env.local.sh; set +a`.
- DEPLOY_TARGET: `quotes`

## Image / registry (Artifact Registry)
- IMAGE: `insurance-quotes`
- Repo: `${REGION}-docker.pkg.dev/${PROJECT_ID}/coveragecompassai/insurance-quotes:latest`
- PROJECT_ID / REGION from `env.local.sh` (`gen-lang-client-0489622085` / `us-west1`).

## Cloud Run rollout target
- SERVICE: `insurance-quotes` — **INTERNAL ingress** (direct `*.run.app` hits 404 by design).

## Build specifics / conventions
- **Heavy image (~5 min build).** The Dockerfile bakes the **vehicle-rates Chroma DB +
  the embedding model** and runs HF offline (`HF_HUB_OFFLINE=1`) — no S3/HuggingFace
  download at startup. Keep those bake steps.
- `vehicle_rates_chroma_db/` is **gitignored** but MUST be in the build context — a
  `.gcloudignore` in this repo keeps it (do not let `.gitignore` exclude it from Cloud Build).
- Keep `--forwarded-allow-ips=*` in the Dockerfile. Cloud Build builds amd64.
- Full gotchas: `../cc-stack/decisions/2026-06-10-gcp-deployment.md`.
