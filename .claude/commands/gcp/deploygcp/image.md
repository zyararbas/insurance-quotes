---
description: GCP deploy phase 4 — build on Cloud Build + push to Artifact Registry (heavy)
---

Phase 4 of the GCP release lifecycle. Builds and publishes the image.

Read DEPLOY_TARGET/IMAGE from `gcp/deploygcp/config.md`. No local docker, no registry
login — the build runs on **Cloud Build** (amd64) and pushes `:latest` to Artifact Registry.

**HEAVY build (~5 min):** the context upload is ~282 MB (it includes the baked
`vehicle_rates_chroma_db/`), and the build downloads torch + the embedding model. Expect
it to run a while; the stack uses a 30-min Cloud Build timeout.

Confirm once before running (this pushes `:latest`):
```bash
cd ../cc-stack/gcp
set -a; source ./env.local.sh; set +a
python3 -c "from config import Config; from gcputil import Gcloud; from stacks.services import ImagesStack; c=Config(); g=Gcloud(c.project_id); ImagesStack(c,g,targets=['quotes']).deploy()"
```
- Verify the context size line shows ~282 MB (Chroma included). If it's tiny (<1 MB),
  `.gcloudignore` is dropping the Chroma DB — STOP and fix before the build bakes an empty store.
- If `gcloud` auth is expired, STOP and ask the user to refresh, then retry.

End with "Image: built + pushed latest (digest <sha>)".
