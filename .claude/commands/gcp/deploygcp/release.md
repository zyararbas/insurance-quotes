---
description: GCP deploy phase 5 — Cloud Run rollout (approval-gated)
---

Phase 5 of the GCP release lifecycle. [APPROVAL GATE]

Read the target (SERVICE `insurance-quotes`, DEPLOY_TARGET `quotes`) from
`gcp/deploygcp/config.md`. Release deploys a new revision serving the `:latest` image.

GATE: ask "Roll out insurance-quotes to Cloud Run now?" Proceed ONLY on explicit
"yes / run it".

### Rollout (after approval)
```bash
cd ../cc-stack/gcp
set -a; source ./env.local.sh; set +a
python3 deploy.py quotes
```

### Verify (internal ingress — no public health endpoint)
```bash
gcloud run services describe insurance-quotes --region "$REGION" \
  --format='value(status.latestReadyRevisionName, status.url)'
gcloud run services logs read insurance-quotes --region "$REGION" --limit 20
```
Confirm the new revision is Ready and the logs show it using the **local** Chroma store
(`[Chroma] Using local store`), not an S3 download. Do NOT browser-test the internal URL.

End with "Release: <revision> serving | MANUAL handed off".
