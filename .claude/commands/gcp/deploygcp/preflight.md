---
description: GCP deploy phase 1 — preflight (git, .env guard, gcloud auth, import, Chroma DB present)
---

Phase 1 of the GCP release lifecycle. Read-only checks. Report GO / NO-GO; STOP on failure.

1. `git status --short` — summarize what would ship.
2. Guard secrets: `.env` not staged. STOP loudly if it is.
3. `gcloud` authenticated + project set:
   `gcloud auth list --filter=status:ACTIVE --format='value(account)'` (non-empty).
   If "Reauthentication failed", STOP and ask the user to `gcloud auth login`.
4. Stack env present: `../cc-stack/gcp/env.local.sh`. STOP if missing.
5. Sanity import: `python -c "import app.main"`. STOP on traceback.
6. **Chroma DB present** — it is gitignored but baked into the image:
   `test -f vehicle_rates_chroma_db/chroma.sqlite3` must succeed. STOP if missing (the
   build would fail / fall back to S3). Also confirm `.gcloudignore` does NOT exclude it.

End with "Preflight: GO" or "Preflight: NO-GO (reason)".
