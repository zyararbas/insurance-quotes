---
description: Deploy phase 1 — DEV preflight (git status, .env guard, import check)
---

Phase 1 of the release lifecycle. Safe, read-only checks. Report a clear GO / NO-GO.
If anything fails, STOP and report — do not continue to later phases.

1. `git status --short` — summarize exactly what would ship (or note a clean tree).
2. Guard secrets: confirm `.env` is NOT staged and won't be committed. If it appears in
   the staged set, STOP and warn loudly.
3. Sanity-import the app so we don't ship an import error: `python -c "import app.main"`.
   If it raises, STOP and report the traceback.

End with an explicit "Preflight: GO" or "Preflight: NO-GO (reason)".
