---
description: GCP deploy phase 3 — git commit + push
---

Phase 3 of the GCP release lifecycle.

1. Stage changes with EXPLICIT paths — never `git add .env`, never blanket `git add -A`.
   Re-confirm `.env` is not staged. (Do NOT commit `vehicle_rates_chroma_db/` — it is gitignored.)
2. Commit with a concise message ending in:
   `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
3. Show the commit summary, then CONFIRM with the user before pushing.
4. `git push origin <current-branch>`.

End with "Git: pushed <short-sha> to <branch>".
