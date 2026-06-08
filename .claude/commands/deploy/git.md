---
description: Deploy phase 3 — git commit + push
---

Phase 3 of the release lifecycle.

1. Stage changes with EXPLICIT paths — never `git add .env`, never a blanket `git add -A`
   without listing what it includes. Re-confirm `.env` is not among the staged files.
2. Commit with a concise, descriptive message. End the message with:
   `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
3. Show the commit summary, then CONFIRM with the user before pushing.
4. Push to the current branch: `git push origin <current-branch>` (use the actual branch).

End with "Git: pushed <short-sha> to <branch>".
