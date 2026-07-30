---
description: GCP deploy phase 2 — smoke tests with a human approval gate
---

Phase 2 of the GCP release lifecycle. [HUMAN APPROVAL GATE]

1. Run the smoke tests: `python -m pytest tests/ -q` (or this repo's smoke subset).
   - If `pytest` isn't installed, do NOT silently install it — report and let the human decide.
2. Show the user the full result summary.
3. GATE — explicit human sign-off: "Do you approve the smoke tests as passing?" Proceed
   ONLY on "yes / approved". Never self-certify.

End with "Smoke: APPROVED" or "Smoke: NOT APPROVED (reason)".
