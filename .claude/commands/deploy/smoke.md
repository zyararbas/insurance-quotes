---
description: Deploy phase 2 — smoke tests with a human approval gate
---

Phase 2 of the release lifecycle. [HUMAN APPROVAL GATE]

1. Run the smoke tests: `python -m pytest -q`
   - This repo may not have a `tests/` suite yet. If there are no tests, do NOT fabricate a
     pass — report "no automated tests" and fall back to a manual sanity check (e.g. import
     the app and exercise a representative quote calculation), then rely on the gate below.
   - If `pytest` is not installed, do NOT silently install it — report that and let the
     human decide (install, use another command, or manually approve).
2. Show the user the full result summary (pass/fail counts, failures, warnings) — or the
   manual sanity-check result if there are no tests.
3. GATE — require explicit human sign-off: ask directly, "Do you approve the smoke tests as
   passing?" The human makes the pass/fail call, not the automated exit code. Proceed ONLY on
   an explicit "yes / approved". Anything else (silence, "looks off", a question) is NOT
   approved — stop and address it. Never self-certify the smoke tests.

End with "Smoke: APPROVED" or "Smoke: NOT APPROVED (reason)".
