---
description: GCP release lifecycle orchestrator — runs the GCP deploy phases in order with gates
---

You are running the **GCP release lifecycle** for **insurance-quotes** (Cloud Build →
Artifact Registry → Cloud Run, driven by the shared stack tool in
`../cc-stack/gcp/`). Five phase commands; run in order, enforce the flow.

This deploys **only the insurance-quotes service** — not the whole stack.

Execute each phase via its slash command (Skill tool), in order. Treat every phase's
STOP/GATE rules as binding. Halt immediately on any NO-GO / NOT APPROVED / failure.

Track progress with TodoWrite, one todo per phase.

1. `/gcp:deploygcp:preflight` — DEV checks (git, `.env` guard, gcloud auth, import, Chroma DB present). GO only.
2. `/gcp:deploygcp:smoke` — smoke tests + human approval gate. APPROVED only.
3. `/gcp:deploygcp:git` — commit + push (confirm).
4. `/gcp:deploygcp:image` — Cloud Build → push `latest` to Artifact Registry (confirm). HEAVY build.
5. `/gcp:deploygcp:release` — Cloud Run rollout (approval-gated).

Rules:
- Don't skip/reorder; don't self-approve smoke/release; never stage/push `.env`.

### Close out
Summarize: commit hash, image digest, and the new Cloud Run revision.

Individual phases run standalone, e.g. `/gcp:deploygcp:image` or `/gcp:deploygcp:release`.

Infra coordinates live in [`deploygcp/config.md`](deploygcp/config.md) — the single
source of truth (no runnable `:config`).
