---
description: Release lifecycle orchestrator — runs the deploy phases in order with gates
---

You are running the **release lifecycle** for the insurance-quotes service. It is composed
of five phase commands; this orchestrator runs them in order and enforces the flow.

Execute each phase by invoking its slash command (via the Skill tool), in this order. Treat
every phase's own STOP/GATE rules as binding. If any phase reports NO-GO / NOT APPROVED /
a failure, **halt the lifecycle immediately** and report — do not run later phases.

Track progress with TodoWrite, one todo per phase.

1. `/deploy:preflight` — DEV checks (git status, `.env` guard, import). Continue only on GO.
2. `/deploy:smoke` — smoke tests + human approval gate. Continue only on APPROVED.
3. `/deploy:git` — commit + push (confirm before push).
4. `/deploy:image` — docker build, tag, ECR login, push `latest` (confirm before push).
5. `/deploy:release` — ECS rollout (approval-gated automated force-deploy, or manual).

Rules:
- Do not skip a phase or reorder them.
- Do not self-approve the smoke gate or the release gate — those require explicit human "yes".
- Never stage or push `.env`.
- The ECS cluster is SHARED — the release phase must guard on the `insurance-quotes-app`
  image before touching the service.

### Close out
When phase 5 finishes, summarize what shipped: commit hash, image tag(s) + digest, and the
deployment result (rollout state + running/desired, or "manual handoff").

Individual phases can also be run standalone, e.g. `/deploy:image` to rebuild and push without
re-running tests, or `/deploy:release` to roll out an already-pushed image.

Infra coordinates (ECR repo, ECS cluster/service, region) live in
`.claude/commands/deploy/config.md` — the single source of truth the phases read from. There
is no `/deploy:config` to run; it is a reference file.
