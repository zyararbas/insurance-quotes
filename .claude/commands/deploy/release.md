---
description: Deploy phase 5 — ECS rollout (approval-gated force-deploy, or manual)
---

Phase 5 of the release lifecycle. [APPROVAL GATE]

First read the ECS target (CLUSTER, SERVICE, REGION) from
`.claude/commands/deploy/config.md` — that file is the single source of truth. Force-deploy
pulls `:latest` because the task def points at the `latest` tag.
(Requires valid AWS creds exported in the shell. If the account/cluster/service changes,
update config.md — rediscover with `aws ecs list-clusters` / `list-services` if needed.)

GATE: ask the user, "Run the automated ECS force-deploy now, or deploy manually?" Proceed to
5b ONLY on an explicit "yes / run it". Otherwise do 5c. Never auto-deploy without approval.

### 5b — Automated force-deploy (after approval)
1. Guard: confirm the service's task def image contains `insurance-quotes-app` before acting
   (the cluster is SHARED with other services):
   `aws ecs describe-task-definition --task-definition <td> --region us-east-1 --query 'taskDefinition.containerDefinitions[].image'`
2. `aws ecs update-service --cluster <CLUSTER> --service <SERVICE> --force-new-deployment --region us-east-1`
3. `aws ecs wait services-stable --cluster <CLUSTER> --services <SERVICE> --region us-east-1`
4. Verify: `aws ecs describe-services --cluster <CLUSTER> --services <SERVICE> --region us-east-1 --query 'services[0].{running:runningCount,desired:desiredCount,rollout:deployments[0].rolloutState}'`
   — require `rollout=COMPLETED` and `running==desired`.
5. If `wait` times out or rollout != COMPLETED, inspect `describe-services ... --query 'services[0].events[:5]'`
   and report; do NOT declare success.

### 5c — Manual (if not approved)
Tell the user: ECS console → cluster → the InsuranceQuotes service → Update service →
check "Force new deployment" → Update (or run the 5b CLI themselves). Wait for them to
confirm it is live.

End with "Release: <COMPLETED running N/N | MANUAL handed off>".
