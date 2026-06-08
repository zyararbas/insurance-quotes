---
description: Deploy config — single source of truth for image/registry/ECS targets
---

Canonical deploy variables. The deploy phase commands read their values from HERE rather
than inlining them. Update infra coordinates in this file only.

## Image / registry
- IMAGE: `insurance-quotes`
- ECR_HOST: `889572107296.dkr.ecr.us-east-1.amazonaws.com`
- ECR_REPO: `889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app`
- REGION: `us-east-1`

## ECS rollout target
- CLUSTER: `SharedResourcesStack-coveragecompassaiclusterDAA01724-zSMoczm4Emoz`
- SERVICE: `InsuranceQuotesStack-InsuranceQuotesServiceCED71145-OYElk15TeQ6U`
- The cluster is SHARED with other services (coveragecompassai, insurance-graph). Always
  guard on the task def image (`insurance-quotes-app`) before acting on the service.
- The service task def points at the `:latest` tag, so `--force-new-deployment` re-pulls the
  freshly pushed image.

## App
- Import check module: `app.main`
- Container port: `8002`

## Conventions
- Build for `linux/amd64` (ECS runtime).
- Push `latest` only; keep the dated `yyyy-MM-dd` tag locally as a build record.
- zsh gotcha: use LITERAL `repo:tag` strings (not `$VAR:latest`; `:l` is a zsh modifier).
  If you must use a variable, brace it: `"${ECR_REPO}:latest"`.
