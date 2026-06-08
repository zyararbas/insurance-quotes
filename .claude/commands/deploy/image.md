---
description: Deploy phase 4 — docker build, tag, ECR login, push latest
---

Phase 4 of the release lifecycle. Builds and publishes the image to ECR.

First read the deploy variables (IMAGE, ECR_HOST, ECR_REPO, REGION + conventions) from
`.claude/commands/deploy/config.md` — that file is the single source of truth.

NOTE (zsh gotcha, per config): write image references as LITERAL `repo:tag` strings, not
`$VAR:latest`. Under zsh, `$ECR:latest` applies the `:l` parameter modifier and mangles the
tag (e.g. `insurance-quotes-appatest`). If you must use a variable, brace it: `"${ECR_REPO}:latest"`.

Steps:
a. Verify Docker is running. Build (linux/amd64 for ECS):
   `docker buildx build --platform linux/amd64 -t insurance-quotes .`
b. Tag `latest` and a dated tag (`DATE=$(date +%F)`, yyyy-MM-dd). The dated tag is kept
   locally as a build record; only `latest` is pushed.
   `docker tag insurance-quotes:latest 889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app:latest`
   `docker tag insurance-quotes:latest 889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app:<DATE>`
c. Login to ECR (needs valid AWS creds in the shell):
   `aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 889572107296.dkr.ecr.us-east-1.amazonaws.com`
   - If login fails (expired SSO/session token), STOP and ask the user to refresh AWS
     credentials, then retry this step only.
d. Push `latest` ONLY (do NOT push the dated tag). Confirm with the user once before pushing:
   `docker push 889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app:latest`

End with "Image: pushed latest (digest <sha>)".
