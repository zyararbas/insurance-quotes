Build the Docker image for linux/amd64, push it to AWS ECR us-east-1, and force a new deployment on the ECS service. Run each step sequentially and stop immediately if any step fails.

## Constants

| Variable | Value |
|---|---|
| REGION | `us-east-1` |
| ECR_REGISTRY | `889572107296.dkr.ecr.us-east-1.amazonaws.com` |
| ECR_IMAGE | `889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app:latest` |
| LOCAL_TAG | `insurance-quotes:latest` |
| CLUSTER | `SharedResourcesStack-coveragecompassaiclusterDAA01724-zSMoczm4Emoz` |
| SERVICE | `InsuranceQuotesStack-InsuranceQuotesServiceCED71145-OYElk15TeQ6U` |

## Steps

### 1. ECR Login
```bash
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 889572107296.dkr.ecr.us-east-1.amazonaws.com
```

### 2. Build (linux/amd64, no cache)
```bash
docker buildx build --no-cache --platform linux/amd64 -t insurance-quotes:latest .
```

### 3. Tag for ECR
```bash
docker tag insurance-quotes:latest 889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app:latest
```

### 4. Push to ECR
```bash
docker push 889572107296.dkr.ecr.us-east-1.amazonaws.com/insurance-quotes-app:latest
```

### 5. Force new ECS deployment
```bash
aws ecs update-service \
  --region us-east-1 \
  --cluster SharedResourcesStack-coveragecompassaiclusterDAA01724-zSMoczm4Emoz \
  --service InsuranceQuotesStack-InsuranceQuotesServiceCED71145-OYElk15TeQ6U \
  --force-new-deployment
```

### 6. Confirm deployment started
After the update-service call succeeds, report the service ARN, running/pending task counts, and deployment status from the JSON response so the user can confirm the rollout is in progress.
