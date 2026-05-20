# Fraud Scorer Pipeline — VAST DataEngine Deployment Guide

## Overview

This guide documents the end-to-end deployment of the `fraud-scorer-pipeline` on VAST DataEngine.
The pipeline scores financial transactions for fraud in real time using an element trigger that
watches an S3 bucket for new objects, invokes the fraud scorer function, and publishes results
to Kafka topics.

```
S3 Bucket (yg-de-source-bucket)
        ↓  [ObjectCreated:* event]
  fraudrawtrig (Element Trigger)
        ↓  [fraudtransactionsraw topic]
  fraud-scorer (DataEngine Function)
        ↓                    ↓
fraud.transactions.scored  fraud.alerts
     (all txns)           (score ≥ 0.8)
```

---

## Prerequisites

- VAST DataEngine CLI (`vastde`) installed and configured
- Docker installed (version 26.x recommended — see Docker version note below)
- Docker Hub account
- Access to VAST cluster admin UI
- `kubectl` configured for the DataEngine Kubernetes cluster

---

## Environment

| Component | Value |
|-----------|-------|
| VAST Cluster | `var204.selab.vastdata.com` |
| Tenant | `yg-tenant` |
| Kubernetes Cluster VRN | `vast:dataengine:kubernetes-clusters:wb-dataengine` |
| Container Registry | `dockerhub-de` (Docker Hub) |
| Docker Hub Username | `malingod` |
| S3 Source Bucket | `yg-de-source-bucket` |
| Kafka Broker VIP | `172.200.204.134:9092` (see VIP pool note below) |
| Kafka Topics | `fraudtransactionsraw`, `fraud.transactions.scored`, `fraud.alerts` |

---

## Known Issues & Workarounds

### Docker Version
`vastde` v5.5.0-dev embeds a Docker SDK at API 1.38. Docker 27+ enforces a minimum of API 1.40,
causing build failures. **Pin Docker to 26.x:**

```bash
sudo apt install -y \
  docker-ce=5:26.1.4-1~ubuntu.24.04~noble \
  docker-ce-cli=5:26.1.4-1~ubuntu.24.04~noble
sudo apt-mark hold docker-ce docker-ce-cli
```

### VIP Pool CNode Assignment
The Kafka VIP pool (`yg-vipool`) must have CNodes assigned or the broker will be unreachable.
If you see `no route to host` errors, assign CNodes via the VAST API:

```bash
curl -sk -X PATCH https://var204.selab.vastdata.com/api/latest/vippools/20/ \
  -u 'admin:PASSWORD' \
  -H 'Content-Type: application/json' \
  -d '{"cnode_ids": [4, 2, 3]}'
```

### Kafka Topics with Dots
`vastde` CLI rejects topic names with dots (`.`) in some contexts due to name validation.
Create topics via the VAST Admin UI under:
`VAST Database → yg-bucket → Kafka-Compatible Broker Topics`

---

## Step-by-Step Deployment

### Step 1 — Initialize the Function

```bash
mkdir -p ~/functions/fraud-scorer
cd ~/functions/fraud-scorer

vastde functions init python-pip fraud-scorer \
  --target ~/functions/fraud-scorer \
  --handlers main.py

# Copy source files into scaffold
cp /path/to/VAST-Data-Streaming/demos/vast/fraud_scorer_pipeline/main.py \
   ~/functions/fraud-scorer/fraud-scorer/
cp /path/to/VAST-Data-Streaming/demos/vast/fraud_scorer_pipeline/requirements.txt \
   ~/functions/fraud-scorer/fraud-scorer/
```

### Step 2 — Build the Function

```bash
cd ~/functions/fraud-scorer/fraud-scorer
vastde functions build fraud-scorer
```

### Step 3 — Push to Docker Hub

```bash
docker tag fraud-scorer:latest malingod/fraud-scorer:v1.0
docker login
docker push malingod/fraud-scorer:v1.0
```

### Step 4 — Create the Function in VAST DataEngine

```bash
vastde functions create \
  --name fraud-scorer \
  --container-registry dockerhub-de \
  --artifact-source malingod/fraud-scorer \
  --image-tag v1.0 \
  --description "Real-time fraud detection scoring function" \
  --publish
```

### Step 5 — Create Kafka Topics

Create via VAST Admin UI:
`VAST Database → yg-bucket → Kafka-Compatible Broker Topics → Add`

Topics needed:
- `fraudtransactionsraw`
- `fraud.transactions.scored`
- `fraud.alerts`
- `fraud.metrics`

### Step 6 — Create the Element Trigger

```bash
vastde triggers create element \
  --name fraudrawtrig \
  --source-bucket yg-de-source-bucket \
  --event ObjectCreated:* \
  --topic-name fraudtransactionsraw \
  --broker-type Internal \
  --broker-name yg-bucket
```

### Step 7 — Create the Pipeline

```bash
vastde pipelines create --config @pipeline-manifest.yaml
```

Where `pipeline-manifest.yaml` contains:

```yaml
name: fraud-scorer-pipeline
description: Real-time fraud detection scoring pipeline
namespace: default
kubernetes_cluster_vrn: vast:dataengine:kubernetes-clusters:wb-dataengine
manifest:
  config:
    environment_variables:
      - name: KAFKA_BOOTSTRAP_SERVERS
        value: "172.200.204.134:9092"
  triggers:
    - name: fraudrawtrig
      vrn: vast:dataengine:triggers:fraudrawtrig
  function_deployments:
    - name: fraud-scorer-2
      function_vrn: vast:dataengine:functions:fraud-scorer
      revision: 1
      config:
        log_level: INFO
        timeout: 60
        resources:
          min_concurrency: 1
          max_concurrency: 10
          min_cpu: 100m
          max_cpu: 500m
          min_memory: 128Mi
          max_memory: 256Mi
  links:
    - source:
        - fraudrawtrig
      destination:
        - fraud-scorer-2
      topic: vast:dataengine:topics:yg-bucket/fraudtransactionsraw
      config:
        retries: 3
        events_order: unordered
```

### Step 8 — Deploy

```bash
vastde pipelines deploy fraud-scorer-pipeline
```

### Step 9 — Verify

```bash
# Check pipeline status
vastde pipelines get fraud-scorer-pipeline -o json

# Check function pod logs
kubectl get pods | grep fraud-scorer
kubectl logs <pod-name>

# Watch for Running status
watch -n 5 'vastde pipelines get fraud-scorer-pipeline -o json | grep -E "status|reason"'
```

---

## Pipeline Architecture Notes

### Trigger → Function Flow
1. File written to `yg-de-source-bucket` fires `ObjectCreated:*` event
2. Event published to `fraudtransactionsraw` Kafka topic
3. `fraud-scorer` function consumes the event
4. Function scores transaction using 5 weighted rules:
   - Velocity (0.25 weight)
   - Geographic impossibility (0.30 weight)
   - Amount anomaly (0.20 weight)
   - Card testing (0.15 weight)
   - Fraud ring merchants (0.10 weight)
5. Scored transaction published to `fraud.transactions.scored`
6. If `risk_score ≥ 0.8` → alert published to `fraud.alerts`

### Visual Builder vs CLI
VAST DataEngine pipelines **must be built in the Visual Builder** before deploying via CLI.
Deploying an empty Draft (no trigger-function connections) results in `RevisionMissing` errors.

Use the Visual Builder at:
`https://var204.selab.vastdata.com/dataengine/#/pipelines/<PIPELINE_GUID>/builder`

---

## Monitoring

```bash
# Live function logs
vastde logs --pipeline fraud-scorer-pipeline

# Kafka topic activity
kubectl exec -n kafka -it my-cluster-kafka-0 -- \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic fraud.transactions.scored \
  --from-beginning

# Pipeline traces
vastde traces --pipeline fraud-scorer-pipeline
```