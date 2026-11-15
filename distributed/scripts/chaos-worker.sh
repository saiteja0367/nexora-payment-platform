#!/usr/bin/env bash
set -euo pipefail
# Demonstrates worker process failure and Kubernetes Deployment restart.
# Requires a working cluster and kubectl context pointing at the lab namespace.
kubectl get pods -l app=nexora-worker
POD=$(kubectl get pods -l app=nexora-worker -o jsonpath='{.items[0].metadata.name}')
echo "Deleting worker pod: $POD"
kubectl delete pod "$POD"
kubectl rollout status deployment/nexora-worker --timeout=120s
kubectl get pods -l app=nexora-worker
