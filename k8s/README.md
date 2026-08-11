# k8s

Kubernetes manifests for the full stack: Kafka, `ingestion-service`,
`processing-service`, and `frontend`, deployed to a `pfm` namespace.

Design decisions: [docs/superpowers/specs/2026-08-11-k8s-design.md](../docs/superpowers/specs/2026-08-11-k8s-design.md).

Targets a local `kind`/`minikube` cluster only — images are built locally and loaded
directly into the cluster, no registry involved.

## Running locally (kind)

From the repo root:

```bash
docker build -f ingestion-service/Dockerfile -t pfm/ingestion-service:local .
docker build -f processing-service/Dockerfile -t pfm/processing-service:local .
docker build -t pfm/frontend:local frontend/

kind create cluster --name pfm
kind load docker-image pfm/ingestion-service:local --name pfm
kind load docker-image pfm/processing-service:local --name pfm
kind load docker-image pfm/frontend:local --name pfm

kubectl apply -f k8s/
```

`processing-service`'s pod waits (via an init container) until `ingestion-service` has
created the `future-transactions` topic, so it may show `Init:0/1` briefly on a fresh
cluster — this is expected, not a failure.

Trigger ingestion of the sample data:

```bash
kubectl port-forward -n pfm svc/ingestion-service 18081:8081 &
curl -X POST http://localhost:18081/api/ingest
```

Then port-forward the frontend and open the report UI (a plain `kind create cluster` doesn't
publish NodePorts to the host, so `http://localhost:30080` isn't reachable directly without extra
kind configuration this project doesn't add):

```bash
kubectl port-forward -n pfm svc/frontend 30080:80 &
```

Open `http://localhost:30080`.

## Tearing down

```bash
kind delete cluster --name pfm
```
