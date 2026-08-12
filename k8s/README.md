# k8s

Kubernetes manifests for the full stack: Kafka, `ingestion-service`,
`processing-service`, and `frontend`, deployed to a `pfm` namespace.

Design decisions: [docs/superpowers/specs/2026-08-11-k8s-design.md](../docs/superpowers/specs/2026-08-11-k8s-design.md).

Targets a local `kind` cluster only — images are built locally and loaded
directly into the cluster, no registry involved. (`minikube` may work too, but the
commands below — `kind load docker-image`, etc. — are `kind`-specific and untested
against `minikube`; the `minikube` equivalent is `minikube image load`.)

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
cluster — this is expected, not a failure. That wait loop has no timeout, so if it's
still `Init:0/1` after more than a minute or two, Kafka itself is likely broken — check
`kubectl logs -n pfm <pod> -c wait-for-topic` for the repeating "waiting for
future-transactions topic..." line versus signs Kafka never came up.

The topic name is defined once, in `k8s/01-topic-config.yaml`'s
`pfm-topic-config` ConfigMap, and referenced by `ingestion-service`,
`processing-service`, and this init container via a `PFM_TOPIC` env var —
not hardcoded per-manifest.

`ingestion-service` has no equivalent init container guarding it against a not-yet-ready
Kafka broker on a cold `kubectl apply -f k8s/`. This was deliberately not added: its
Kafka producer uses the client defaults (effectively-infinite retries bounded by a
120s `delivery.timeout.ms`) and the broker's default `auto.create.topics.enable=true`,
so a `POST /api/v1/ingest` that lands before Kafka's listener is accepting connections
just retries under the hood instead of failing. Verified by repeatedly killing both
the `kafka` and `ingestion-service` pods together on a fresh `kind` cluster and firing
`POST /api/v1/ingest` within ~1s of `ingestion-service` coming up (once via the `Service`,
once straight against the pod IP, bypassing the readiness gate) — every run published
all 717 records with no errors.

Trigger ingestion of the sample data:

```bash
kubectl port-forward -n pfm svc/ingestion-service 18081:8081 &
sleep 2
curl -X POST http://localhost:18081/api/v1/ingest
```

Then port-forward the frontend and open the report UI (a plain `kind create cluster` doesn't
publish NodePorts to the host, so `http://localhost:30080` isn't reachable directly without extra
kind configuration this project doesn't add):

```bash
kubectl port-forward -n pfm svc/frontend 30080:80 &
```

Open `http://localhost:30080`.

## Limitations

Scoped to a local kind demo only, not production-ready:

- Containers run as root — no `securityContext`/`runAsNonRoot` on any pod.
- No resource requests/limits on any container.
- Kafka runs on PLAINTEXT with no authentication and a hardcoded `CLUSTER_ID`.
- Kafka's data directory is an `emptyDir` — all topic data is lost on pod restart.

## Tearing down

```bash
kind delete cluster --name pfm
```
