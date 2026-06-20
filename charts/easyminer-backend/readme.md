# Easyminer Backend Helm Chart

Deploys the EasyMiner Backend (FastAPI API + Celery worker) to Kubernetes.

**Deployed resources:** API Deployment, Worker Deployment, bundled Redis (optional), ConfigMap, Secret, Service, ServiceAccount, and optionally an Ingress and HPAs for the API and worker.

> **Note:** MariaDB is not bundled. You must provide `DATABASE_URL` and `DATABASE_URL_SYNC` environment variables via an external Secret or init container and mount them into the pods manually, or patch the deployment after install.

## Prerequisites

- Helm
- `kubectl` configured against your cluster
- A running MariaDB instance accessible from the cluster
- A running EasyMiner Center instance (or omit `config.easyminerCenterUrl` to use the default, which only works in local dev)

## Installation

```sh
helm install easyminer-backend ./charts/easyminer-backend \
  --set config.easyminerCenterUrl=https://center.example.com
```

To build and use local images instead of pulling from GHCR:

```sh
just build-docker
helm install easyminer-backend ./charts/easyminer-backend \
  --set api.image.repository=easyminer-backend \
  --set api.image.pullPolicy=Never \
  --set worker.image.repository=easyminer-backend-worker \
  --set worker.image.pullPolicy=Never \
  --set config.easyminerCenterUrl=https://center.example.com
```

## Accessing the API

With the default `ClusterIP` service type, use port-forward:

```sh
kubectl port-forward svc/easyminer-backend-api 80:80
# API available at http://localhost:80
```

To expose via Ingress:

```sh
helm upgrade easyminer-backend ./charts/easyminer-backend \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=easyminer.example.com \
  --set ingress.hosts[0].paths[0].path=/ \
  --set ingress.hosts[0].paths[0].pathType=Prefix
```

## Configuration

| Key | Default | Description |
|---|---|---|
| `config.easyminerCenterUrl` | `http://localhost:8001` | URL of EasyMiner Center |
| `config.modules` | `data,preprocessing,miner` | Comma-separated list of enabled API modules |
| `config.storageBackend` | `disk` | Storage backend: `disk` or `s3` |
| `config.s3.bucket` | `""` | S3 bucket name |
| `config.s3.endpointUrl` | `""` | S3 endpoint URL (for non-AWS providers) |
| `config.s3.region` | `us-east-1` | S3 region |
| `config.s3.prefix` | `""` | Key prefix for S3 objects |
| `secrets.s3AccessKey` | `""` | S3 access key (stored in a K8s Secret) |
| `secrets.s3SecretKey` | `""` | S3 secret key (stored in a K8s Secret) |
| `redis.enabled` | `true` | Deploy bundled Redis |
| `externalRedis.brokerUrl` | `""` | Celery broker URL — required when `redis.enabled=false` |
| `externalRedis.backendUrl` | `""` | Celery backend URL — required when `redis.enabled=false` |
| `api.replicaCount` | `1` | Number of API replicas |
| `api.image.repository` | `ghcr.io/kizi/easyminer-backend` | API image |
| `api.image.tag` | `latest` | API image tag |
| `api.autoscaling.enabled` | `false` | Enable HPA for the API |
| `api.autoscaling.minReplicas` | `1` | HPA min replicas |
| `api.autoscaling.maxReplicas` | `5` | HPA max replicas |
| `worker.replicaCount` | `1` | Number of worker replicas |
| `worker.image.repository` | `ghcr.io/kizi/easyminer-backend-worker` | Worker image |
| `worker.image.tag` | `latest` | Worker image tag |
| `worker.autoscaling.enabled` | `false` | Enable HPA for the worker |
| `worker.autoscaling.minReplicas` | `1` | HPA min replicas |
| `worker.autoscaling.maxReplicas` | `10` | HPA max replicas |
| `ingress.enabled` | `false` | Create an Ingress resource |
| `service.type` | `ClusterIP` | Service type |
| `service.port` | `80` | Service port |
