# Lab: Deploy Apache Polaris with Helm on Kubernetes

Prerequisite of the Polaris lab. Polaris, an Iceberg REST catalog, is deployed on the Adaltas Kubernetes platform with a PostgreSQL database and a bootstrap Job that creates the catalog and the access control entities.

## What is deployed

| Component | Role |
|---|---|
| `polaris` (Deployment) | Iceberg REST catalog, API on port 8181, management and health on 8182 |
| `polaris-postgresql` (StatefulSet) | Metadata storage of the catalog (JDBC, schema `polaris_schema`) |
| `polaris-bootstrap` (Job, Helm hook) | Creates principals, principal roles, the catalog, catalog roles, grants and assignments, idempotently |
| `polaris-config` (ConfigMap), `polaris-credential` and `polaris-s3` (Secrets) | Settings, root secret (demo value of the lab), S3 keys used by Polaris |

## Project layout

| File | Content |
|---|---|
| `polaris/deployment/Chart.yaml` | Umbrella chart: Polaris 1.8.0 and PostgreSQL 18.12.15 as dependencies |
| `polaris/deployment/values.yaml` | PostgreSQL and Polaris settings, resource limits, S3 keys from the Secret, init container that bootstraps the realm |
| `polaris/deployment/templates/polaris-db-secret.yaml` | Database Secret with a generated password |
| `polaris/deployment/templates/bootstrap-job.yaml` | Post-install Job calling the Polaris management API |

## How to run

```bash
kubectl patch statefulset <vscode-pyspark statefulset> ...   # memory limit 2500Mi, restarts the pod
helm dependency update polaris/deployment
helm lint polaris/deployment
kubectl create configmap polaris-config ...                   # then the secrets polaris-credential and polaris-s3
helm upgrade --install polaris polaris/deployment --wait --timeout 8m
```

The chart archives in `polaris/deployment/charts/` are not versioned: run `helm dependency update` after a clone.

## Results

| Check | Observed result |
|---|---|
| Pods | `polaris` Running, `polaris-postgresql-0` Running, `polaris-bootstrap` Completed |
| Health | `http://polaris-mgmt:8182/q/health` returns `"status": "UP"` |
| Catalog | one catalog, type `INTERNAL`, default base location `s3://<bucket>/warehouse/` |
| Principals | `root`, `analyst`, `dev` |
| Principal roles | `service_admin`, `data_engineer`, `data_analyst` |
| Catalog roles | `catalog_admin`, `catalog_contributor`, `catalog_reader` |

Access control chain created by the Job: `dev` has `data_engineer`, which has `catalog_contributor` (manage access, metadata and content); `analyst` has `data_analyst`, which has `catalog_reader` (read data and properties).

## Notes

- The memory limit of the Onyxia service had to be raised to 2500Mi with `kubectl patch`, which restarts the pod and clears the shell environment.
- The credentials of `dev` and `analyst` are only returned when the principals are created. Helm recreates the hook Job at each upgrade, so the log of the final Job no longer contained them. They were regenerated with `POST /principals/{name}/reset` and stored in a file outside the repository (`~/.polaris-credentials.env`, mode 600), never committed.
- The `polaris-s3` Secret holds temporary STS keys valid about 12 hours. After that Polaris can no longer reach the bucket: recreate the Secret with fresh keys and restart the `polaris` Deployment.
- The first `helm upgrade --install` was interrupted, which explains the revisions 2 and 3: the release converged on its own.
