# airflowpulse

A heartbeat monitor for Apache Airflow — polls REST API and metadata DB, exposes Prometheus metrics, fires alerts.

## What airflowpulse is

A lightweight, single-binary Prometheus exporter that polls Airflow's REST API for scheduler health, DAG run states, task instances, pool utilization, and import errors. Optionally connects to the metadata database for SLA misses, XCom bloat, and executor health. One deployment per Airflow instance, multi-instance support, Helm chart included.

## What airflowpulse is NOT

- Not a DAG scheduler or executor — it observes Airflow, it does not run tasks
- Not a replacement for Airflow's built-in StatsD/Prometheus integration — it adds operational visibility the built-in metrics don't cover (stuck tasks, pool exhaustion, SLA misses, import errors)
- Not a log aggregator — it reads API responses, not task logs
- Not a replacement for Prometheus or Grafana — it collects and exports, your stack stores and visualizes
- Not a configuration manager — it reads Airflow state, it does not modify DAGs, connections, or variables

## Philosophy

Observe the orchestrator, don't become one. Present evidence, let operators decide. One binary, zero runtime dependencies. REST API first, metadata DB optional for deeper metrics.

## Quick Start

```bash
brew install ppiankov/tap/airflowpulse

export AIRFLOW_API_URL=http://localhost:8080/api/v1
export AIRFLOW_API_USER=admin
export AIRFLOW_API_PASSWORD=admin
airflowpulse serve
```

## Usage

```bash
# Start exporter (poll loop + /metrics + /healthz)
airflowpulse serve

# Start exporter with JSON event stream on stdout
airflowpulse serve --stream

# Diagnose connectivity, auth, scheduler, endpoints
airflowpulse doctor --format json

# One-shot cluster health summary
airflowpulse status --format json

# Filter status by DAG pattern, state, or pool
airflowpulse status --dag '*etl*' --state failed --pool default

# Continuous status refresh
airflowpulse status --watch

# Investigate why a task is stuck
airflowpulse why etl_daily load_data --format json

# Live TUI dashboard (htop for Airflow)
airflowpulse pulse

# Task run history with duration trend
airflowpulse history etl_daily extract_data --runs 20

# What changed since last check
airflowpulse diff --format json

# Task dependency graph (ASCII, JSON, or DOT)
airflowpulse deps etl_daily --format dot | dot -Tpng -o deps.png

# Continuous JSON event stream for agents/scripts
airflowpulse stream | jq 'select(.severity == "critical")'

# Print default config
airflowpulse init > .env

# Multi-instance
export AIRFLOW_API_URL="http://airflow-prod:8080/api/v1,http://airflow-staging:8080/api/v1"
airflowpulse serve

# With metadata DB (enables SLA, XCom, executor collectors)
export AIRFLOW_METADATA_DSN="<your-metadata-db-dsn>"
airflowpulse serve
```

## Architecture

```
Airflow REST API → airflowpulse (poll loop) → /metrics → Prometheus → Grafana
Metadata DB (optional) ↗                    → /healthz
                                            → Telegram/webhook alerts
                                            → Grafana annotations
                                            → JSON event stream (stdout)
                                            → TUI dashboard (pulse)
```

## Kubernetes Deployment

```bash
helm upgrade --install airflowpulse charts/airflowpulse \
  -n airflowpulse-system --create-namespace \
  --set airflow.apiURL="http://airflow-webserver.airflow.svc.cluster.local:8080/api/v1" \
  --set airflow.apiUser="admin" \
  --set airflow.apiPassword="<password>" \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release=prometheus-operator \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=prometheus-operator \
  --set prometheusRule.additionalRuleLabels.team=<your-team> \
  --set image.tag=v0.1.1
```

### Prerequisites

**API URL format:** must include the port (typically `:8080`) and the `/api/v1` path suffix. The service name depends on your Airflow deployment — check with `kubectl get svc -n airflow`.

**Airflow API auth backend:** Airflow defaults to `session` auth which only works for browser sessions, not API clients. You must enable basic auth:

```yaml
# In your Airflow Helm values:
config:
  api:
    auth_backends: "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
```

Or via environment variable on the Airflow webserver deployment:

```bash
kubectl set env deploy/<airflow-web> -n airflow \
  AIRFLOW__API__AUTH_BACKENDS="airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
```

**Dedicated API user (recommended):** create a read-only service account instead of using the admin user:

```bash
kubectl exec -n airflow -it deploy/<airflow-web> -c <container> -- \
  airflow users create \
    --username airflowpulse \
    --password "<password>" \
    --role Admin \
    --firstname airflowpulse \
    --lastname bot \
    --email airflowpulse@local
```

### Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `airflow_up == 0`, logs show connection refused | Wrong API URL or port | Check URL includes `:8080/api/v1` |
| `403 Forbidden` on all endpoints | Auth backend is `session` only | Enable `basic_auth` backend (see above) |
| `Login Failed` in Airflow logs | Wrong password in Kubernetes secret | Verify with `kubectl get secret -n airflowpulse-system airflowpulse -o jsonpath='{.data.airflow-api-password}' \| base64 -d` |
| `instance` label shows pod IP | Prometheus overwrites exporter labels | ServiceMonitor needs `honorLabels: true` (included by default) |

## Known Limitations

- Requires Airflow 2.0+ REST API (stable API)
- Basic auth only (OAuth2/LDAP not yet supported)
- Metadata DB collectors require direct DB access — not available in managed Airflow (MWAA, Cloud Composer) unless DB is exposed
- Dataset events collector requires Airflow 2.4+

## Roadmap

See work orders in workledger (project: airflowpulse).

## License

[MIT](LICENSE)
