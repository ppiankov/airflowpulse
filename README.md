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
# Start exporter
airflowpulse serve

# Health check
airflowpulse doctor --format json

# One-shot status
airflowpulse status --format json

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
```

## Known Limitations

- Requires Airflow 2.0+ REST API (stable API)
- Basic auth only (OAuth2/LDAP not yet supported)
- Metadata DB collectors require direct DB access — not available in managed Airflow (MWAA, Cloud Composer) unless DB is exposed
- Dataset events collector requires Airflow 2.4+

## Roadmap

See work orders in workledger (project: airflowpulse).

## License

MIT
