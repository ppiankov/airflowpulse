# airflowpulse

Heartbeat monitor for Apache Airflow — polls REST API, exposes Prometheus metrics, fires alerts.

## Install

```bash
brew install ppiankov/tap/airflowpulse
```

Or from source:

```bash
go install github.com/ppiankov/airflowpulse/cmd/airflowpulse@latest
```

## Commands

### serve

Start the poll loop and HTTP server exposing /metrics and /healthz.

**Flags:**
- `--stream` — output JSON-lines event stream to stdout alongside metrics server

**Exit codes:**
- 0: clean shutdown
- 1: startup failure

### doctor

Diagnose API connectivity, authentication, scheduler health, and endpoint permissions.

**Flags:**
- `--format json` — output as JSON (default: text)

**Exit codes:**
- 0: all checks pass
- 1: any check failed
- 2: warnings only

**JSON output:**
```json
{
  "status": "pass|warn|fail",
  "version": "string",
  "checks": [
    {
      "name": "string",
      "instance": "string",
      "status": "pass|warn|fail",
      "message": "string",
      "remediation": "string"
    }
  ],
  "provenance": { "field.path": "observed|declared|inferred|unknown" }
}
```

### status

One-shot health summary of all configured Airflow instances.

**Flags:**
- `--format json` — output as JSON (default: text)
- `--dag PATTERN` — filter DAGs by glob pattern
- `--state STATE` — filter by run state (comma-separated: failed,running)
- `--pool NAME` — filter pools by name
- `--watch` — continuously refresh
- `--interval DURATION` — watch refresh interval

**Exit codes:**
- 0: success
- 1: failure

**JSON output:**
```json
{
  "instances": [
    {
      "instance": "string",
      "scheduler": "healthy|unhealthy",
      "metadatabase": "healthy|unhealthy",
      "heartbeat_age": "string",
      "dag_runs": { "state": 0 },
      "pools": [
        { "name": "string", "used": 0, "queued": 0, "open": 0, "total": 0 }
      ],
      "import_errors": 0
    }
  ],
  "filters": { "dag": "string", "state": ["string"], "pool": "string" },
  "provenance": { "field.path": "observed|declared|inferred" }
}
```

### why

Investigate why a task is stuck, failed, or slow. Traces the root cause chain.

Usage: `airflowpulse why <dag_id> <task_id> [--run-id ID]`

**Flags:**
- `--format json` — output as JSON (default: text)
- `--run-id ID` — specific DAG run ID (default: latest)

**Exit codes:**
- 0: task healthy
- 1: problem found
- 2: partial results

**JSON output:**
```json
{
  "dag_id": "string",
  "task_id": "string",
  "run_id": "string",
  "instance": "string",
  "state": "string",
  "verdict": "healthy|warning|problem found",
  "chain": [
    {
      "check": "string",
      "status": "pass|warn|fail",
      "detail": "string",
      "remediation": "string"
    }
  ],
  "provenance": { "field.path": "observed|inferred" }
}
```

### pulse

Live TUI dashboard showing scheduler health, pools, DAG runs, and import errors.

Keybindings: q=quit, /=filter, Tab=switch instance, j/k=scroll

**Exit codes:**
- 0: clean exit

### history

Show last N runs of a task with duration trend, state, retries, and sparkline.

Usage: `airflowpulse history <dag_id> <task_id>`

**Flags:**
- `--format json` — output as JSON (default: text)
- `--runs N` — number of recent runs (default: 10)

**Exit codes:**
- 0: success
- 1: DAG or task not found

### diff

Compare current Airflow state against a previous snapshot.

**Flags:**
- `--format json` — output as JSON (default: text)

**Exit codes:**
- 0: success

### deps

Show task dependency graph for a DAG.

Usage: `airflowpulse deps <dag_id>`

**Flags:**
- `--format json` — output as JSON (default: text). Also supports `dot` for graphviz
- `--task TASK` — highlight a specific task

**Exit codes:**
- 0: success
- 1: DAG not found

### stream

Continuous JSON-lines event stream for AI agent consumption.

**Exit codes:**
- 0: clean exit

### init

Print default .env configuration with all supported environment variables.

**Exit codes:**
- 0: success

### version

Print airflowpulse version.

**Exit codes:**
- 0: success

## What this does NOT do

- Does NOT **execute** DAGs, tasks, or any Airflow operations — it observes, never acts
- Does NOT **store** metrics, logs, or state — it collects and exports to Prometheus; your stack stores
- Does NOT **replace** Airflow's built-in StatsD/Prometheus integration — it adds operational visibility the built-in metrics don't cover
- Does NOT **manage** Airflow configuration, connections, variables, or pools — read-only access only
- Does NOT **own** alerting delivery infrastructure — it fires webhooks; PagerDuty/Slack/OpsGenie routes them

## Parsing examples

```bash
# Get failed doctor checks
airflowpulse doctor --format json | jq '.checks[] | select(.status == "fail")'

# Count failed DAG runs
airflowpulse status --format json | jq '.instances[].dag_runs.failed // 0'

# Find exhausted pools
airflowpulse status --format json | jq '.instances[].pools[] | select(.open == 0)'

# Get root cause for stuck task
airflowpulse why etl_daily load_data --format json | jq '.chain[] | select(.status == "fail")'

# Task failure rate
airflowpulse history etl_daily extract --format json | jq '.stats.failure_rate'

# New failures since last check
airflowpulse diff --format json | jq '.new_failures[]'

# Filter critical events from stream
airflowpulse stream | jq 'select(.severity == "critical")'
```

## Handoffs

| Output | Next tool category | Example |
|--------|-------------------|---------|
| `/metrics` endpoint | Prometheus scraper | `prometheus` scrape config |
| `--format json` | JSON processor | `jq`, AI agent |
| Doctor fail verdict | Runbook automation | `airflow` CLI, kubectl |
| Webhook alert | Incident management | PagerDuty, OpsGenie |
| Grafana annotation | Dashboard | Grafana |
| `--format dot` | Graph renderer | `graphviz` |
| JSON-lines stream | Agent pipeline | `jq`, custom scripts |

## Failure Modes

| Scenario | Behavior | Fallback |
|----------|----------|----------|
| API unreachable | `airflow_up=0`, scrape error incremented | Retry with exponential backoff (3 attempts) |
| Auth failed (401/403) | Doctor reports `fail` for connectivity | Check AIRFLOW_API_USER and AIRFLOW_API_PASSWORD |
| Partial collector failure | Working collectors continue, failed ones log error | `airflow_scrape_errors_total` incremented per failure |
| Scheduler endpoint stale | Doctor/status report `warn` if heartbeat > 30s | Check scheduler process directly |
| Pool endpoint 403 | Pool metrics unavailable, other collectors unaffected | Grant API user pool read permissions |
| Dataset events 404 | Graceful degradation (Airflow <2.4) | No dataset metrics emitted |

## Provenance

JSON outputs include a `provenance` map declaring data lineage:
- `observed` — live value from Airflow REST API
- `declared` — from configuration or user annotation
- `inferred` — computed from other fields (e.g., heartbeat age)
- `unknown` — source unclear or stale

---

This tool follows the [Agent-Native CLI Convention](https://ancc.dev). Validate with: `ancc validate .`
