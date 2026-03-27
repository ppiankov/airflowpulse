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

### `airflowpulse serve`

Start the poll loop and HTTP server exposing /metrics and /healthz.

**Flags:** none (configured via environment variables)

**Exit codes:** 0 = clean shutdown, 1 = startup failure

**JSON output:** N/A (long-running server)

---

### `airflowpulse doctor [--format text|json]`

Diagnose API connectivity, authentication, scheduler health, and endpoint permissions.

**Flags:**
- `--format text|json` — output format (default: text)

**Exit codes:** 0 = all checks pass, 1 = any check failed, 2 = warnings only

**JSON schema:**
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
      "remediation": "string (optional)"
    }
  ],
  "provenance": { "field.path": "observed|declared|inferred|unknown" }
}
```

**Parsing example:**
```bash
airflowpulse doctor --format json | jq '.checks[] | select(.status == "fail")'
```

---

### `airflowpulse status [--format text|json] [--dag PATTERN] [--state STATE] [--pool NAME] [--watch]`

One-shot health summary of all configured Airflow instances.

**Flags:**
- `--format text|json` — output format (default: text)
- `--dag PATTERN` — filter DAGs by glob pattern
- `--state STATE` — filter by run state (comma-separated: failed,running)
- `--pool NAME` — filter pools by name
- `--watch, -w` — continuously refresh
- `--interval DURATION` — watch refresh interval

**Exit codes:** 0 = success, 1 = failure

**JSON schema:**
```json
{
  "instances": [
    {
      "instance": "string",
      "scheduler": "healthy|unhealthy",
      "metadatabase": "healthy|unhealthy",
      "heartbeat_age": "string",
      "dag_runs": { "state": "count" },
      "pools": [
        { "name": "string", "used": 0, "queued": 0, "open": 0, "total": 0 }
      ],
      "import_errors": 0
    }
  ],
  "filters": { "dag": "string", "state": ["string"], "pool": "string" }
}
```

**Parsing examples:**
```bash
# Failed DAG runs
airflowpulse status --format json | jq '.instances[].dag_runs.failed // 0'

# Pools with zero open slots
airflowpulse status --format json | jq '.instances[].pools[] | select(.open == 0)'
```

---

### `airflowpulse why <dag_id> <task_id> [--run-id ID] [--format text|json]`

Investigate why a task is stuck, failed, or slow. Traces the root cause chain: pool capacity, scheduler health, upstream dependencies.

**Flags:**
- `--format text|json` — output format (default: text)
- `--run-id ID` — specific DAG run ID (default: latest)

**Exit codes:** 0 = task healthy, 1 = problem found, 2 = partial (some checks failed)

**JSON schema:**
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
      "remediation": "string (optional)"
    }
  ]
}
```

**Parsing example:**
```bash
airflowpulse why etl_daily load_data --format json | jq '.chain[] | select(.status == "fail")'
```

---

### `airflowpulse pulse`

Live TUI dashboard. Full-screen terminal view showing scheduler health, pools, DAG runs, and import errors.

**Keybindings:** q=quit, /=filter, Tab=switch instance, j/k=scroll

**Exit codes:** 0 = clean exit

---

### `airflowpulse history <dag_id> <task_id> [--runs N] [--format text|json]`

Show last N runs of a task with duration trend, state, retries, and sparkline.

**Flags:**
- `--format text|json` — output format (default: text)
- `--runs N` — number of recent runs (default: 10)

**Exit codes:** 0 = success, 1 = DAG/task not found

**Parsing example:**
```bash
airflowpulse history etl_daily load_data --format json | jq '.stats.failure_rate'
```

---

### `airflowpulse diff [--since DURATION] [--format text|json]`

Compare current Airflow state against a previous snapshot. Shows new failures, recoveries, pool changes.

**Flags:**
- `--format text|json` — output format (default: text)

**Exit codes:** 0 = success (no changes or changes detected)

**Parsing example:**
```bash
airflowpulse diff --format json | jq '.new_failures[]'
```

---

### `airflowpulse deps <dag_id> [--task TASK] [--format text|json|dot]`

Show task dependency graph for a DAG in ASCII, JSON, or DOT format.

**Flags:**
- `--format text|json|dot` — output format (default: text)
- `--task TASK` — highlight a specific task

**Exit codes:** 0 = success, 1 = DAG not found

**Parsing example:**
```bash
airflowpulse deps etl_daily --format dot | dot -Tpng -o deps.png
```

---

### `airflowpulse stream`

Continuous JSON-lines event stream for AI agent consumption. Emits state change events.

**Exit codes:** 0 = clean exit (Ctrl+C)

**Parsing example:**
```bash
airflowpulse stream | jq 'select(.severity == "critical")'
```

---

### `airflowpulse init`

Print default .env configuration with all supported environment variables.

**Exit codes:** 0 = success

---

### `airflowpulse version`

Print version.

**Exit codes:** 0 = success

## What this does NOT do

- Does NOT **execute** DAGs, tasks, or any Airflow operations — it observes, never acts
- Does NOT **store** metrics, logs, or state — it collects and exports to Prometheus; your stack stores
- Does NOT **replace** Airflow's built-in StatsD/Prometheus integration — it adds operational visibility the built-in metrics don't cover
- Does NOT **manage** Airflow configuration, connections, variables, or pools — read-only access only
- Does NOT **own** alerting delivery infrastructure — it fires webhooks; PagerDuty/Slack/OpsGenie routes them

## Handoffs

| Output | Next tool category | Example |
|--------|-------------------|---------|
| `/metrics` endpoint | Prometheus scraper | `prometheus` scrape config |
| `--format json` | JSON processor | `jq`, AI agent |
| Doctor fail verdict | Runbook automation | `airflow` CLI, kubectl |
| Webhook alert | Incident management | PagerDuty, OpsGenie |
| Grafana annotation | Dashboard | Grafana |

## Failure Modes

| Scenario | Behavior | Fallback |
|----------|----------|----------|
| API unreachable | `airflow_up=0`, scrape error incremented | Retry with exponential backoff (3 attempts) |
| Auth failed (401/403) | Doctor reports `fail` for connectivity | Check AIRFLOW_API_USER and AIRFLOW_API_PASSWORD |
| Partial collector failure | Working collectors continue, failed ones log error | `airflow_scrape_errors_total` incremented per failure |
| Scheduler endpoint stale | Doctor/status report `warn` if heartbeat > 30s | Check scheduler process directly |
| Pool endpoint 403 | Pool metrics unavailable, other collectors unaffected | Grant API user pool read permissions |

## Provenance

JSON outputs include a `provenance` map declaring data lineage:
- `observed` — live value from Airflow REST API
- `declared` — from configuration or user annotation
- `inferred` — computed from other fields (e.g., heartbeat age)
- `unknown` — source unclear or stale

---

This tool follows the [Agent-Native CLI Convention](https://ancc.dev). Validate with: `ancc validate .`
