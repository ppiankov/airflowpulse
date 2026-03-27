# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added
- `serve` command: poll loop with retry, HTTP server (/metrics, /healthz), multi-instance support
- `doctor` command: API connectivity, auth, scheduler, metadatabase, endpoint checks with remediations
- `status` command: cluster health summary with --dag, --state, --pool filters and --watch mode
- `why` command: stuck task root cause investigator (pool, scheduler, upstream, zombie detection)
- `pulse` command: live TUI dashboard (bubbletea) with vim keys, DAG filter, multi-instance tab
- `history` command: task run history with duration sparkline, trend, P95, failure rate
- `diff` command: state change comparison against previous snapshots
- `deps` command: task dependency graph in ASCII, JSON, or DOT format
- `stream` command: continuous JSON-lines event stream for agent consumption
- 7 Prometheus collectors: scheduler, DAG runs, task instances, pools, import errors, DAG inventory, dataset events
- 3 metadata DB collectors: SLA misses, XCom bloat, executor slots (requires AIRFLOW_METADATA_DSN)
- Built-in alerting: Telegram bot and webhook with cooldown
- Grafana annotation push on instance state transitions
- Helm chart with ServiceMonitor and PrometheusRule
- Grafana dashboard JSON
- Release workflow with CHANGELOG validation and Homebrew tap update
- ANCC-compliant SKILL.md with JSON schemas, jq examples, handoffs, failure modes
