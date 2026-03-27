# Changelog

All notable changes to this project will be documented in this file.

## [0.1.2] - 2026-03-27

### Fixed
- Add `honorLabels: true` to ServiceMonitor (prevents Prometheus overwriting instance label with pod IP)
- Fix git identity for Homebrew tap push in release workflow

### Added
- Kubernetes deployment prerequisites and troubleshooting guide in README
- Link LICENSE in README

## [0.1.1] - 2026-03-27

### Fixed
- Add missing Secret template to Helm chart
- Add Docker image build and push to release workflow (ghcr.io)
- Add service filter to Grafana dashboard (service dropdown with instance cascade)

## [0.1.0] - 2026-03-27

### Added
- `serve` command: poll loop with retry, HTTP server (/metrics, /healthz), multi-instance support, `--stream` flag
- `doctor` command: API connectivity, auth, scheduler, metadatabase, endpoint checks with remediations
- `status` command: cluster health summary with --dag, --state, --pool filters and --watch mode
- `why` command: stuck task root cause investigator (pool, scheduler, upstream, zombie detection)
- `pulse` command: live TUI dashboard (bubbletea) with vim keys, DAG filter, multi-instance tab
- `history` command: task run history with duration sparkline, trend, P95, failure rate
- `diff` command: state change comparison against previous snapshots
- `deps` command: task dependency graph in ASCII, JSON, or DOT format
- `stream` command: continuous JSON-lines event stream for agent consumption
- `init` command: print default .env configuration
- 7 API collectors: scheduler, DAG runs, task instances, pools, import errors, DAG inventory, dataset events
- 3 metadata DB collectors: SLA misses, XCom bloat, executor slots (requires AIRFLOW_METADATA_DSN)
- Built-in alerting: Telegram bot and webhook with cooldown and custom labels (ALERT_LABELS)
- DAG run failure alerts and pool exhaustion alerts via Telegram/webhook
- Grafana annotation push on instance state transitions
- Helm chart with ServiceMonitor and PrometheusRule (additionalRuleLabels for team routing)
- Grafana dashboard JSON
- Release workflow with CHANGELOG validation and Homebrew tap update
- ANCC-compliant SKILL.md with JSON schemas, jq examples, handoffs, failure modes
- Provenance fields in all JSON outputs (observed, declared, inferred, unknown)
