package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

const defaultEnvConfig = `# airflowpulse configuration (environment variables)

# Required: Airflow REST API base URL (comma-separated for multi-instance)
AIRFLOW_API_URL=http://localhost:8080/api/v1
# Multi-instance example:
# AIRFLOW_API_URL=http://airflow-prod:8080/api/v1,http://airflow-staging:8080/api/v1

# Airflow API authentication (basic auth)
AIRFLOW_API_USER=admin
AIRFLOW_API_PASSWORD=admin

# Optional: Airflow metadata database DSN (enables SLA miss, XCom, executor collectors)
# Supports Postgres and MySQL connection strings
# AIRFLOW_METADATA_DSN=

# HTTP server port for /metrics and /healthz
METRICS_PORT=9190

# Poll interval (Go duration)
POLL_INTERVAL=15s

# Telegram alerts (optional)
# TELEGRAM_BOT_TOKEN=
# TELEGRAM_CHAT_ID=

# Webhook alerts (optional)
# ALERT_WEBHOOK_URL=

# Alert cooldown
ALERT_COOLDOWN=5m

# Grafana annotations (optional)
# GRAFANA_URL=
# GRAFANA_TOKEN=
# GRAFANA_DASHBOARD_UID=
`

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Print default configuration",
	Long:  "Print a default .env configuration file with all supported environment variables and sensible defaults.",
	RunE: func(cmd *cobra.Command, args []string) error {
		_, err := fmt.Fprint(cmd.OutOrStdout(), defaultEnvConfig)
		return err
	},
}
