package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"
)

// Config holds all airflowpulse configuration.
type Config struct {
	// Airflow REST API
	APIURLs     []string
	APIUser     string
	APIPassword string

	// Optional: Airflow metadata database
	MetadataDSN string

	// HTTP server
	MetricsPort int

	// Polling
	PollInterval time.Duration

	// Alerting
	TelegramBotToken string
	TelegramChatID   string
	AlertWebhookURL  string
	AlertCooldown    time.Duration

	// Grafana annotations
	GrafanaURL          string
	GrafanaToken        string
	GrafanaDashboardUID string
}

// Load reads configuration from environment variables.
func Load() (*Config, error) {
	raw := os.Getenv("AIRFLOW_API_URL")
	if raw == "" {
		return nil, fmt.Errorf("AIRFLOW_API_URL is required")
	}

	urls := strings.Split(raw, ",")
	for i := range urls {
		urls[i] = strings.TrimSpace(urls[i])
	}

	return &Config{
		APIURLs:             urls,
		APIUser:             os.Getenv("AIRFLOW_API_USER"),
		APIPassword:         os.Getenv("AIRFLOW_API_PASSWORD"),
		MetadataDSN:         os.Getenv("AIRFLOW_METADATA_DSN"),
		MetricsPort:         envInt("METRICS_PORT", 9190),
		PollInterval:        envDuration("POLL_INTERVAL", 15*time.Second),
		TelegramBotToken:    os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:      os.Getenv("TELEGRAM_CHAT_ID"),
		AlertWebhookURL:     os.Getenv("ALERT_WEBHOOK_URL"),
		AlertCooldown:       envDuration("ALERT_COOLDOWN", 5*time.Minute),
		GrafanaURL:          os.Getenv("GRAFANA_URL"),
		GrafanaToken:        os.Getenv("GRAFANA_TOKEN"),
		GrafanaDashboardUID: os.Getenv("GRAFANA_DASHBOARD_UID"),
	}, nil
}

// InstanceLabel extracts a short label from an Airflow API URL for use in metrics.
func InstanceLabel(apiURL string) string {
	u, err := url.Parse(apiURL)
	if err != nil || u.Host == "" {
		return apiURL
	}
	return u.Host
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return def
	}
	return n
}

func envDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
