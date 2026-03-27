package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadRequiresAPIURL(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "")

	cfg, err := Load()
	if err == nil {
		t.Fatal("Load() error = nil, want error")
	}
	if cfg != nil {
		t.Fatalf("Load() config = %#v, want nil", cfg)
	}
	if !strings.Contains(err.Error(), "AIRFLOW_API_URL is required") {
		t.Fatalf("Load() error = %q, want missing AIRFLOW_API_URL", err)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", "http://airflow.example")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if len(cfg.APIURLs) != 1 || cfg.APIURLs[0] != "http://airflow.example" {
		t.Fatalf("Load() APIURLs = %#v", cfg.APIURLs)
	}
	if cfg.MetricsPort != 9190 {
		t.Fatalf("Load() MetricsPort = %d, want 9190", cfg.MetricsPort)
	}
	if cfg.PollInterval != 15*time.Second {
		t.Fatalf("Load() PollInterval = %v, want 15s", cfg.PollInterval)
	}
	if cfg.AlertCooldown != 5*time.Minute {
		t.Fatalf("Load() AlertCooldown = %v, want 5m", cfg.AlertCooldown)
	}
}

func TestLoadParsesExplicitValues(t *testing.T) {
	t.Setenv("AIRFLOW_API_URL", " https://one.example/api , https://two.example/api ")
	t.Setenv("AIRFLOW_API_USER", "airflow")
	t.Setenv("AIRFLOW_API_PASSWORD", "secret")
	t.Setenv("AIRFLOW_METADATA_DSN", "postgres://db")
	t.Setenv("METRICS_PORT", "9292")
	t.Setenv("POLL_INTERVAL", "45s")
	t.Setenv("TELEGRAM_BOT_TOKEN", "token")
	t.Setenv("TELEGRAM_CHAT_ID", "chat")
	t.Setenv("ALERT_WEBHOOK_URL", "https://hooks.example")
	t.Setenv("ALERT_COOLDOWN", "90s")
	t.Setenv("GRAFANA_URL", "https://grafana.example")
	t.Setenv("GRAFANA_TOKEN", "grafana-token")
	t.Setenv("GRAFANA_DASHBOARD_UID", "dash")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	wantURLs := []string{"https://one.example/api", "https://two.example/api"}
	if len(cfg.APIURLs) != len(wantURLs) {
		t.Fatalf("Load() APIURLs len = %d, want %d", len(cfg.APIURLs), len(wantURLs))
	}
	for i, want := range wantURLs {
		if cfg.APIURLs[i] != want {
			t.Fatalf("Load() APIURLs[%d] = %q, want %q", i, cfg.APIURLs[i], want)
		}
	}
	if cfg.APIUser != "airflow" || cfg.APIPassword != "secret" {
		t.Fatalf("Load() auth = %q/%q", cfg.APIUser, cfg.APIPassword)
	}
	if cfg.MetadataDSN != "postgres://db" {
		t.Fatalf("Load() MetadataDSN = %q", cfg.MetadataDSN)
	}
	if cfg.MetricsPort != 9292 {
		t.Fatalf("Load() MetricsPort = %d, want 9292", cfg.MetricsPort)
	}
	if cfg.PollInterval != 45*time.Second {
		t.Fatalf("Load() PollInterval = %v, want 45s", cfg.PollInterval)
	}
	if cfg.TelegramBotToken != "token" || cfg.TelegramChatID != "chat" {
		t.Fatalf("Load() telegram = %q/%q", cfg.TelegramBotToken, cfg.TelegramChatID)
	}
	if cfg.AlertWebhookURL != "https://hooks.example" {
		t.Fatalf("Load() AlertWebhookURL = %q", cfg.AlertWebhookURL)
	}
	if cfg.AlertCooldown != 90*time.Second {
		t.Fatalf("Load() AlertCooldown = %v, want 90s", cfg.AlertCooldown)
	}
	if cfg.GrafanaURL != "https://grafana.example" || cfg.GrafanaToken != "grafana-token" {
		t.Fatalf("Load() grafana = %q/%q", cfg.GrafanaURL, cfg.GrafanaToken)
	}
	if cfg.GrafanaDashboardUID != "dash" {
		t.Fatalf("Load() GrafanaDashboardUID = %q", cfg.GrafanaDashboardUID)
	}
}

func TestInstanceLabel(t *testing.T) {
	tests := []struct {
		name   string
		apiURL string
		want   string
	}{
		{name: "host", apiURL: "https://airflow.example/api/v1", want: "airflow.example"},
		{name: "host with port", apiURL: "http://airflow.example:8080", want: "airflow.example:8080"},
		{name: "invalid", apiURL: "://bad url", want: "://bad url"},
	}

	for _, tt := range tests {
		if got := InstanceLabel(tt.apiURL); got != tt.want {
			t.Fatalf("%s: InstanceLabel(%q) = %q, want %q", tt.name, tt.apiURL, got, tt.want)
		}
	}
}

func TestEnvInt(t *testing.T) {
	t.Setenv("INT_VALUE", "")
	if got := envInt("INT_VALUE", 17); got != 17 {
		t.Fatalf("envInt() empty = %d, want 17", got)
	}

	t.Setenv("INT_VALUE", "bad")
	if got := envInt("INT_VALUE", 17); got != 17 {
		t.Fatalf("envInt() invalid = %d, want 17", got)
	}

	t.Setenv("INT_VALUE", "42")
	if got := envInt("INT_VALUE", 17); got != 42 {
		t.Fatalf("envInt() valid = %d, want 42", got)
	}
}

func TestEnvDuration(t *testing.T) {
	t.Setenv("DURATION_VALUE", "")
	if got := envDuration("DURATION_VALUE", 3*time.Second); got != 3*time.Second {
		t.Fatalf("envDuration() empty = %v, want 3s", got)
	}

	t.Setenv("DURATION_VALUE", "bad")
	if got := envDuration("DURATION_VALUE", 3*time.Second); got != 3*time.Second {
		t.Fatalf("envDuration() invalid = %v, want 3s", got)
	}

	t.Setenv("DURATION_VALUE", "2m30s")
	if got := envDuration("DURATION_VALUE", 3*time.Second); got != 150*time.Second {
		t.Fatalf("envDuration() valid = %v, want 2m30s", got)
	}
}
