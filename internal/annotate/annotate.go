package annotate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client pushes annotations to Grafana.
type Client struct {
	url          string
	token        string
	dashboardUID string
}

// Config for Grafana annotation client.
type Config struct {
	URL          string
	Token        string
	DashboardUID string
}

// New creates a Grafana annotation client.
func New(cfg Config) *Client {
	return &Client{
		url:          cfg.URL,
		token:        cfg.Token,
		dashboardUID: cfg.DashboardUID,
	}
}

// Enabled returns true if Grafana annotation is configured.
func (c *Client) Enabled() bool {
	return c.url != "" && c.token != ""
}

// Annotation describes a Grafana annotation.
type Annotation struct {
	Time time.Time
	Text string
	Tags []string
}

// Push creates a Grafana annotation.
func (c *Client) Push(ctx context.Context, a Annotation) error {
	payload := map[string]any{
		"time":         a.Time.UnixMilli(),
		"text":         a.Text,
		"tags":         a.Tags,
		"dashboardUID": c.dashboardUID,
	}

	body, _ := json.Marshal(payload)
	url := c.url + "/api/annotations"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("grafana returned %d", resp.StatusCode)
	}
	return nil
}
