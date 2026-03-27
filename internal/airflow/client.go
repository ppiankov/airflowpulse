package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const defaultTimeout = 10 * time.Second

// Client is a typed HTTP client for the Airflow REST API v1.
type Client struct {
	baseURL  string
	user     string
	password string
	http     *http.Client
}

// New creates an Airflow API client.
func New(baseURL, user, password string) *Client {
	return &Client{
		baseURL:  baseURL,
		user:     user,
		password: password,
		http:     &http.Client{Timeout: defaultTimeout},
	}
}

// BaseURL returns the base URL for labeling.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// HealthResponse represents GET /health.
type HealthResponse struct {
	Metadatabase struct {
		Status string `json:"status"`
	} `json:"metadatabase"`
	Scheduler struct {
		Status             string `json:"status"`
		LatestHeartbeat    string `json:"latest_heartbeat"`
		LatestDagProcessed string `json:"latest_dag_processor_heartbeat"`
	} `json:"scheduler"`
}

// Health checks /health endpoint.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var resp HealthResponse
	if err := c.get(ctx, "/health", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	url := c.baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("airflow API %s returned %d: %s", path, resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(out)
}
