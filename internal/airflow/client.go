package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// --- Response types ---

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

// DAGList represents GET /dags response.
type DAGList struct {
	DAGs         []DAG `json:"dags"`
	TotalEntries int   `json:"total_entries"`
}

// DAG represents a single DAG.
type DAG struct {
	DagID       string   `json:"dag_id"`
	Description string   `json:"description"`
	IsPaused    bool     `json:"is_paused"`
	IsActive    bool     `json:"is_active"`
	Tags        []DAGTag `json:"tags"`
}

// DAGTag represents a DAG tag.
type DAGTag struct {
	Name string `json:"name"`
}

// DAGRunList represents GET /dags/{dag_id}/dagRuns response.
type DAGRunList struct {
	DAGRuns      []DAGRun `json:"dag_runs"`
	TotalEntries int      `json:"total_entries"`
}

// DAGRun represents a single DAG run.
type DAGRun struct {
	DagID           string  `json:"dag_id"`
	DagRunID        string  `json:"dag_run_id"`
	State           string  `json:"state"`
	ExecutionDate   string  `json:"execution_date"`
	StartDate       *string `json:"start_date"`
	EndDate         *string `json:"end_date"`
	ExternalTrigger bool    `json:"external_trigger"`
}

// TaskInstanceList represents task instances response.
type TaskInstanceList struct {
	TaskInstances []TaskInstance `json:"task_instances"`
	TotalEntries  int            `json:"total_entries"`
}

// TaskInstance represents a single task instance.
type TaskInstance struct {
	TaskID    string   `json:"task_id"`
	DagID     string   `json:"dag_id"`
	DagRunID  string   `json:"dag_run_id"`
	State     *string  `json:"state"`
	StartDate *string  `json:"start_date"`
	EndDate   *string  `json:"end_date"`
	Duration  *float64 `json:"duration"`
	TryNumber int      `json:"try_number"`
	Pool      string   `json:"pool"`
	Operator  string   `json:"operator"`
}

// PoolList represents GET /pools response.
type PoolList struct {
	Pools        []Pool `json:"pools"`
	TotalEntries int    `json:"total_entries"`
}

// Pool represents a single pool.
type Pool struct {
	Name          string `json:"name"`
	Slots         int    `json:"slots"`
	OccupiedSlots int    `json:"occupied_slots"`
	RunningSlots  int    `json:"running_slots"`
	QueuedSlots   int    `json:"queued_slots"`
	OpenSlots     int    `json:"open_slots"`
}

// ImportErrorList represents GET /importErrors response.
type ImportErrorList struct {
	ImportErrors []ImportError `json:"import_errors"`
	TotalEntries int           `json:"total_entries"`
}

// ImportError represents a single import error.
type ImportError struct {
	ImportErrorID int    `json:"import_error_id"`
	Timestamp     string `json:"timestamp"`
	Filename      string `json:"filename"`
	StackTrace    string `json:"stack_trace"`
}

// --- API methods ---

// Health checks GET /health.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var resp HealthResponse
	if err := c.get(ctx, "/health", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListDAGs lists all DAGs.
func (c *Client) ListDAGs(ctx context.Context, limit int) (*DAGList, error) {
	params := url.Values{"limit": {fmt.Sprintf("%d", limit)}}
	var resp DAGList
	if err := c.get(ctx, "/dags", params, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListDAGRuns lists DAG runs. Use dagID="~" for all DAGs.
func (c *Client) ListDAGRuns(ctx context.Context, dagID string, limit int, states ...string) (*DAGRunList, error) {
	params := url.Values{
		"limit":    {fmt.Sprintf("%d", limit)},
		"order_by": {"-start_date"},
	}
	for _, s := range states {
		params.Add("state", s)
	}
	path := fmt.Sprintf("/dags/%s/dagRuns", url.PathEscape(dagID))
	var resp DAGRunList
	if err := c.get(ctx, path, params, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListTaskInstances lists task instances for a DAG run.
func (c *Client) ListTaskInstances(ctx context.Context, dagID, dagRunID string) (*TaskInstanceList, error) {
	path := fmt.Sprintf("/dags/%s/dagRuns/%s/taskInstances",
		url.PathEscape(dagID), url.PathEscape(dagRunID))
	var resp TaskInstanceList
	if err := c.get(ctx, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListPools lists all pools.
func (c *Client) ListPools(ctx context.Context) (*PoolList, error) {
	var resp PoolList
	if err := c.get(ctx, "/pools", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListImportErrors lists all import errors.
func (c *Client) ListImportErrors(ctx context.Context) (*ImportErrorList, error) {
	var resp ImportErrorList
	if err := c.get(ctx, "/importErrors", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) get(ctx context.Context, path string, params url.Values, out any) error {
	u := c.baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
