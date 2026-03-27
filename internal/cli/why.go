package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var whyCmd = &cobra.Command{
	Use:   "why <dag_id> <task_id>",
	Short: "Investigate why a task is stuck",
	Long:  "Trace the root cause chain for a stuck or failed task: pool capacity, scheduler health, upstream dependencies.",
	Args:  cobra.ExactArgs(2),
	RunE:  runWhy,
}

func init() {
	whyCmd.Flags().String("format", "text", "Output format: text or json")
	whyCmd.Flags().String("run-id", "", "Specific DAG run ID (default: latest)")
}

// WhyResult is the top-level investigation output.
type WhyResult struct {
	DagID      string            `json:"dag_id"`
	TaskID     string            `json:"task_id"`
	RunID      string            `json:"run_id"`
	Instance   string            `json:"instance"`
	State      string            `json:"state"`
	Verdict    string            `json:"verdict"`
	Chain      []WhyFinding      `json:"chain"`
	Provenance map[string]string `json:"provenance,omitempty"`
}

// WhyFinding is a single step in the root cause chain.
type WhyFinding struct {
	Check       string `json:"check"`
	Status      string `json:"status"`
	Detail      string `json:"detail"`
	Remediation string `json:"remediation,omitempty"`
}

func runWhy(cmd *cobra.Command, args []string) error {
	dagID := args[0]
	taskID := args[1]
	format, _ := cmd.Flags().GetString("format")
	runID, _ := cmd.Flags().GetString("run-id")

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Find which instance has this DAG.
	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)
		instance := config.InstanceLabel(u)

		result, found := investigateTask(ctx, client, instance, dagID, taskID, runID)
		if !found {
			continue
		}

		if format == "json" {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		}
		return printWhyText(cmd, result)
	}

	return fmt.Errorf("DAG %q not found on any configured instance", dagID)
}

func investigateTask(ctx context.Context, client *airflow.Client, instance, dagID, taskID, runID string) (WhyResult, bool) {
	result := WhyResult{
		DagID:    dagID,
		TaskID:   taskID,
		Instance: instance,
		Provenance: map[string]string{
			"state":               "observed",
			"verdict":             "inferred",
			"chain.*.status":      "inferred",
			"chain.*.detail":      "observed",
			"chain.*.remediation": "inferred",
			"scheduler.status":    "observed",
			"scheduler.heartbeat": "observed",
			"pool.slots":          "observed",
		},
	}

	// Find the DAG run.
	var targetRun *airflow.DAGRun
	if runID != "" {
		runs, err := client.ListDAGRuns(ctx, dagID, 50)
		if err != nil {
			return result, false
		}
		for i, r := range runs.DAGRuns {
			if r.DagRunID == runID {
				targetRun = &runs.DAGRuns[i]
				break
			}
		}
	} else {
		runs, err := client.ListDAGRuns(ctx, dagID, 1)
		if err != nil || len(runs.DAGRuns) == 0 {
			return result, false
		}
		targetRun = &runs.DAGRuns[0]
	}

	if targetRun == nil {
		return result, false
	}
	result.RunID = targetRun.DagRunID

	// Find the task instance.
	tasks, err := client.ListTaskInstances(ctx, dagID, targetRun.DagRunID)
	if err != nil {
		return result, true
	}

	var target *airflow.TaskInstance
	for i, ti := range tasks.TaskInstances {
		if ti.TaskID == taskID {
			target = &tasks.TaskInstances[i]
			break
		}
	}

	if target == nil {
		result.Verdict = "task not found in this DAG run"
		result.Chain = append(result.Chain, WhyFinding{
			Check:       "task_exists",
			Status:      "fail",
			Detail:      fmt.Sprintf("task %q not found in run %s", taskID, targetRun.DagRunID),
			Remediation: "Check the task_id spelling. List tasks with: airflowpulse status --dag " + dagID,
		})
		return result, true
	}

	state := "no_status"
	if target.State != nil {
		state = *target.State
	}
	result.State = state

	// Build the investigation chain based on the task state.
	switch state {
	case "queued", "scheduled":
		result.Chain = investigateStuck(ctx, client, instance, target, state)
	case "running":
		result.Chain = investigateRunning(ctx, client, instance, target)
	case "failed":
		result.Chain = investigateFailed(target)
	case "upstream_failed":
		result.Chain = investigateUpstreamFailed(ctx, client, dagID, targetRun.DagRunID, tasks.TaskInstances)
	case "up_for_retry":
		result.Chain = []WhyFinding{{
			Check:  "retry_pending",
			Status: "warn",
			Detail: fmt.Sprintf("task is up for retry (attempt %d)", target.TryNumber),
		}}
	case "success":
		result.Chain = []WhyFinding{{
			Check:  "task_state",
			Status: "pass",
			Detail: "task completed successfully",
		}}
	default:
		result.Chain = []WhyFinding{{
			Check:  "task_state",
			Status: "warn",
			Detail: fmt.Sprintf("task is in state: %s", state),
		}}
	}

	// Summarize verdict.
	verdict := "healthy"
	for _, f := range result.Chain {
		if f.Status == "fail" {
			verdict = "problem found"
			break
		}
		if f.Status == "warn" {
			verdict = "warning"
		}
	}
	result.Verdict = verdict

	return result, true
}

func investigateStuck(ctx context.Context, client *airflow.Client, instance string, task *airflow.TaskInstance, state string) []WhyFinding {
	var chain []WhyFinding

	chain = append(chain, WhyFinding{
		Check:  "task_state",
		Status: "warn",
		Detail: fmt.Sprintf("task is %s (not yet running)", state),
	})

	// Check scheduler health.
	h, err := client.Health(ctx)
	if err != nil {
		chain = append(chain, WhyFinding{
			Check:       "scheduler_health",
			Status:      "fail",
			Detail:      fmt.Sprintf("cannot check scheduler: %v", err),
			Remediation: "Check API connectivity.",
		})
		return chain
	}

	if h.Scheduler.Status != "healthy" {
		chain = append(chain, WhyFinding{
			Check:       "scheduler_health",
			Status:      "fail",
			Detail:      fmt.Sprintf("scheduler status: %s", h.Scheduler.Status),
			Remediation: "Scheduler is not healthy. Check scheduler logs and process status.",
		})
	} else {
		finding := WhyFinding{
			Check:  "scheduler_health",
			Status: "pass",
			Detail: "scheduler healthy",
		}
		if h.Scheduler.LatestHeartbeat != "" {
			if t, perr := time.Parse(time.RFC3339, h.Scheduler.LatestHeartbeat); perr == nil {
				age := time.Since(t)
				if age > 30*time.Second {
					finding.Status = "warn"
					finding.Detail = fmt.Sprintf("scheduler heartbeat stale (%s ago)", age.Truncate(time.Second))
					finding.Remediation = "Scheduler may be overloaded or stuck. Check scheduler logs."
				}
			}
		}
		chain = append(chain, finding)
	}

	// Check pool capacity.
	pools, perr := client.ListPools(ctx)
	if perr == nil {
		poolName := task.Pool
		if poolName == "" {
			poolName = "default"
		}
		for _, p := range pools.Pools {
			if p.Name == poolName {
				if p.OpenSlots == 0 {
					chain = append(chain, WhyFinding{
						Check:  "pool_capacity",
						Status: "fail",
						Detail: fmt.Sprintf("pool %q has 0 open slots (%d/%d used, %d queued)",
							poolName, p.RunningSlots, p.Slots, p.QueuedSlots),
						Remediation: fmt.Sprintf("Increase pool size: airflow pools set %s %d, or check for long-running tasks.", poolName, p.Slots*2),
					})
				} else {
					chain = append(chain, WhyFinding{
						Check:  "pool_capacity",
						Status: "pass",
						Detail: fmt.Sprintf("pool %q has %d open slots (%d/%d used)", poolName, p.OpenSlots, p.RunningSlots, p.Slots),
					})
				}
				break
			}
		}
	}

	return chain
}

func investigateRunning(ctx context.Context, client *airflow.Client, instance string, task *airflow.TaskInstance) []WhyFinding {
	var chain []WhyFinding

	chain = append(chain, WhyFinding{
		Check:  "task_state",
		Status: "pass",
		Detail: "task is running",
	})

	if task.Duration != nil && *task.Duration > 3600 {
		chain = append(chain, WhyFinding{
			Check:       "long_running",
			Status:      "warn",
			Detail:      fmt.Sprintf("task has been running for %.0f seconds (%.1f hours)", *task.Duration, *task.Duration/3600),
			Remediation: "Check if the task is stuck or performing expected long work. Consider adding a timeout.",
		})
	}

	return chain
}

func investigateFailed(task *airflow.TaskInstance) []WhyFinding {
	return []WhyFinding{{
		Check:       "task_state",
		Status:      "fail",
		Detail:      fmt.Sprintf("task failed on attempt %d", task.TryNumber),
		Remediation: "Check task logs in Airflow UI for error details.",
	}}
}

func investigateUpstreamFailed(ctx context.Context, client *airflow.Client, dagID, runID string, allTasks []airflow.TaskInstance) []WhyFinding {
	var chain []WhyFinding
	chain = append(chain, WhyFinding{
		Check:  "task_state",
		Status: "fail",
		Detail: "task skipped due to upstream failure",
	})

	// Find which upstream tasks failed.
	var failed []string
	for _, ti := range allTasks {
		if ti.State != nil && *ti.State == "failed" {
			failed = append(failed, ti.TaskID)
		}
	}
	if len(failed) > 0 {
		chain = append(chain, WhyFinding{
			Check:       "upstream_tasks",
			Status:      "fail",
			Detail:      fmt.Sprintf("failed upstream tasks: %v", failed),
			Remediation: "Fix the upstream task failures first. Check their logs in Airflow UI.",
		})
	}

	return chain
}

func printWhyText(cmd *cobra.Command, result WhyResult) error {
	w := cmd.OutOrStdout()
	_, _ = fmt.Fprintf(w, "Investigation: %s.%s (run: %s, instance: %s)\n", result.DagID, result.TaskID, result.RunID, result.Instance)
	_, _ = fmt.Fprintf(w, "State: %s  Verdict: %s\n\n", result.State, result.Verdict)

	for _, f := range result.Chain {
		icon := "+"
		switch f.Status {
		case "fail":
			icon = "x"
		case "warn":
			icon = "!"
		}
		_, _ = fmt.Fprintf(w, "  [%s] %s: %s\n", icon, f.Check, f.Detail)
		if f.Remediation != "" {
			_, _ = fmt.Fprintf(w, "      -> %s\n", f.Remediation)
		}
	}

	return nil
}
