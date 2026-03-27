package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Output a continuous JSON-lines event stream",
	Long:  "Emit state change events as newline-delimited JSON for AI agent consumption or piping to jq.",
	RunE:  runStream,
}

// StreamEvent is a single event in the JSON-lines stream.
type StreamEvent struct {
	Timestamp string `json:"timestamp"`
	EventType string `json:"event_type"`
	Instance  string `json:"instance"`
	DagID     string `json:"dag_id,omitempty"`
	TaskID    string `json:"task_id,omitempty"`
	Before    string `json:"before,omitempty"`
	After     string `json:"after,omitempty"`
	Severity  string `json:"severity"`
}

func runStream(cmd *cobra.Command, args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	enc := json.NewEncoder(os.Stdout)

	// Track previous state for change detection.
	prevRuns := make(map[string]string)      // key: instance::dag::run -> state
	prevPools := make(map[string]int)        // key: instance::pool -> open slots
	prevScheduler := make(map[string]string) // key: instance -> status

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	// Run first poll immediately.
	pollAndEmit(ctx, cfg, enc, prevRuns, prevPools, prevScheduler)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			pollAndEmit(ctx, cfg, enc, prevRuns, prevPools, prevScheduler)
		}
	}
}

func pollAndEmit(ctx context.Context, cfg *config.Config, enc *json.Encoder, prevRuns map[string]string, prevPools map[string]int, prevScheduler map[string]string) {
	now := time.Now().UTC().Format(time.RFC3339)

	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)
		instance := config.InstanceLabel(u)

		// Scheduler changes.
		h, err := client.Health(ctx)
		if err == nil {
			prev, existed := prevScheduler[instance]
			if existed && prev != h.Scheduler.Status {
				severity := "info"
				if h.Scheduler.Status != "healthy" {
					severity = "critical"
				}
				_ = enc.Encode(StreamEvent{
					Timestamp: now, EventType: "scheduler_state_change", Instance: instance,
					Before: prev, After: h.Scheduler.Status, Severity: severity,
				})
			}
			prevScheduler[instance] = h.Scheduler.Status
		}

		// DAG run state changes.
		runs, rerr := client.ListDAGRuns(ctx, "~", 200)
		if rerr == nil {
			for _, r := range runs.DAGRuns {
				key := fmt.Sprintf("%s::%s::%s", instance, r.DagID, r.DagRunID)
				prev, existed := prevRuns[key]
				if existed && prev != r.State {
					severity := "info"
					if r.State == "failed" {
						severity = "warn"
					}
					_ = enc.Encode(StreamEvent{
						Timestamp: now, EventType: "dag_run_state_change", Instance: instance,
						DagID: r.DagID, Before: prev, After: r.State, Severity: severity,
					})
				}
				prevRuns[key] = r.State
			}
		}

		// Pool saturation changes.
		pools, perr := client.ListPools(ctx)
		if perr == nil {
			for _, p := range pools.Pools {
				key := fmt.Sprintf("%s::%s", instance, p.Name)
				prev, existed := prevPools[key]
				if existed && prev != p.OpenSlots {
					severity := "info"
					if p.OpenSlots == 0 && p.QueuedSlots > 0 {
						severity = "critical"
					} else if p.OpenSlots == 0 {
						severity = "warn"
					}
					_ = enc.Encode(StreamEvent{
						Timestamp: now, EventType: "pool_saturation", Instance: instance,
						DagID: p.Name, Before: fmt.Sprintf("%d open", prev), After: fmt.Sprintf("%d open", p.OpenSlots),
						Severity: severity,
					})
				}
				prevPools[key] = p.OpenSlots
			}
		}
	}
}
