package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Airflow cluster health summary",
	Long:  "One-shot health summary of all configured Airflow instances with optional filters.",
	RunE:  runWithWatch(runStatus),
}

func init() {
	statusCmd.Flags().String("format", "text", "Output format: text or json")
	statusCmd.Flags().String("dag", "", "Filter DAG runs by glob pattern")
	statusCmd.Flags().String("state", "", "Filter by state (comma-separated: failed,running)")
	statusCmd.Flags().String("pool", "", "Filter pools by name")
	statusCmd.Flags().BoolP("watch", "w", false, "Continuously refresh output")
	statusCmd.Flags().Duration("interval", 0, "Watch refresh interval (default: poll interval)")
}

// StatusResult is the top-level status output.
type StatusResult struct {
	Instances  []InstanceStatus  `json:"instances"`
	Filters    *StatusFilters    `json:"filters,omitempty"`
	Provenance map[string]string `json:"provenance,omitempty"`
}

// StatusFilters records applied filters.
type StatusFilters struct {
	DAG   string   `json:"dag,omitempty"`
	State []string `json:"state,omitempty"`
	Pool  string   `json:"pool,omitempty"`
}

// InstanceStatus is the per-instance status.
type InstanceStatus struct {
	Instance     string         `json:"instance"`
	Scheduler    string         `json:"scheduler"`
	Metadatabase string         `json:"metadatabase"`
	HeartbeatAge string         `json:"heartbeat_age,omitempty"`
	DAGRuns      map[string]int `json:"dag_runs"`
	Pools        []PoolStatus   `json:"pools"`
	ImportErrors int            `json:"import_errors"`
}

// PoolStatus is a single pool's status.
type PoolStatus struct {
	Name   string `json:"name"`
	Used   int    `json:"used"`
	Queued int    `json:"queued"`
	Open   int    `json:"open"`
	Total  int    `json:"total"`
}

func runStatus(cmd *cobra.Command, args []string) error {
	format, _ := cmd.Flags().GetString("format")
	dagFilter, _ := cmd.Flags().GetString("dag")
	stateFilter, _ := cmd.Flags().GetString("state")
	poolFilter, _ := cmd.Flags().GetString("pool")

	var states []string
	if stateFilter != "" {
		states = strings.Split(stateFilter, ",")
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var filters *StatusFilters
	if dagFilter != "" || stateFilter != "" || poolFilter != "" {
		filters = &StatusFilters{DAG: dagFilter, State: states, Pool: poolFilter}
	}

	var instances []InstanceStatus
	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)
		instance := config.InstanceLabel(u)
		is, serr := fetchInstanceStatus(ctx, client, instance, dagFilter, states, poolFilter)
		if serr != nil {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s: %v\n", instance, serr)
			continue
		}
		instances = append(instances, is)
	}

	result := StatusResult{
		Instances: instances,
		Filters:   filters,
		Provenance: map[string]string{
			"instances.*.scheduler":     "observed",
			"instances.*.metadatabase":  "observed",
			"instances.*.heartbeat_age": "inferred",
			"instances.*.dag_runs":      "observed",
			"instances.*.pools":         "observed",
			"instances.*.import_errors": "observed",
			"filters":                   "declared",
		},
	}

	if format == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	return printStatusText(cmd, result)
}

func fetchInstanceStatus(ctx context.Context, client *airflow.Client, instance, dagFilter string, states []string, poolFilter string) (InstanceStatus, error) {
	is := InstanceStatus{
		Instance: instance,
		DAGRuns:  make(map[string]int),
	}

	// Health.
	h, err := client.Health(ctx)
	if err != nil {
		return is, err
	}
	is.Scheduler = h.Scheduler.Status
	is.Metadatabase = h.Metadatabase.Status
	if h.Scheduler.LatestHeartbeat != "" {
		if t, perr := time.Parse(time.RFC3339, h.Scheduler.LatestHeartbeat); perr == nil {
			is.HeartbeatAge = time.Since(t).Truncate(time.Second).String()
		}
	}

	// DAG runs.
	runs, rerr := client.ListDAGRuns(ctx, "~", 200, states...)
	if rerr == nil {
		for _, r := range runs.DAGRuns {
			if dagFilter != "" {
				matched, _ := filepath.Match(dagFilter, r.DagID)
				if !matched {
					continue
				}
			}
			is.DAGRuns[r.State]++
		}
	}

	// Pools.
	pools, perr := client.ListPools(ctx)
	if perr == nil {
		for _, p := range pools.Pools {
			if poolFilter != "" && p.Name != poolFilter {
				continue
			}
			is.Pools = append(is.Pools, PoolStatus{
				Name:   p.Name,
				Used:   p.RunningSlots,
				Queued: p.QueuedSlots,
				Open:   p.OpenSlots,
				Total:  p.Slots,
			})
		}
	}

	// Import errors.
	ie, ierr := client.ListImportErrors(ctx)
	if ierr == nil {
		is.ImportErrors = ie.TotalEntries
	}

	return is, nil
}

func printStatusText(cmd *cobra.Command, result StatusResult) error {
	w := cmd.OutOrStdout()

	for i, inst := range result.Instances {
		if i > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintf(w, "Instance: %s\n", inst.Instance)
		fmt.Fprintf(w, "  Scheduler:    %s", inst.Scheduler)
		if inst.HeartbeatAge != "" {
			fmt.Fprintf(w, " (heartbeat %s ago)", inst.HeartbeatAge)
		}
		fmt.Fprintln(w)
		fmt.Fprintf(w, "  Metadatabase: %s\n", inst.Metadatabase)

		if len(inst.DAGRuns) > 0 {
			fmt.Fprintf(w, "  DAG Runs:    ")
			parts := make([]string, 0, len(inst.DAGRuns))
			for state, count := range inst.DAGRuns {
				parts = append(parts, fmt.Sprintf("%s=%d", state, count))
			}
			fmt.Fprintln(w, strings.Join(parts, "  "))
		}

		if len(inst.Pools) > 0 {
			fmt.Fprintf(w, "  Pools:\n")
			fmt.Fprintf(w, "    %-20s %6s %6s %6s %6s\n", "NAME", "USED", "QUEUED", "OPEN", "TOTAL")
			for _, p := range inst.Pools {
				fmt.Fprintf(w, "    %-20s %6d %6d %6d %6d", p.Name, p.Used, p.Queued, p.Open, p.Total)
				if p.Open == 0 && p.Queued > 0 {
					fmt.Fprint(w, "  !!")
				}
				fmt.Fprintln(w)
			}
		}

		if inst.ImportErrors > 0 {
			fmt.Fprintf(w, "  Import Errors: %d\n", inst.ImportErrors)
		}
	}

	return nil
}
