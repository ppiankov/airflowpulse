package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show state changes since last check",
	Long:  "Compare current Airflow state against a previous snapshot. Shows new failures, recoveries, and pool changes.",
	RunE:  runDiff,
}

func init() {
	diffCmd.Flags().String("format", "text", "Output format: text or json")
	diffCmd.Flags().Duration("since", 0, "Compare against snapshot from N ago (unused if snapshot dir is empty)")
}

var snapshotDir = "/tmp/airflowpulse/snapshots"

// DiffSnapshot is the saved state for comparison.
type DiffSnapshot struct {
	Timestamp    time.Time         `json:"timestamp"`
	DAGRunStates map[string]string `json:"dag_run_states"`
	PoolOpen     map[string]int    `json:"pool_open"`
	ImportErrors int               `json:"import_errors"`
	Scheduler    string            `json:"scheduler"`
}

// DiffResult is the output of the diff command.
type DiffResult struct {
	HasChanges    bool           `json:"has_changes"`
	Since         string         `json:"since"`
	NewFailures   []string       `json:"new_failures,omitempty"`
	Recoveries    []string       `json:"recoveries,omitempty"`
	PoolChanges   []PoolChange   `json:"pool_changes,omitempty"`
	SchedulerDiff *SchedulerDiff `json:"scheduler_diff,omitempty"`
	ImportErrors  *ImportDiff    `json:"import_errors_diff,omitempty"`
}

// PoolChange records a change in pool utilization.
type PoolChange struct {
	Pool   string `json:"pool"`
	Before int    `json:"open_before"`
	After  int    `json:"open_after"`
}

// SchedulerDiff records scheduler state change.
type SchedulerDiff struct {
	Before string `json:"before"`
	After  string `json:"after"`
}

// ImportDiff records import error count change.
type ImportDiff struct {
	Before int `json:"before"`
	After  int `json:"after"`
}

func runDiff(cmd *cobra.Command, args []string) error {
	format, _ := cmd.Flags().GetString("format")

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Take current snapshot.
	current := takeSnapshot(ctx, cfg)

	// Load previous snapshot.
	prev, prevErr := loadLatestSnapshot()

	// Save current as new snapshot.
	_ = saveSnapshot(current)

	if prevErr != nil {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "No previous snapshot found. Current state saved. Run again to see changes.\n")
		return nil
	}

	result := computeDiff(prev, current)

	if format == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	return printDiffText(cmd, result)
}

func takeSnapshot(ctx context.Context, cfg *config.Config) DiffSnapshot {
	snap := DiffSnapshot{
		Timestamp:    time.Now(),
		DAGRunStates: make(map[string]string),
		PoolOpen:     make(map[string]int),
	}

	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)

		h, err := client.Health(ctx)
		if err == nil {
			snap.Scheduler = h.Scheduler.Status
		}

		runs, err := client.ListDAGRuns(ctx, "~", 200)
		if err == nil {
			for _, r := range runs.DAGRuns {
				snap.DAGRunStates[r.DagID+"::"+r.DagRunID] = r.State
			}
		}

		pools, err := client.ListPools(ctx)
		if err == nil {
			for _, p := range pools.Pools {
				snap.PoolOpen[p.Name] = p.OpenSlots
			}
		}

		ie, err := client.ListImportErrors(ctx)
		if err == nil {
			snap.ImportErrors = ie.TotalEntries
		}
	}

	return snap
}

func saveSnapshot(snap DiffSnapshot) error {
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return err
	}
	name := fmt.Sprintf("%s.json", snap.Timestamp.Format("20060102-150405"))
	data, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	path := filepath.Join(snapshotDir, name)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}

	// Keep only last 60 snapshots.
	entries, _ := os.ReadDir(snapshotDir)
	if len(entries) > 60 {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})
		for _, e := range entries[:len(entries)-60] {
			_ = os.Remove(filepath.Join(snapshotDir, e.Name()))
		}
	}

	return nil
}

func loadLatestSnapshot() (DiffSnapshot, error) {
	entries, err := os.ReadDir(snapshotDir)
	if err != nil || len(entries) == 0 {
		return DiffSnapshot{}, fmt.Errorf("no snapshots")
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	// Load second-to-last (latest is the one we just saved... but we save after load, so load the actual latest).
	path := filepath.Join(snapshotDir, entries[len(entries)-1].Name())
	data, err := os.ReadFile(path)
	if err != nil {
		return DiffSnapshot{}, err
	}

	var snap DiffSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return DiffSnapshot{}, err
	}
	return snap, nil
}

func computeDiff(prev, current DiffSnapshot) DiffResult {
	result := DiffResult{
		Since: prev.Timestamp.Format("15:04:05"),
	}

	// DAG run state changes.
	for key, curState := range current.DAGRunStates {
		prevState, existed := prev.DAGRunStates[key]
		if !existed {
			if curState == "failed" {
				result.NewFailures = append(result.NewFailures, key)
			}
			continue
		}
		if prevState != "failed" && curState == "failed" {
			result.NewFailures = append(result.NewFailures, key)
		}
		if prevState == "failed" && curState == "success" {
			result.Recoveries = append(result.Recoveries, key)
		}
	}

	// Pool changes.
	for pool, curOpen := range current.PoolOpen {
		if prevOpen, ok := prev.PoolOpen[pool]; ok && prevOpen != curOpen {
			result.PoolChanges = append(result.PoolChanges, PoolChange{
				Pool: pool, Before: prevOpen, After: curOpen,
			})
		}
	}

	// Scheduler diff.
	if prev.Scheduler != current.Scheduler {
		result.SchedulerDiff = &SchedulerDiff{Before: prev.Scheduler, After: current.Scheduler}
	}

	// Import error diff.
	if prev.ImportErrors != current.ImportErrors {
		result.ImportErrors = &ImportDiff{Before: prev.ImportErrors, After: current.ImportErrors}
	}

	result.HasChanges = len(result.NewFailures) > 0 || len(result.Recoveries) > 0 ||
		len(result.PoolChanges) > 0 || result.SchedulerDiff != nil || result.ImportErrors != nil

	return result
}

func printDiffText(cmd *cobra.Command, result DiffResult) error {
	w := cmd.OutOrStdout()

	if !result.HasChanges {
		_, _ = fmt.Fprintf(w, "No changes since %s\n", result.Since)
		return nil
	}

	_, _ = fmt.Fprintf(w, "Changes since %s:\n\n", result.Since)

	if len(result.NewFailures) > 0 {
		_, _ = fmt.Fprintf(w, "  New failures:\n")
		for _, f := range result.NewFailures {
			_, _ = fmt.Fprintf(w, "    [x] %s\n", f)
		}
	}

	if len(result.Recoveries) > 0 {
		_, _ = fmt.Fprintf(w, "  Recoveries:\n")
		for _, r := range result.Recoveries {
			_, _ = fmt.Fprintf(w, "    [+] %s\n", r)
		}
	}

	if len(result.PoolChanges) > 0 {
		_, _ = fmt.Fprintf(w, "  Pool changes:\n")
		for _, pc := range result.PoolChanges {
			_, _ = fmt.Fprintf(w, "    %s: open %d -> %d\n", pc.Pool, pc.Before, pc.After)
		}
	}

	if result.SchedulerDiff != nil {
		_, _ = fmt.Fprintf(w, "  Scheduler: %s -> %s\n", result.SchedulerDiff.Before, result.SchedulerDiff.After)
	}

	if result.ImportErrors != nil {
		_, _ = fmt.Fprintf(w, "  Import errors: %d -> %d\n", result.ImportErrors.Before, result.ImportErrors.After)
	}

	return nil
}
