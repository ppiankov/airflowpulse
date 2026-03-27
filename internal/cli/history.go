package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var historyCmd = &cobra.Command{
	Use:   "history <dag_id> <task_id>",
	Short: "Show recent run history for a task",
	Long:  "Display last N runs of a task with duration trend, state, and retries.",
	Args:  cobra.ExactArgs(2),
	RunE:  runHistory,
}

func init() {
	historyCmd.Flags().String("format", "text", "Output format: text or json")
	historyCmd.Flags().Int("runs", 10, "Number of recent runs to show")
}

// HistoryResult is the output for the history command.
type HistoryResult struct {
	DagID    string         `json:"dag_id"`
	TaskID   string         `json:"task_id"`
	Instance string         `json:"instance"`
	Runs     []HistoryEntry `json:"runs"`
	Stats    *HistoryStats  `json:"stats,omitempty"`
}

// HistoryEntry is a single run record.
type HistoryEntry struct {
	RunID    string   `json:"run_id"`
	State    string   `json:"state"`
	Duration *float64 `json:"duration_seconds"`
	Retries  int      `json:"retries"`
	Date     string   `json:"date"`
}

// HistoryStats provides aggregate statistics.
type HistoryStats struct {
	MeanDuration float64 `json:"mean_duration"`
	P95Duration  float64 `json:"p95_duration"`
	FailureRate  float64 `json:"failure_rate"`
	Trend        string  `json:"trend"`
}

func runHistory(cmd *cobra.Command, args []string) error {
	dagID := args[0]
	taskID := args[1]
	format, _ := cmd.Flags().GetString("format")
	numRuns, _ := cmd.Flags().GetInt("runs")

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)
		instance := config.InstanceLabel(u)

		result, found := fetchHistory(ctx, client, instance, dagID, taskID, numRuns)
		if !found {
			continue
		}

		if format == "json" {
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		}
		return printHistoryText(cmd, result)
	}

	return fmt.Errorf("DAG %q not found on any configured instance", dagID)
}

func fetchHistory(ctx context.Context, client *airflow.Client, instance, dagID, taskID string, numRuns int) (HistoryResult, bool) {
	result := HistoryResult{DagID: dagID, TaskID: taskID, Instance: instance}

	runs, err := client.ListDAGRuns(ctx, dagID, numRuns)
	if err != nil || len(runs.DAGRuns) == 0 {
		return result, false
	}

	var durations []float64
	var failures int

	for _, r := range runs.DAGRuns {
		tasks, terr := client.ListTaskInstances(ctx, dagID, r.DagRunID)
		if terr != nil {
			continue
		}
		for _, ti := range tasks.TaskInstances {
			if ti.TaskID != taskID {
				continue
			}
			state := "no_status"
			if ti.State != nil {
				state = *ti.State
			}
			entry := HistoryEntry{
				RunID:    r.DagRunID,
				State:    state,
				Duration: ti.Duration,
				Retries:  ti.TryNumber - 1,
			}
			if r.StartDate != nil {
				entry.Date = *r.StartDate
			}
			result.Runs = append(result.Runs, entry)

			if ti.Duration != nil {
				durations = append(durations, *ti.Duration)
			}
			if state == "failed" {
				failures++
			}
		}
	}

	if len(durations) > 0 {
		stats := &HistoryStats{}
		var sum float64
		for _, d := range durations {
			sum += d
		}
		stats.MeanDuration = sum / float64(len(durations))

		// Simple P95 approximation.
		if len(durations) >= 2 {
			sorted := make([]float64, len(durations))
			copy(sorted, durations)
			sortFloat64s(sorted)
			idx := int(math.Ceil(0.95*float64(len(sorted)))) - 1
			if idx >= len(sorted) {
				idx = len(sorted) - 1
			}
			stats.P95Duration = sorted[idx]
		} else {
			stats.P95Duration = durations[0]
		}

		stats.FailureRate = float64(failures) / float64(len(result.Runs))

		// Trend: compare first half avg vs second half avg.
		if len(durations) >= 4 {
			mid := len(durations) / 2
			var first, second float64
			for _, d := range durations[:mid] {
				first += d
			}
			for _, d := range durations[mid:] {
				second += d
			}
			first /= float64(mid)
			second /= float64(len(durations) - mid)
			// Note: runs are ordered newest-first, so "first" is recent, "second" is older.
			if first > second*1.2 {
				stats.Trend = "increasing"
			} else if first < second*0.8 {
				stats.Trend = "decreasing"
			} else {
				stats.Trend = "stable"
			}
		} else {
			stats.Trend = "insufficient data"
		}

		result.Stats = stats
	}

	return result, len(result.Runs) > 0
}

func sortFloat64s(a []float64) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] < a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}

func printHistoryText(cmd *cobra.Command, result HistoryResult) error {
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "History: %s.%s (%s)\n\n", result.DagID, result.TaskID, result.Instance)
	fmt.Fprintf(w, "  %-30s %-12s %10s %8s  %s\n", "RUN", "STATE", "DURATION", "RETRIES", "DATE")

	for _, e := range result.Runs {
		dur := "N/A"
		if e.Duration != nil {
			dur = fmt.Sprintf("%.1fs", *e.Duration)
		}
		date := e.Date
		if len(date) > 19 {
			date = date[:19]
		}
		fmt.Fprintf(w, "  %-30s %-12s %10s %8d  %s\n", truncate(e.RunID, 30), e.State, dur, e.Retries, date)
	}

	if result.Stats != nil {
		fmt.Fprintf(w, "\n  Mean: %.1fs  P95: %.1fs  Failure rate: %.0f%%  Trend: %s\n",
			result.Stats.MeanDuration, result.Stats.P95Duration,
			result.Stats.FailureRate*100, result.Stats.Trend)

		// Sparkline for durations.
		var vals []float64
		for _, e := range result.Runs {
			if e.Duration != nil {
				vals = append(vals, *e.Duration)
			}
		}
		if len(vals) > 1 {
			fmt.Fprintf(w, "  Duration: %s\n", sparkline(vals))
		}
	}

	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func sparkline(vals []float64) string {
	bars := []rune("_.-~*")
	if len(vals) == 0 {
		return ""
	}
	min, max := vals[0], vals[0]
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	rng := max - min
	if rng == 0 {
		rng = 1
	}
	var b strings.Builder
	for _, v := range vals {
		idx := int((v - min) / rng * float64(len(bars)-1))
		if idx >= len(bars) {
			idx = len(bars) - 1
		}
		b.WriteRune(bars[idx])
	}
	return b.String()
}
