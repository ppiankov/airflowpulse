package collector

import (
	"context"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/metrics"
)

const defaultZombieThreshold = 3600.0 // seconds — tasks running longer than this are suspected zombies

// TaskInstances collects task instance state counts for active DAG runs.
type TaskInstances struct{}

func (t *TaskInstances) Name() string { return "taskinstances" }

func (t *TaskInstances) Collect(ctx context.Context, client *airflow.Client, instance string) error {
	// Fetch only active runs to limit API calls.
	runs, err := client.ListDAGRuns(ctx, "~", 100, "running", "queued")
	if err != nil {
		return err
	}

	metrics.TaskInstancesByState.Reset()
	metrics.TaskInstancesTotal.Reset()

	perDAG := make(map[string]map[string]int)
	totals := make(map[string]int)
	zombies := 0

	for _, r := range runs.DAGRuns {
		tasks, terr := client.ListTaskInstances(ctx, r.DagID, r.DagRunID)
		if terr != nil {
			continue
		}
		for _, ti := range tasks.TaskInstances {
			state := "no_status"
			if ti.State != nil {
				state = *ti.State
			}
			if perDAG[r.DagID] == nil {
				perDAG[r.DagID] = make(map[string]int)
			}
			perDAG[r.DagID][state]++
			totals[state]++

			// Zombie detection: running tasks with duration exceeding threshold.
			if state == "running" && ti.Duration != nil && *ti.Duration > defaultZombieThreshold {
				zombies++
			}
		}
	}

	for dagID, states := range perDAG {
		for state, count := range states {
			metrics.TaskInstancesByState.WithLabelValues(instance, dagID, state).Set(float64(count))
		}
	}
	for state, count := range totals {
		metrics.TaskInstancesTotal.WithLabelValues(instance, state).Set(float64(count))
	}

	metrics.ZombieTasks.WithLabelValues(instance).Set(float64(zombies))

	return nil
}
