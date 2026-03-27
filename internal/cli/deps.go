package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var depsCmd = &cobra.Command{
	Use:   "deps <dag_id>",
	Short: "Show task dependency graph for a DAG",
	Long:  "Display upstream/downstream task dependencies in ASCII, JSON, or DOT format.",
	Args:  cobra.ExactArgs(1),
	RunE:  runDeps,
}

func init() {
	depsCmd.Flags().String("format", "text", "Output format: text, json, or dot")
	depsCmd.Flags().String("task", "", "Highlight a specific task in the graph")
}

// DepsResult is the output of the deps command.
type DepsResult struct {
	DagID    string     `json:"dag_id"`
	Instance string     `json:"instance"`
	Tasks    []DepsNode `json:"tasks"`
}

// DepsNode represents a task and its dependencies.
type DepsNode struct {
	TaskID     string   `json:"task_id"`
	Downstream []string `json:"downstream"`
}

func runDeps(cmd *cobra.Command, args []string) error {
	dagID := args[0]
	format, _ := cmd.Flags().GetString("format")
	highlightTask, _ := cmd.Flags().GetString("task")

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)
		instance := config.InstanceLabel(u)

		tasks, terr := client.GetDAGTasks(ctx, dagID)
		if terr != nil {
			continue
		}

		result := DepsResult{DagID: dagID, Instance: instance}
		for _, t := range tasks.Tasks {
			result.Tasks = append(result.Tasks, DepsNode{
				TaskID:     t.TaskID,
				Downstream: t.DownstreamTaskIDs,
			})
		}

		switch format {
		case "json":
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		case "dot":
			return printDepsDOT(cmd, result)
		default:
			return printDepsText(cmd, result, highlightTask)
		}
	}

	return fmt.Errorf("DAG %q not found on any configured instance", dagID)
}

func printDepsText(cmd *cobra.Command, result DepsResult, highlight string) error {
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "Dependencies: %s (%s)\n\n", result.DagID, result.Instance)

	// Build upstream map.
	upstream := make(map[string][]string)
	for _, t := range result.Tasks {
		for _, d := range t.Downstream {
			upstream[d] = append(upstream[d], t.TaskID)
		}
	}

	// Find roots (no upstream).
	downMap := make(map[string][]string)
	allTasks := make(map[string]bool)
	for _, t := range result.Tasks {
		allTasks[t.TaskID] = true
		downMap[t.TaskID] = t.Downstream
	}

	var roots []string
	for id := range allTasks {
		if len(upstream[id]) == 0 {
			roots = append(roots, id)
		}
	}

	// Print tree.
	visited := make(map[string]bool)
	for _, root := range roots {
		printTree(w, root, downMap, visited, "", true, highlight)
	}

	return nil
}

func printTree(w io.Writer, taskID string, downMap map[string][]string, visited map[string]bool, prefix string, isLast bool, highlight string) {
	if visited[taskID] {
		return
	}
	visited[taskID] = true

	connector := "|-- "
	if isLast {
		connector = "`-- "
	}

	marker := "   "
	if taskID == highlight {
		marker = ">> "
	}

	fmt.Fprintf(w, "%s%s%s%s\n", prefix, connector, marker, taskID)

	children := downMap[taskID]
	childPrefix := prefix
	if isLast {
		childPrefix += "    "
	} else {
		childPrefix += "|   "
	}

	for i, child := range children {
		printTree(w, child, downMap, visited, childPrefix, i == len(children)-1, highlight)
	}
}

func printDepsDOT(cmd *cobra.Command, result DepsResult) error {
	w := cmd.OutOrStdout()
	var b strings.Builder
	b.WriteString(fmt.Sprintf("digraph %q {\n", result.DagID))
	b.WriteString("  rankdir=LR;\n")
	for _, t := range result.Tasks {
		for _, d := range t.Downstream {
			b.WriteString(fmt.Sprintf("  %q -> %q;\n", t.TaskID, d))
		}
	}
	b.WriteString("}\n")
	_, err := fmt.Fprint(w, b.String())
	return err
}
