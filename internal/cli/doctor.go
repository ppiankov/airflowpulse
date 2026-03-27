package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/airflow"
	"github.com/ppiankov/airflowpulse/internal/config"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose Airflow API connectivity and permissions",
	Long:  "Run connectivity, authentication, and endpoint checks against each configured Airflow instance.",
	RunE:  runDoctor,
}

func init() {
	doctorCmd.Flags().String("format", "text", "Output format: text or json")
}

// DoctorResult is the top-level doctor output.
type DoctorResult struct {
	Status     string            `json:"status"`
	Version    string            `json:"version"`
	Checks     []DoctorCheck     `json:"checks"`
	Provenance map[string]string `json:"provenance,omitempty"`
}

// DoctorCheck is a single diagnostic check.
type DoctorCheck struct {
	Name        string `json:"name"`
	Instance    string `json:"instance"`
	Status      string `json:"status"`
	Message     string `json:"message"`
	Remediation string `json:"remediation,omitempty"`
}

func runDoctor(cmd *cobra.Command, args []string) error {
	format, _ := cmd.Flags().GetString("format")

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var checks []DoctorCheck
	overall := "pass"

	for _, u := range cfg.APIURLs {
		client := airflow.New(u, cfg.APIUser, cfg.APIPassword)
		instance := config.InstanceLabel(u)
		checks = append(checks, runInstanceChecks(ctx, client, instance)...)
	}

	for _, c := range checks {
		if c.Status == "fail" {
			overall = "fail"
			break
		}
		if c.Status == "warn" && overall == "pass" {
			overall = "warn"
		}
	}

	result := DoctorResult{
		Status:  overall,
		Version: appVersion,
		Checks:  checks,
	}

	if format == "json" {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	return printDoctorText(cmd, result)
}

func runInstanceChecks(ctx context.Context, client *airflow.Client, instance string) []DoctorCheck {
	var checks []DoctorCheck

	// Check API connectivity.
	h, err := client.Health(ctx)
	if err != nil {
		checks = append(checks, DoctorCheck{
			Name:        "api_connectivity",
			Instance:    instance,
			Status:      "fail",
			Message:     fmt.Sprintf("cannot reach API: %v", err),
			Remediation: "Check AIRFLOW_API_URL, network connectivity, and firewall rules.",
		})
		return checks
	}
	checks = append(checks, DoctorCheck{
		Name:     "api_connectivity",
		Instance: instance,
		Status:   "pass",
		Message:  "API reachable",
	})

	// Scheduler health.
	if h.Scheduler.Status == "healthy" {
		msg := "scheduler healthy"
		status := "pass"
		if h.Scheduler.LatestHeartbeat != "" {
			if t, perr := time.Parse(time.RFC3339, h.Scheduler.LatestHeartbeat); perr == nil {
				age := time.Since(t)
				msg = fmt.Sprintf("scheduler healthy (heartbeat %s ago)", age.Truncate(time.Second))
				if age > 30*time.Second {
					status = "warn"
					msg = fmt.Sprintf("scheduler heartbeat stale (%s ago)", age.Truncate(time.Second))
				}
			}
		}
		checks = append(checks, DoctorCheck{
			Name:        "scheduler",
			Instance:    instance,
			Status:      status,
			Message:     msg,
			Remediation: condStr(status != "pass", "Check scheduler process. Consider restarting the scheduler."),
		})
	} else {
		checks = append(checks, DoctorCheck{
			Name:        "scheduler",
			Instance:    instance,
			Status:      "fail",
			Message:     fmt.Sprintf("scheduler status: %s", h.Scheduler.Status),
			Remediation: "Scheduler is not healthy. Check scheduler logs and process status.",
		})
	}

	// Metadatabase health.
	if h.Metadatabase.Status == "healthy" {
		checks = append(checks, DoctorCheck{
			Name:     "metadatabase",
			Instance: instance,
			Status:   "pass",
			Message:  "metadatabase healthy",
		})
	} else {
		checks = append(checks, DoctorCheck{
			Name:        "metadatabase",
			Instance:    instance,
			Status:      "fail",
			Message:     fmt.Sprintf("metadatabase status: %s", h.Metadatabase.Status),
			Remediation: "Check database connectivity and run 'airflow db check'.",
		})
	}

	// Pools endpoint.
	if _, perr := client.ListPools(ctx); perr != nil {
		checks = append(checks, DoctorCheck{
			Name:        "pools_endpoint",
			Instance:    instance,
			Status:      "warn",
			Message:     fmt.Sprintf("cannot list pools: %v", perr),
			Remediation: "Check API user permissions. The pools endpoint requires read access.",
		})
	} else {
		checks = append(checks, DoctorCheck{
			Name:     "pools_endpoint",
			Instance: instance,
			Status:   "pass",
			Message:  "pools endpoint accessible",
		})
	}

	// DAGs endpoint.
	if _, derr := client.ListDAGs(ctx, 1); derr != nil {
		checks = append(checks, DoctorCheck{
			Name:        "dags_endpoint",
			Instance:    instance,
			Status:      "warn",
			Message:     fmt.Sprintf("cannot list DAGs: %v", derr),
			Remediation: "Check API user permissions. The dags endpoint requires read access.",
		})
	} else {
		checks = append(checks, DoctorCheck{
			Name:     "dags_endpoint",
			Instance: instance,
			Status:   "pass",
			Message:  "dags endpoint accessible",
		})
	}

	// Import errors endpoint.
	if _, ierr := client.ListImportErrors(ctx); ierr != nil {
		checks = append(checks, DoctorCheck{
			Name:        "import_errors_endpoint",
			Instance:    instance,
			Status:      "warn",
			Message:     fmt.Sprintf("cannot list import errors: %v", ierr),
			Remediation: "Check API user permissions for the importErrors endpoint.",
		})
	} else {
		checks = append(checks, DoctorCheck{
			Name:     "import_errors_endpoint",
			Instance: instance,
			Status:   "pass",
			Message:  "import errors endpoint accessible",
		})
	}

	return checks
}

func printDoctorText(cmd *cobra.Command, result DoctorResult) error {
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "airflowpulse doctor (v%s)\n\n", result.Version)

	for _, c := range result.Checks {
		icon := "+"
		switch c.Status {
		case "fail":
			icon = "x"
		case "warn":
			icon = "!"
		}
		fmt.Fprintf(w, "  [%s] %s (%s): %s\n", icon, c.Name, c.Instance, c.Message)
		if c.Remediation != "" {
			fmt.Fprintf(w, "      -> %s\n", c.Remediation)
		}
	}

	fmt.Fprintf(w, "\nOverall: %s\n", result.Status)

	if result.Status == "fail" {
		os.Exit(1)
	}
	if result.Status == "warn" {
		os.Exit(2)
	}
	return nil
}

func condStr(cond bool, s string) string {
	if cond {
		return s
	}
	return ""
}
