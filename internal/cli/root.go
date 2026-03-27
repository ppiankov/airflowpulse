package cli

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "airflowpulse",
	Short: "A heartbeat monitor for Apache Airflow",
	Long:  "airflowpulse polls Airflow REST API and metadata DB, exposes Prometheus metrics on /metrics.",
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(statusCmd)
	rootCmd.AddCommand(doctorCmd)
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(whyCmd)
	rootCmd.AddCommand(pulseCmd)
}
