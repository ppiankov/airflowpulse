package cli

import (
	"github.com/spf13/cobra"
)

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Diagnose Airflow API connectivity and permissions",
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement doctor command — WO-3
		return nil
	},
}

func init() {
	doctorCmd.Flags().String("format", "text", "Output format: text or json")
}
