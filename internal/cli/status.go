package cli

import (
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Airflow cluster health summary",
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement status command — WO-19
		return nil
	},
}

func init() {
	statusCmd.Flags().String("format", "text", "Output format: text or json")
}
