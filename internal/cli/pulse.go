package cli

import (
	"github.com/spf13/cobra"

	"github.com/ppiankov/airflowpulse/internal/config"
	"github.com/ppiankov/airflowpulse/internal/tui"
)

var pulseCmd = &cobra.Command{
	Use:   "pulse",
	Short: "Live TUI dashboard for Airflow health",
	Long:  "Full-screen terminal dashboard showing DAG runs, pools, scheduler health, and import errors — updating live.",
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load()
		if err != nil {
			return err
		}
		return tui.Run(cfg)
	},
}
