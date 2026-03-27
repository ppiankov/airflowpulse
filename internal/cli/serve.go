package cli

import (
	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the metrics exporter",
	RunE: func(cmd *cobra.Command, args []string) error {
		// TODO: implement serve loop — WO-2
		return nil
	},
}
