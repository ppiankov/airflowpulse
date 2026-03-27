package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var appVersion = "dev"

// SetVersion sets the application version from ldflags.
func SetVersion(v string) {
	appVersion = v
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print airflowpulse version",
	Run: func(cmd *cobra.Command, args []string) {
		_, _ = fmt.Fprintf(cmd.OutOrStdout(), "airflowpulse %s\n", appVersion)
	},
}
